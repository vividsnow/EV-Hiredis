#include "EXTERN.h"
#include "perl.h"
#include "XSUB.h"
#include "ppport.h"

#include "EVAPI.h"

#include "hiredis.h"
#include "async.h"
#include "libev_adapter.h"
#include "ngx-queue.h"

typedef struct ev_hiredis_s ev_hiredis_t;
typedef struct ev_hiredis_cb_s ev_hiredis_cb_t;
typedef struct ev_hiredis_wait_s ev_hiredis_wait_t;

typedef ev_hiredis_t* EV__Hiredis;
typedef struct ev_loop* EV__Loop;

struct ev_hiredis_s {
    struct ev_loop* loop;
    redisAsyncContext* ac;
    SV* error_handler;
    SV* connect_handler;
    SV* disconnect_handler;
    struct timeval* connect_timeout;
    struct timeval* command_timeout;
    ngx_queue_t cb_queue;
    ngx_queue_t wait_queue;
    int pending_count;
    int waiting_count;
    int max_pending; /* 0 = unlimited */
    ev_hiredis_cb_t* current_cb; /* callback currently executing */
    int resume_waiting_on_reconnect; /* keep waiting queue on disconnect */
    ev_tstamp waiting_timeout; /* max seconds in waiting queue, 0 = unlimited */
    ev_timer waiting_timer;
    int waiting_timer_active;

    /* Reconnect settings */
    char* host;
    int port;
    char* path;
    int reconnect;              /* 0 = disabled, 1 = enabled */
    int reconnect_delay_ms;     /* delay between reconnect attempts */
    int max_reconnect_attempts; /* 0 = unlimited */
    int reconnect_attempts;     /* current attempt count */
    ev_timer reconnect_timer;
    int reconnect_timer_active;
    int intentional_disconnect; /* set before explicit disconnect() */
    int priority; /* libev watcher priority, default 0 */
};

struct ev_hiredis_cb_s {
    SV* cb;
    ngx_queue_t queue;
    int persist;
    int skipped;
};

struct ev_hiredis_wait_s {
    char** argv;
    size_t* argvlen;
    int argc;
    SV* cb;
    int persist;
    ngx_queue_t queue;
    ev_tstamp queued_at;
};

static void emit_error(EV__Hiredis self, SV* error) {
    if (NULL == self->error_handler) return;

    dSP;

    ENTER;
    SAVETMPS;

    PUSHMARK(SP);
    XPUSHs(error);
    PUTBACK;

    call_sv(self->error_handler, G_DISCARD | G_EVAL);
    if (SvTRUE(ERRSV)) {
        warn("EV::Hiredis: exception in error handler: %s", SvPV_nolen(ERRSV));
    }

    FREETMPS;
    LEAVE;
}

static void emit_error_str(EV__Hiredis self, const char* error) {
    if (NULL == self->error_handler) return;
    emit_error(self, sv_2mortal(newSVpv(error, 0)));
}

static void remove_cb_queue(EV__Hiredis self, const char* error_msg) {
    ngx_queue_t* q;
    ngx_queue_t* next;
    ev_hiredis_cb_t* cbt;

    for (q = ngx_queue_head(&self->cb_queue);
         q != ngx_queue_sentinel(&self->cb_queue);
         q = next) {
        next = ngx_queue_next(q);
        cbt = ngx_queue_data(q, ev_hiredis_cb_t, queue);

        if (cbt == self->current_cb) {
            continue;
        }

        ngx_queue_remove(q);
        self->pending_count--;

        if (NULL != cbt->cb) {
            if (NULL != error_msg) {
                dSP;

                ENTER;
                SAVETMPS;

                PUSHMARK(SP);
                PUSHs(&PL_sv_undef);
                PUSHs(sv_2mortal(newSVpv(error_msg, 0)));
                PUTBACK;

                call_sv(cbt->cb, G_DISCARD | G_EVAL);
                if (SvTRUE(ERRSV)) {
                    warn("EV::Hiredis: exception in command callback: %s", SvPV_nolen(ERRSV));
                }

                FREETMPS;
                LEAVE;
            }

            SvREFCNT_dec(cbt->cb);
        }
        Safefree(cbt);
    }
}

static void free_wait_entry(ev_hiredis_wait_t* wt) {
    int i;
    for (i = 0; i < wt->argc; i++) {
        Safefree(wt->argv[i]);
    }
    Safefree(wt->argv);
    Safefree(wt->argvlen);
    SvREFCNT_dec(wt->cb);
    Safefree(wt);
}

static void clear_wait_queue(EV__Hiredis self, const char* error_msg) {
    ngx_queue_t* q;
    ev_hiredis_wait_t* wt;

    while (!ngx_queue_empty(&self->wait_queue)) {
        q  = ngx_queue_head(&self->wait_queue);
        wt = ngx_queue_data(q, ev_hiredis_wait_t, queue);
        ngx_queue_remove(q);
        self->waiting_count--;

        if (NULL != error_msg) {
            dSP;
            ENTER;
            SAVETMPS;
            PUSHMARK(SP);
            PUSHs(&PL_sv_undef);
            PUSHs(sv_2mortal(newSVpv(error_msg, 0)));
            PUTBACK;
            call_sv(wt->cb, G_DISCARD | G_EVAL);
            if (SvTRUE(ERRSV)) {
                warn("EV::Hiredis: exception in command callback: %s", SvPV_nolen(ERRSV));
            }
            FREETMPS;
            LEAVE;
        }

        free_wait_entry(wt);
    }
}

/* Forward declarations */
static void do_reconnect(EV__Hiredis self);
static void send_next_waiting(EV__Hiredis self);
static void schedule_waiting_timer(EV__Hiredis self);
static void expire_waiting_commands(EV__Hiredis self);
static void schedule_reconnect(EV__Hiredis self);
static void EV__hiredis_connect_cb(redisAsyncContext* c, int status);
static void EV__hiredis_disconnect_cb(redisAsyncContext* c, int status);

static void reconnect_timer_cb(EV_P_ ev_timer* w, int revents) {
    EV__Hiredis self = (EV__Hiredis)w->data;

    (void)loop;
    (void)revents;

    self->reconnect_timer_active = 0;
    do_reconnect(self);
}

static void schedule_reconnect(EV__Hiredis self) {
    ev_tstamp delay;

    if (!self->reconnect) return;
    if (self->max_reconnect_attempts > 0 &&
        self->reconnect_attempts >= self->max_reconnect_attempts) {
        emit_error_str(self, "max reconnect attempts reached");
        return;
    }

    self->reconnect_attempts++;
    delay = self->reconnect_delay_ms / 1000.0;

    ev_timer_init(&self->reconnect_timer, reconnect_timer_cb, delay, 0);
    self->reconnect_timer.data = (void*)self;
    ev_timer_start(self->loop, &self->reconnect_timer);
    self->reconnect_timer_active = 1;
}

static void expire_waiting_commands(EV__Hiredis self) {
    ngx_queue_t* q;
    ngx_queue_t* next;
    ev_hiredis_wait_t* wt;
    ev_tstamp now = ev_now(self->loop);

    for (q = ngx_queue_head(&self->wait_queue);
         q != ngx_queue_sentinel(&self->wait_queue);
         q = next) {
        next = ngx_queue_next(q);
        wt = ngx_queue_data(q, ev_hiredis_wait_t, queue);

        if (now - wt->queued_at >= self->waiting_timeout) {
            ngx_queue_remove(q);
            self->waiting_count--;

            {
                dSP;
                ENTER;
                SAVETMPS;
                PUSHMARK(SP);
                PUSHs(&PL_sv_undef);
                PUSHs(sv_2mortal(newSVpv("waiting timeout", 0)));
                PUTBACK;
                call_sv(wt->cb, G_DISCARD | G_EVAL);
                if (SvTRUE(ERRSV)) {
                    warn("EV::Hiredis: exception in command callback: %s", SvPV_nolen(ERRSV));
                }
                FREETMPS;
                LEAVE;
            }

            free_wait_entry(wt);
        }
    }
}

static void waiting_timer_cb(EV_P_ ev_timer* w, int revents) {
    EV__Hiredis self = (EV__Hiredis)w->data;

    (void)loop;
    (void)revents;

    self->waiting_timer_active = 0;
    expire_waiting_commands(self);
    schedule_waiting_timer(self);
}

static void schedule_waiting_timer(EV__Hiredis self) {
    ngx_queue_t* q;
    ev_hiredis_wait_t* wt;
    ev_tstamp now, expires_at, delay;

    if (self->waiting_timer_active) {
        ev_timer_stop(self->loop, &self->waiting_timer);
        self->waiting_timer_active = 0;
    }

    if (self->waiting_timeout <= 0) return;
    if (ngx_queue_empty(&self->wait_queue)) return;

    q = ngx_queue_head(&self->wait_queue);
    wt = ngx_queue_data(q, ev_hiredis_wait_t, queue);

    now = ev_now(self->loop);
    expires_at = wt->queued_at + self->waiting_timeout;
    delay = expires_at - now;
    if (delay < 0) delay = 0;

    ev_timer_init(&self->waiting_timer, waiting_timer_cb, delay, 0);
    self->waiting_timer.data = (void*)self;
    ev_timer_start(self->loop, &self->waiting_timer);
    self->waiting_timer_active = 1;
}

static void do_reconnect(EV__Hiredis self) {
    redisOptions opts = {0};

    if (NULL != self->ac) {
        /* Already connected or connecting */
        return;
    }

    if (NULL != self->connect_timeout) {
        opts.connect_timeout = self->connect_timeout;
    }
    if (NULL != self->command_timeout) {
        opts.command_timeout = self->command_timeout;
    }

    if (NULL != self->path) {
        REDIS_OPTIONS_SET_UNIX(&opts, self->path);
    }
    else if (NULL != self->host) {
        REDIS_OPTIONS_SET_TCP(&opts, self->host, self->port);
    }
    else {
        emit_error_str(self, "reconnect error: no connection parameters");
        return;
    }

    self->ac = redisAsyncConnectWithOptions(&opts);
    if (NULL == self->ac) {
        emit_error_str(self, "reconnect error: cannot allocate memory");
        schedule_reconnect(self);
        return;
    }

    self->ac->data = (void*)self;

    if (REDIS_OK != redisLibevAttach(self->loop, self->ac)) {
        redisAsyncFree(self->ac);
        self->ac = NULL;
        emit_error_str(self, "reconnect error: cannot attach libev");
        schedule_reconnect(self);
        return;
    }

    if (self->priority != 0) {
        redisLibevSetPriority(self->ac, self->priority);
    }

    redisAsyncSetConnectCallback(self->ac, (redisConnectCallback*)EV__hiredis_connect_cb);
    redisAsyncSetDisconnectCallback(self->ac, (redisDisconnectCallback*)EV__hiredis_disconnect_cb);

    if (self->ac->err) {
        emit_error_str(self, self->ac->errstr);
        redisAsyncFree(self->ac);
        self->ac = NULL;
        schedule_reconnect(self);
        return;
    }
}

static void EV__hiredis_connect_cb(redisAsyncContext* c, int status) {
    EV__Hiredis self = (EV__Hiredis)c->data;

    if (REDIS_OK != status) {
        self->ac = NULL;
        emit_error_str(self, c->errstr);
        schedule_reconnect(self);
    }
    else {
        self->reconnect_attempts = 0;

        if (NULL != self->connect_handler) {
            dSP;

            ENTER;
            SAVETMPS;

            PUSHMARK(SP);
            PUTBACK;

            call_sv(self->connect_handler, G_DISCARD | G_EVAL);
            if (SvTRUE(ERRSV)) {
                warn("EV::Hiredis: exception in connect handler: %s", SvPV_nolen(ERRSV));
            }

            FREETMPS;
            LEAVE;
        }

        /* Resume waiting commands if any */
        while (!ngx_queue_empty(&self->wait_queue)) {
            if (self->max_pending > 0 && self->pending_count >= self->max_pending) {
                break;
            }
            send_next_waiting(self);
        }
    }
}

static void EV__hiredis_disconnect_cb(redisAsyncContext* c, int status) {
    EV__Hiredis self = (EV__Hiredis)c->data;
    const char* error_msg;
    int should_reconnect = 0;
    int was_intentional = self->intentional_disconnect;

    self->intentional_disconnect = 0;

    if (REDIS_OK == status) {
        self->ac = NULL;
        error_msg = "disconnected";
    }
    else {
        error_msg = c->errstr;
        self->ac = NULL;
        emit_error_str(self, c->errstr);
        if (!was_intentional) {
            should_reconnect = 1;
        }
    }

    if (NULL != self->disconnect_handler) {
        dSP;

        ENTER;
        SAVETMPS;

        PUSHMARK(SP);
        PUTBACK;

        call_sv(self->disconnect_handler, G_DISCARD | G_EVAL);
        if (SvTRUE(ERRSV)) {
            warn("EV::Hiredis: exception in disconnect handler: %s", SvPV_nolen(ERRSV));
        }

        FREETMPS;
        LEAVE;
    }

    remove_cb_queue(self, error_msg);

    if (!self->resume_waiting_on_reconnect) {
        clear_wait_queue(self, error_msg);
    }

    if (should_reconnect) {
        schedule_reconnect(self);
    }
}

static void pre_connect_common(EV__Hiredis self, redisOptions* opts) {
    if (NULL != self->connect_timeout) {
        opts->connect_timeout = self->connect_timeout;
    }
    if (NULL != self->command_timeout) {
        opts->command_timeout = self->command_timeout;
    }
}

static void connect_common(EV__Hiredis self) {
    int r;

    self->ac->data = (void*)self;

    r = redisLibevAttach(self->loop, self->ac);
    if (REDIS_OK != r) {
        redisAsyncFree(self->ac);
        self->ac = NULL;
        emit_error_str(self, "connect error: cannot attach libev");
        return;
    }

    if (self->priority != 0) {
        redisLibevSetPriority(self->ac, self->priority);
    }

    redisAsyncSetConnectCallback(self->ac, (redisConnectCallback*)EV__hiredis_connect_cb);
    redisAsyncSetDisconnectCallback(self->ac, (redisDisconnectCallback*)EV__hiredis_disconnect_cb);

    if (self->ac->err) {
        SV* sv_error = sv_2mortal(newSVpvf("connect error: %s", self->ac->errstr));
        redisAsyncFree(self->ac);
        self->ac = NULL;
        emit_error(self, sv_error);
        return;
    }
}

static SV* EV__hiredis_decode_reply(redisReply* reply) {
    SV* res = NULL;

    switch (reply->type) {
        case REDIS_REPLY_STRING:
        case REDIS_REPLY_ERROR:
        case REDIS_REPLY_STATUS:
        case REDIS_REPLY_BIGNUM:
        case REDIS_REPLY_VERB:
            res = newSVpvn(reply->str, reply->len);
            break;

        case REDIS_REPLY_INTEGER:
            res = newSViv(reply->integer);
            break;

        case REDIS_REPLY_DOUBLE:
            res = newSVnv(reply->dval);
            break;

        case REDIS_REPLY_BOOL:
            res = newSViv(reply->integer ? 1 : 0);
            break;

        case REDIS_REPLY_NIL:
            res = newSV(0);
            break;

        case REDIS_REPLY_ARRAY:
        case REDIS_REPLY_MAP:
        case REDIS_REPLY_SET:
        case REDIS_REPLY_ATTR:
        case REDIS_REPLY_PUSH: {
            AV* av = newAV();
            size_t i;
            if (reply->elements > 0) {
                av_extend(av, (SSize_t)(reply->elements - 1));
                for (i = 0; i < reply->elements; i++) {
                    if (reply->element[i] != NULL) {
                        av_push(av, EV__hiredis_decode_reply(reply->element[i]));
                    }
                    else {
                        av_push(av, newSV(0));
                    }
                }
            }
            res = newRV_noinc((SV*)av);
            break;
        }

        default:
            /* Unknown type, return undef */
            res = newSV(0);
            break;
    }

    return res;
}

static void EV__hiredis_reply_cb(redisAsyncContext* c, void* reply, void* privdata) {
    EV__Hiredis self = (EV__Hiredis)c->data;
    ev_hiredis_cb_t* cbt;
    SV* sv_reply;
    SV* sv_err;

    cbt = (ev_hiredis_cb_t*)privdata;

    if (cbt->skipped) {
        Safefree(cbt);
        return;
    }

    self->current_cb = cbt;

    if (NULL == reply) {
        dSP;

        ENTER;
        SAVETMPS;

        sv_err = sv_2mortal(newSVpv(c->errstr, 0));

        PUSHMARK(SP);
        PUSHs(&PL_sv_undef);
        PUSHs(sv_err);
        PUTBACK;

        call_sv(cbt->cb, G_DISCARD | G_EVAL);
        if (SvTRUE(ERRSV)) {
            warn("EV::Hiredis: exception in command callback: %s", SvPV_nolen(ERRSV));
        }

        FREETMPS;
        LEAVE;
    }
    else {
        dSP;

        ENTER;
        SAVETMPS;

        PUSHMARK(SP);
        sv_reply = sv_2mortal(EV__hiredis_decode_reply((redisReply*)reply));
        if (((redisReply*)reply)->type == REDIS_REPLY_ERROR) {
            PUSHs(&PL_sv_undef);
            PUSHs(sv_reply);
        }
        else {
            PUSHs(sv_reply);
        }
        PUTBACK;

        call_sv(cbt->cb, G_DISCARD | G_EVAL);
        if (SvTRUE(ERRSV)) {
            warn("EV::Hiredis: exception in command callback: %s", SvPV_nolen(ERRSV));
        }

        FREETMPS;
        LEAVE;
    }

    self->current_cb = NULL;

    if (cbt->skipped) {
        Safefree(cbt);
        return;
    }

    if (0 == cbt->persist) {
        SvREFCNT_dec(cbt->cb);
        ngx_queue_remove(&cbt->queue);
        self->pending_count--;
        Safefree(cbt);
        send_next_waiting(self);
    }
}

static void send_next_waiting(EV__Hiredis self) {
    ngx_queue_t* q;
    ev_hiredis_wait_t* wt;
    ev_hiredis_cb_t* cbt;
    int r;

    if (NULL == self->ac) return;
    if (ngx_queue_empty(&self->wait_queue)) return;
    if (self->max_pending > 0 && self->pending_count >= self->max_pending) return;

    q  = ngx_queue_head(&self->wait_queue);
    wt = ngx_queue_data(q, ev_hiredis_wait_t, queue);
    ngx_queue_remove(q);
    self->waiting_count--;

    Newx(cbt, 1, ev_hiredis_cb_t);
    cbt->cb = wt->cb;
    wt->cb = NULL;
    cbt->skipped = 0;
    cbt->persist = wt->persist;
    ngx_queue_init(&cbt->queue);
    ngx_queue_insert_tail(&self->cb_queue, &cbt->queue);
    self->pending_count++;

    r = redisAsyncCommandArgv(
        self->ac, EV__hiredis_reply_cb, (void*)cbt,
        wt->argc, (const char**)wt->argv, wt->argvlen
    );

    if (REDIS_OK != r) {
        ngx_queue_remove(&cbt->queue);
        self->pending_count--;

        {
            dSP;
            ENTER;
            SAVETMPS;
            PUSHMARK(SP);
            PUSHs(&PL_sv_undef);
            PUSHs(sv_2mortal(newSVpv(
                (self->ac && self->ac->errstr[0]) ? self->ac->errstr : "command failed", 0)));
            PUTBACK;
            call_sv(cbt->cb, G_DISCARD | G_EVAL);
            if (SvTRUE(ERRSV)) {
                warn("EV::Hiredis: exception in command callback: %s", SvPV_nolen(ERRSV));
            }
            FREETMPS;
            LEAVE;
        }

        SvREFCNT_dec(cbt->cb);
        Safefree(cbt);
    }

    free_wait_entry(wt);
}

MODULE = EV::Hiredis PACKAGE = EV::Hiredis

BOOT:
{
    I_EV_API("EV::Hiredis");
}

EV::Hiredis
_new(char* class, EV::Loop loop);
CODE:
{
    PERL_UNUSED_VAR(class);
    Newxz(RETVAL, 1, ev_hiredis_t);
    ngx_queue_init(&RETVAL->cb_queue);
    ngx_queue_init(&RETVAL->wait_queue);
    RETVAL->loop = loop;
}
OUTPUT:
    RETVAL

void
DESTROY(EV::Hiredis self);
CODE:
{
    if (self->reconnect_timer_active && NULL != self->loop) {
        ev_timer_stop(self->loop, &self->reconnect_timer);
        self->reconnect_timer_active = 0;
    }
    if (self->waiting_timer_active && NULL != self->loop) {
        ev_timer_stop(self->loop, &self->waiting_timer);
        self->waiting_timer_active = 0;
    }

    self->reconnect = 0;

    self->loop = NULL;
    if (NULL != self->ac) {
        redisAsyncFree(self->ac);
        self->ac = NULL;
    }
    if (NULL != self->error_handler) {
        SvREFCNT_dec(self->error_handler);
        self->error_handler = NULL;
    }
    if (NULL != self->connect_handler) {
        SvREFCNT_dec(self->connect_handler);
        self->connect_handler = NULL;
    }
    if (NULL != self->disconnect_handler) {
        SvREFCNT_dec(self->disconnect_handler);
        self->disconnect_handler = NULL;
    }
    if (NULL != self->connect_timeout) {
        Safefree(self->connect_timeout);
        self->connect_timeout = NULL;
    }
    if (NULL != self->command_timeout) {
        Safefree(self->command_timeout);
        self->command_timeout = NULL;
    }
    if (NULL != self->host) {
        Safefree(self->host);
        self->host = NULL;
    }
    if (NULL != self->path) {
        Safefree(self->path);
        self->path = NULL;
    }

    clear_wait_queue(self, NULL);
    remove_cb_queue(self, NULL);

    Safefree(self);
}

void
connect(EV::Hiredis self, char* hostname, int port = 6379);
CODE:
{
    size_t len;

    if (NULL != self->ac) {
        croak("already connected");
    }

    if (NULL != self->host) {
        Safefree(self->host);
    }
    if (NULL != self->path) {
        Safefree(self->path);
        self->path = NULL;
    }
    len = strlen(hostname);
    Newx(self->host, len + 1, char);
    Copy(hostname, self->host, len + 1, char);
    self->port = port;

    redisOptions opts = {0};
    pre_connect_common(self, &opts);
    REDIS_OPTIONS_SET_TCP(&opts, hostname, port);
    self->ac = redisAsyncConnectWithOptions(&opts);
    if (NULL == self->ac) {
        croak("cannot allocate memory");
    }

    connect_common(self);
}

void
connect_unix(EV::Hiredis self, const char* path);
CODE:
{
    size_t len;

    if (NULL != self->ac) {
        croak("already connected");
    }

    if (NULL != self->path) {
        Safefree(self->path);
    }
    if (NULL != self->host) {
        Safefree(self->host);
        self->host = NULL;
    }
    len = strlen(path);
    Newx(self->path, len + 1, char);
    Copy(path, self->path, len + 1, char);

    redisOptions opts = {0};
    pre_connect_common(self, &opts);
    REDIS_OPTIONS_SET_UNIX(&opts, path);
    self->ac = redisAsyncConnectWithOptions(&opts);
    if (NULL == self->ac) {
        croak("cannot allocate memory");
    }

    connect_common(self);
}

void
disconnect(EV::Hiredis self);
CODE:
{
    if (NULL == self->ac) {
        emit_error_str(self, "not connected");
        return;
    }

    self->intentional_disconnect = 1;
    redisAsyncDisconnect(self->ac);
}

int
is_connected(EV::Hiredis self);
CODE:
{
    RETVAL = (NULL != self->ac) ? 1 : 0;
}
OUTPUT:
    RETVAL

SV*
connect_timeout(EV::Hiredis self, SV* timeout_ms = NULL);
CODE:
{
    if (NULL != timeout_ms && SvOK(timeout_ms)) {
        int ms = SvIV(timeout_ms);
        if (ms < 0) {
            croak("timeout must be non-negative");
        }
        if (NULL == self->connect_timeout) {
            Newx(self->connect_timeout, 1, struct timeval);
        }
        self->connect_timeout->tv_sec = ms / 1000;
        self->connect_timeout->tv_usec = (ms % 1000) * 1000;
    }

    if (NULL != self->connect_timeout) {
        RETVAL = newSViv(self->connect_timeout->tv_sec * 1000 +
                         self->connect_timeout->tv_usec / 1000);
    }
    else {
        RETVAL = &PL_sv_undef;
    }
}
OUTPUT:
    RETVAL

SV*
command_timeout(EV::Hiredis self, SV* timeout_ms = NULL);
CODE:
{
    if (NULL != timeout_ms && SvOK(timeout_ms)) {
        int ms = SvIV(timeout_ms);
        if (ms < 0) {
            croak("timeout must be non-negative");
        }
        if (NULL == self->command_timeout) {
            Newx(self->command_timeout, 1, struct timeval);
        }
        self->command_timeout->tv_sec = ms / 1000;
        self->command_timeout->tv_usec = (ms % 1000) * 1000;
    }

    if (NULL != self->command_timeout) {
        RETVAL = newSViv(self->command_timeout->tv_sec * 1000 +
                         self->command_timeout->tv_usec / 1000);
    }
    else {
        RETVAL = &PL_sv_undef;
    }
}
OUTPUT:
    RETVAL

CV*
on_error(EV::Hiredis self, CV* handler = NULL);
CODE:
{
    if (NULL != self->error_handler) {
        SvREFCNT_dec(self->error_handler);
        self->error_handler = NULL;
    }

    if (NULL != handler) {
        self->error_handler = SvREFCNT_inc(handler);
    }

    RETVAL = (CV*)self->error_handler;
}
OUTPUT:
    RETVAL

CV*
on_connect(EV::Hiredis self, CV* handler = NULL);
CODE:
{
    if (NULL != self->connect_handler) {
        SvREFCNT_dec(self->connect_handler);
        self->connect_handler = NULL;
    }

    if (NULL != handler) {
        self->connect_handler = SvREFCNT_inc(handler);
    }

    RETVAL = (CV*)self->connect_handler;
}
OUTPUT:
    RETVAL

CV*
on_disconnect(EV::Hiredis self, CV* handler = NULL);
CODE:
{
    if (NULL != self->disconnect_handler) {
        SvREFCNT_dec(self->disconnect_handler);
        self->disconnect_handler = NULL;
    }

    if (NULL != handler) {
        self->disconnect_handler = SvREFCNT_inc(handler);
    }

    RETVAL = (CV*)self->disconnect_handler;
}
OUTPUT:
    RETVAL

int
command(EV::Hiredis self, ...);
PREINIT:
    SV* cb;
    char** argv;
    size_t* argvlen;
    STRLEN len;
    int argc, i, persist;
    ev_hiredis_cb_t* cbt;
    ev_hiredis_wait_t* wt;
    char* p;
CODE:
{
    if (items <= 2) {
        croak("Usage: command(\"command\", ..., $callback)");
    }

    cb = ST(items - 1);
    if (!(SvROK(cb) && SvTYPE(SvRV(cb)) == SVt_PVCV)) {
        croak("last arguments should be CODE reference");
    }

    if (NULL == self->ac) {
        croak("connect required before call command");
    }

    argc = items - 2;
    Newx(argv, argc, char*);
    Newx(argvlen, argc, size_t);

    for (i = 0; i < argc; i++) {
        argv[i] = SvPV(ST(i + 1), len);
        argvlen[i] = len;
    }

    persist = (0 == strcasecmp(argv[0], "subscribe")
        || 0 == strcasecmp(argv[0], "psubscribe")
        || 0 == strcasecmp(argv[0], "ssubscribe")
        || 0 == strcasecmp(argv[0], "monitor"));

    if (self->max_pending > 0 && self->pending_count >= self->max_pending) {
        Newx(wt, 1, ev_hiredis_wait_t);
        Newx(wt->argv, argc, char*);
        Newx(wt->argvlen, argc, size_t);
        for (i = 0; i < argc; i++) {
            Newx(p, argvlen[i] + 1, char);
            Copy(argv[i], p, argvlen[i], char);
            p[argvlen[i]] = '\0';
            wt->argv[i] = p;
            wt->argvlen[i] = argvlen[i];
        }
        wt->argc = argc;
        wt->cb = SvREFCNT_inc(cb);
        wt->persist = persist;
        wt->queued_at = ev_now(self->loop);
        ngx_queue_init(&wt->queue);
        ngx_queue_insert_tail(&self->wait_queue, &wt->queue);
        self->waiting_count++;
        schedule_waiting_timer(self);
        RETVAL = REDIS_OK;

        Safefree(argv);
        Safefree(argvlen);
    }
    else {
        Newx(cbt, 1, ev_hiredis_cb_t);
        cbt->cb = SvREFCNT_inc(cb);
        cbt->skipped = 0;
        cbt->persist = persist;
        ngx_queue_init(&cbt->queue);
        ngx_queue_insert_tail(&self->cb_queue, &cbt->queue);
        self->pending_count++;

        RETVAL = redisAsyncCommandArgv(
            self->ac, EV__hiredis_reply_cb, (void*)cbt,
            argc, (const char**)argv, argvlen
        );

        if (REDIS_OK != RETVAL) {
            ngx_queue_remove(&cbt->queue);
            self->pending_count--;
            SvREFCNT_dec(cbt->cb);
            Safefree(cbt);
        }

        Safefree(argv);
        Safefree(argvlen);
    }
}
OUTPUT:
    RETVAL

void
reconnect(EV::Hiredis self, int enable, int delay_ms = 1000, int max_attempts = 0);
CODE:
{
    self->reconnect = enable ? 1 : 0;
    self->reconnect_delay_ms = delay_ms > 0 ? delay_ms : 1000;
    self->max_reconnect_attempts = max_attempts >= 0 ? max_attempts : 0;

    if (!enable && self->reconnect_timer_active && NULL != self->loop) {
        ev_timer_stop(self->loop, &self->reconnect_timer);
        self->reconnect_timer_active = 0;
    }
}

int
reconnect_enabled(EV::Hiredis self);
CODE:
{
    RETVAL = self->reconnect;
}
OUTPUT:
    RETVAL

int
pending_count(EV::Hiredis self);
CODE:
{
    RETVAL = self->pending_count;
}
OUTPUT:
    RETVAL

int
waiting_count(EV::Hiredis self);
CODE:
{
    RETVAL = self->waiting_count;
}
OUTPUT:
    RETVAL

int
max_pending(EV::Hiredis self, SV* limit = NULL);
CODE:
{
    if (NULL != limit && SvOK(limit)) {
        int val = SvIV(limit);
        if (val < 0) {
            croak("max_pending must be non-negative");
        }
        self->max_pending = val;

        /* When limit is increased or removed, send waiting commands */
        while (self->waiting_count > 0 &&
               (self->max_pending == 0 || self->pending_count < self->max_pending)) {
            send_next_waiting(self);
        }
    }
    RETVAL = self->max_pending;
}
OUTPUT:
    RETVAL

SV*
waiting_timeout(EV::Hiredis self, SV* timeout_ms = NULL);
CODE:
{
    if (NULL != timeout_ms && SvOK(timeout_ms)) {
        int ms = SvIV(timeout_ms);
        if (ms < 0) {
            croak("waiting_timeout must be non-negative");
        }
        self->waiting_timeout = ms / 1000.0;
        schedule_waiting_timer(self);
    }

    if (self->waiting_timeout > 0) {
        RETVAL = newSViv((int)(self->waiting_timeout * 1000));
    }
    else {
        RETVAL = &PL_sv_undef;
    }
}
OUTPUT:
    RETVAL

int
resume_waiting_on_reconnect(EV::Hiredis self, SV* value = NULL);
CODE:
{
    if (NULL != value && SvOK(value)) {
        self->resume_waiting_on_reconnect = SvTRUE(value) ? 1 : 0;
    }
    RETVAL = self->resume_waiting_on_reconnect;
}
OUTPUT:
    RETVAL

int
priority(EV::Hiredis self, SV* value = NULL);
CODE:
{
    if (NULL != value && SvOK(value)) {
        int prio = SvIV(value);
        self->priority = prio;
        if (NULL != self->ac) {
            redisLibevSetPriority(self->ac, prio);
        }
    }
    RETVAL = self->priority;
}
OUTPUT:
    RETVAL

void
skip_waiting(EV::Hiredis self);
CODE:
{
    clear_wait_queue(self, "skipped");
}

void
skip_pending(EV::Hiredis self);
CODE:
{
    ngx_queue_t* q;
    ngx_queue_t* next;
    ev_hiredis_cb_t* cbt;

    clear_wait_queue(self, "skipped");

    for (q = ngx_queue_head(&self->cb_queue);
         q != ngx_queue_sentinel(&self->cb_queue);
         q = next) {
        next = ngx_queue_next(q);
        cbt = ngx_queue_data(q, ev_hiredis_cb_t, queue);

        if (cbt == self->current_cb) {
            continue;
        }

        ngx_queue_remove(q);
        self->pending_count--;

        dSP;
        ENTER;
        SAVETMPS;
        PUSHMARK(SP);
        PUSHs(&PL_sv_undef);
        PUSHs(sv_2mortal(newSVpv("skipped", 0)));
        PUTBACK;
        call_sv(cbt->cb, G_DISCARD | G_EVAL);
        if (SvTRUE(ERRSV)) {
            warn("EV::Hiredis: exception in command callback: %s", SvPV_nolen(ERRSV));
        }
        FREETMPS;
        LEAVE;

        SvREFCNT_dec(cbt->cb);
        cbt->cb = NULL;
        cbt->skipped = 1;
    }
}
