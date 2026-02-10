#include "EXTERN.h"
#include "perl.h"
#include "XSUB.h"
#include "ppport.h"

#include "EVAPI.h"

#include "hiredis.h"
#include "async.h"
#include "libev_adapter.h"
#include "ngx-queue.h"

#ifdef EV_HIREDIS_SSL
#include "hiredis_ssl.h"
#endif

typedef struct ev_hiredis_s ev_hiredis_t;
typedef struct ev_hiredis_cb_s ev_hiredis_cb_t;
typedef struct ev_hiredis_wait_s ev_hiredis_wait_t;

typedef ev_hiredis_t* EV__Hiredis;
typedef struct ev_loop* EV__Loop;

/* Magic number to detect use-after-free in DESTROY */
#define EV_HIREDIS_MAGIC 0xDEADBEEF
#define EV_HIREDIS_FREED 0xFEEDFACE

struct ev_hiredis_s {
    unsigned int magic;  /* Set to EV_HIREDIS_MAGIC when alive */
    struct ev_loop* loop;
    redisAsyncContext* ac;
    SV* error_handler;
    SV* connect_handler;
    SV* disconnect_handler;
    SV* push_handler;
    struct timeval* connect_timeout;
    struct timeval* command_timeout;
    ngx_queue_t cb_queue;
    ngx_queue_t wait_queue;
    int pending_count;
    int waiting_count;
    int max_pending; /* 0 = unlimited */
    ev_hiredis_cb_t* current_cb; /* callback currently executing */
    int resume_waiting_on_reconnect; /* keep waiting queue on disconnect */
    int waiting_timeout_ms; /* max ms in waiting queue, 0 = unlimited */
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
    int in_cleanup; /* set during remove_cb_queue_sv to prevent re-entrant modification */
    int callback_depth; /* nesting depth of C-level callbacks invoking Perl code */
    int keepalive; /* TCP keepalive interval in seconds, 0 = disabled */
    int prefer_ipv4; /* prefer IPv4 DNS resolution */
    int prefer_ipv6; /* prefer IPv6 DNS resolution */
    char* source_addr; /* local address to bind to */
    unsigned int tcp_user_timeout; /* TCP_USER_TIMEOUT in ms, 0 = OS default */
    int cloexec; /* set SOCK_CLOEXEC on socket */
    int reuseaddr; /* set SO_REUSEADDR on socket */
    redisAsyncContext* ac_saved; /* saved ac pointer for deferred disconnect cleanup */
#ifdef EV_HIREDIS_SSL
    redisSSLContext* ssl_ctx;
#endif
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

/* Pre-allocated error message strings for common cases.
 * These are created once at module load and reused to avoid
 * repeated string allocation overhead. */
static SV* err_skipped = NULL;
static SV* err_waiting_timeout = NULL;
static SV* err_disconnected = NULL;

/* Fast check for persistent commands (subscribe, psubscribe, ssubscribe, monitor).
 * Uses first-character filtering to avoid unnecessary strcasecmp calls for
 * the majority of commands. */
static int is_persistent_command(const char* cmd) {
    char c = cmd[0];

    /* Fast path: check first character (case-insensitive) */
    if (c == 's' || c == 'S') {
        /* Could be subscribe or ssubscribe */
        if (0 == strcasecmp(cmd, "subscribe")) return 1;
        if (0 == strcasecmp(cmd, "ssubscribe")) return 1;
        return 0;
    }
    if (c == 'p' || c == 'P') {
        /* Could be psubscribe */
        return (0 == strcasecmp(cmd, "psubscribe"));
    }
    if (c == 'm' || c == 'M') {
        /* Could be monitor */
        return (0 == strcasecmp(cmd, "monitor"));
    }

    return 0;
}

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

/* Helper to invoke a callback with (undef, error) arguments.
 * Used throughout error paths to reduce code duplication. */
static void invoke_callback_error(SV* cb, SV* error_sv) {
    dSP;
    ENTER;
    SAVETMPS;
    PUSHMARK(SP);
    PUSHs(&PL_sv_undef);
    PUSHs(error_sv);
    PUTBACK;
    call_sv(cb, G_DISCARD | G_EVAL);
    if (SvTRUE(ERRSV)) {
        warn("EV::Hiredis: exception in command callback: %s", SvPV_nolen(ERRSV));
    }
    FREETMPS;
    LEAVE;
}

/* Check if DESTROY was called during a callback and deferred Safefree.
 * Call after decrementing callback_depth. Returns 1 if self was freed
 * (caller MUST NOT access self afterward). */
static int check_destroyed(EV__Hiredis self) {
    if (self->magic == EV_HIREDIS_FREED &&
        self->callback_depth == 0 &&
        self->current_cb == NULL) {
        if (self->ac_saved != NULL) {
            self->ac_saved->data = NULL;
            self->ac_saved = NULL;
        }
        Safefree(self);
        return 1;
    }
    return 0;
}

/* Timer stop helpers to reduce code duplication */
static void stop_waiting_timer(EV__Hiredis self) {
    if (self->waiting_timer_active && NULL != self->loop) {
        ev_timer_stop(self->loop, &self->waiting_timer);
        self->waiting_timer_active = 0;
    }
}

static void stop_reconnect_timer(EV__Hiredis self) {
    if (self->reconnect_timer_active && NULL != self->loop) {
        ev_timer_stop(self->loop, &self->reconnect_timer);
        self->reconnect_timer_active = 0;
    }
}

/* Helper to set/get a timeval timeout field.
 * If timeout_ms is provided and valid, sets the timeout.
 * Returns the current timeout value as SV (or undef if not set). */
/* Maximum timeout: ~24 days (fits safely in 32-bit calculations) */
#define MAX_TIMEOUT_MS 2000000000

static SV* timeout_accessor(struct timeval** tv_ptr, SV* timeout_ms) {
    if (NULL != timeout_ms && SvOK(timeout_ms)) {
        IV ms = SvIV(timeout_ms);
        if (ms < 0) {
            croak("timeout must be non-negative");
        }
        if (ms > MAX_TIMEOUT_MS) {
            croak("timeout too large (max %d ms)", MAX_TIMEOUT_MS);
        }
        if (NULL == *tv_ptr) {
            Newx(*tv_ptr, 1, struct timeval);
        }
        (*tv_ptr)->tv_sec = (long)(ms / 1000);
        (*tv_ptr)->tv_usec = (long)((ms % 1000) * 1000);
    }

    if (NULL != *tv_ptr) {
        return newSViv((IV)(*tv_ptr)->tv_sec * 1000 + (*tv_ptr)->tv_usec / 1000);
    }
    return &PL_sv_undef;
}

/* Helper to set/clear a callback handler field.
 * If called without handler (items == 1), clears the handler.
 * If called with handler, sets it (or clears if handler is undef/not CODE).
 * Returns the current handler (with refcount incremented) or undef. */
static SV* handler_accessor(SV** handler_ptr, SV* handler, int has_handler_arg) {
    /* Clear existing handler first - both no-arg calls and set calls clear first */
    if (NULL != *handler_ptr) {
        SvREFCNT_dec(*handler_ptr);
        *handler_ptr = NULL;
    }

    /* If a handler argument was provided and it's a valid CODE ref, set it */
    if (has_handler_arg && NULL != handler && SvOK(handler) && SvROK(handler) &&
        SvTYPE(SvRV(handler)) == SVt_PVCV) {
        *handler_ptr = SvREFCNT_inc(handler);
    }

    return (NULL != *handler_ptr)
        ? SvREFCNT_inc(*handler_ptr)
        : &PL_sv_undef;
}

/* Optimized version using pre-allocated SV error message.
 * Uses in_cleanup flag to prevent re-entrant queue modification from
 * user callbacks (e.g., if callback calls skip_pending). */
static void remove_cb_queue_sv(EV__Hiredis self, SV* error_sv) {
    ngx_queue_t* q;
    ev_hiredis_cb_t* cbt;
    int was_in_cleanup = self->in_cleanup;

    self->in_cleanup = 1;

    /* Use while loop with re-fetch of head each iteration.
     * This is safe against re-entrant modifications because we
     * re-check the queue state after each callback invocation. */
    while (!ngx_queue_empty(&self->cb_queue)) {
        q = ngx_queue_head(&self->cb_queue);
        cbt = ngx_queue_data(q, ev_hiredis_cb_t, queue);

        if (cbt == self->current_cb) {
            /* Skip current_cb - check if it's the only item left */
            if (ngx_queue_next(q) == ngx_queue_sentinel(&self->cb_queue)) {
                break;  /* Only current_cb remains, we're done */
            }
            /* Move to next item */
            q = ngx_queue_next(q);
            cbt = ngx_queue_data(q, ev_hiredis_cb_t, queue);
        }

        ngx_queue_remove(q);
        self->pending_count--;

        if (NULL != cbt->cb) {
            if (NULL != error_sv) {
                invoke_callback_error(cbt->cb, error_sv);
            }
            SvREFCNT_dec(cbt->cb);
        }
        Safefree(cbt);
    }

    self->in_cleanup = was_in_cleanup;
}

static void free_wait_entry(ev_hiredis_wait_t* wt) {
    int i;
    for (i = 0; i < wt->argc; i++) {
        Safefree(wt->argv[i]);
    }
    Safefree(wt->argv);
    Safefree(wt->argvlen);
    if (NULL != wt->cb) {
        SvREFCNT_dec(wt->cb);
    }
    Safefree(wt);
}

/* Optimized version using pre-allocated SV error message */
static void clear_wait_queue_sv(EV__Hiredis self, SV* error_sv) {
    ngx_queue_t* q;
    ev_hiredis_wait_t* wt;
    int was_in_cleanup = self->in_cleanup;

    /* Protect against re-entrancy: if a callback invokes skip_waiting() or
     * skip_pending(), they should no-op since we're already clearing. */
    self->in_cleanup = 1;

    while (!ngx_queue_empty(&self->wait_queue)) {
        q = ngx_queue_head(&self->wait_queue);
        wt = ngx_queue_data(q, ev_hiredis_wait_t, queue);
        ngx_queue_remove(q);
        self->waiting_count--;

        if (NULL != error_sv && NULL != wt->cb) {
            invoke_callback_error(wt->cb, error_sv);
        }

        free_wait_entry(wt);
    }

    self->in_cleanup = was_in_cleanup;
}

/* Forward declarations */
static void pre_connect_common(EV__Hiredis self, redisOptions* opts);
static void do_reconnect(EV__Hiredis self);
static void send_next_waiting(EV__Hiredis self);
static void schedule_waiting_timer(EV__Hiredis self);
static void expire_waiting_commands(EV__Hiredis self);
static void schedule_reconnect(EV__Hiredis self);
static void EV__hiredis_connect_cb(redisAsyncContext* c, int status);
static void EV__hiredis_disconnect_cb(redisAsyncContext* c, int status);
static void EV__hiredis_push_cb(redisAsyncContext* ac, void* reply_ptr);
static SV* EV__hiredis_decode_reply(redisReply* reply);

/* Clear stored connection parameters (host/path) before new connection */
static void clear_connection_params(EV__Hiredis self) {
    if (NULL != self->host) {
        Safefree(self->host);
        self->host = NULL;
    }
    if (NULL != self->path) {
        Safefree(self->path);
        self->path = NULL;
    }
}

static void reconnect_timer_cb(EV_P_ ev_timer* w, int revents) {
    EV__Hiredis self = (EV__Hiredis)w->data;

    (void)loop;
    (void)revents;

    /* Safety check: if self is NULL or already freed, skip */
    if (self == NULL || self->magic != EV_HIREDIS_MAGIC) return;

    self->reconnect_timer_active = 0;
    self->callback_depth++;
    do_reconnect(self);
    self->callback_depth--;
    if (check_destroyed(self)) return;
}

static void schedule_reconnect(EV__Hiredis self) {
    ev_tstamp delay;

    if (!self->reconnect) return;
    if (NULL == self->loop) return;
    if (self->max_reconnect_attempts > 0 &&
        self->reconnect_attempts >= self->max_reconnect_attempts) {
        /* Clear waiting queue that was preserved for reconnect - reconnect has
         * permanently failed, so these commands will never be sent. */
        clear_wait_queue_sv(self, sv_2mortal(newSVpv("reconnect error: max attempts reached", 0)));
        stop_waiting_timer(self);
        emit_error_str(self, "reconnect error: max attempts reached");
        return;
    }

    self->reconnect_attempts++;
    delay = self->reconnect_delay_ms / 1000.0;

    ev_timer_init(&self->reconnect_timer, reconnect_timer_cb, delay, 0);
    self->reconnect_timer.data = (void*)self;
    ev_timer_start(self->loop, &self->reconnect_timer);
    self->reconnect_timer_active = 1;
}

/* Expire waiting commands that have exceeded waiting_timeout.
 * Uses in_cleanup flag to prevent re-entrant queue modification from
 * user callbacks (e.g., if callback calls skip_waiting). */
static void expire_waiting_commands(EV__Hiredis self) {
    ngx_queue_t* q;
    ev_hiredis_wait_t* wt;
    ev_tstamp now;
    ev_tstamp timeout;
    int was_in_cleanup = self->in_cleanup;

    self->in_cleanup = 1;
    now = ev_now(self->loop);
    /* Capture timeout at start - callbacks may modify self->waiting_timeout_ms
     * and we need consistent behavior for the entire batch. */
    timeout = self->waiting_timeout_ms / 1000.0;

    /* Use while loop with re-fetch of head each iteration.
     * This is safe against re-entrant modifications. */
    while (!ngx_queue_empty(&self->wait_queue)) {
        q = ngx_queue_head(&self->wait_queue);
        wt = ngx_queue_data(q, ev_hiredis_wait_t, queue);

        if (now - wt->queued_at >= timeout) {
            ngx_queue_remove(q);
            self->waiting_count--;

            if (NULL != wt->cb) {
                invoke_callback_error(wt->cb, err_waiting_timeout);
            }

            free_wait_entry(wt);
        }
        else {
            /* Queue is FIFO with monotonically increasing queued_at times.
             * If this entry hasn't expired, neither have any following entries. */
            break;
        }
    }

    self->in_cleanup = was_in_cleanup;
}

static void waiting_timer_cb(EV_P_ ev_timer* w, int revents) {
    EV__Hiredis self = (EV__Hiredis)w->data;

    (void)loop;
    (void)revents;

    /* Safety check: if self is NULL or already freed, skip */
    if (self == NULL || self->magic != EV_HIREDIS_MAGIC) return;

    self->waiting_timer_active = 0;
    self->callback_depth++;
    expire_waiting_commands(self);
    schedule_waiting_timer(self);
    self->callback_depth--;
    if (check_destroyed(self)) return;
}

static void schedule_waiting_timer(EV__Hiredis self) {
    ngx_queue_t* q;
    ev_hiredis_wait_t* wt;
    ev_tstamp now, expires_at, delay;

    /* Use helper which includes NULL loop check */
    stop_waiting_timer(self);

    if (NULL == self->loop) return;
    if (self->waiting_timeout_ms <= 0) return;
    if (ngx_queue_empty(&self->wait_queue)) return;

    q = ngx_queue_head(&self->wait_queue);
    wt = ngx_queue_data(q, ev_hiredis_wait_t, queue);

    now = ev_now(self->loop);
    expires_at = wt->queued_at + self->waiting_timeout_ms / 1000.0;
    delay = expires_at - now;
    if (delay < 0) delay = 0;

    ev_timer_init(&self->waiting_timer, waiting_timer_cb, delay, 0);
    self->waiting_timer.data = (void*)self;
    ev_timer_start(self->loop, &self->waiting_timer);
    self->waiting_timer_active = 1;
}

static void do_reconnect(EV__Hiredis self) {
    redisOptions opts;
    memset(&opts, 0, sizeof(opts));

    if (NULL == self->loop) {
        /* Object is being destroyed */
        return;
    }

    if (NULL != self->ac) {
        /* Already connected or connecting */
        return;
    }

    pre_connect_common(self, &opts);

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

    self->ac_saved = NULL;
    self->ac->data = (void*)self;

#ifdef EV_HIREDIS_SSL
    if (NULL != self->ssl_ctx) {
        if (REDIS_OK != redisInitiateSSLWithContext(&self->ac->c, self->ssl_ctx)) {
            emit_error_str(self, "reconnect error: SSL initiation failed");
            redisAsyncFree(self->ac);
            self->ac = NULL;
            schedule_reconnect(self);
            return;
        }
    }
#endif

    if (self->keepalive > 0) {
        redisEnableKeepAliveWithInterval(&self->ac->c, self->keepalive);
    }
    if (self->tcp_user_timeout > 0) {
        redisSetTcpUserTimeout(&self->ac->c, self->tcp_user_timeout);
    }

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

    redisAsyncSetConnectCallbackNC(self->ac, EV__hiredis_connect_cb);
    redisAsyncSetDisconnectCallback(self->ac, (redisDisconnectCallback*)EV__hiredis_disconnect_cb);
    if (NULL != self->push_handler) {
        redisAsyncSetPushCallback(self->ac, EV__hiredis_push_cb);
    }

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

    /* Safety check: if self is NULL or already freed, skip callback */
    if (self == NULL) return;
    if (self->magic != EV_HIREDIS_MAGIC) return;

    self->callback_depth++;

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

        /* Resume waiting commands if any.
         * Check self->ac and intentional_disconnect in case connect_handler
         * triggered disconnect (redisAsyncDisconnect is async, so ac isn't
         * immediately NULL). */
        while (NULL != self->ac && !self->intentional_disconnect &&
               !ngx_queue_empty(&self->wait_queue)) {
            if (self->max_pending > 0 && self->pending_count >= self->max_pending) {
                break;
            }
            send_next_waiting(self);
        }
    }

    self->callback_depth--;
    check_destroyed(self);
}

static void EV__hiredis_disconnect_cb(redisAsyncContext* c, int status) {
    EV__Hiredis self = (EV__Hiredis)c->data;
    SV* error_sv;
    int should_reconnect = 0;
    int was_intentional;

    /* Safety check: if self is NULL or already freed, skip callback */
    if (self == NULL) return;
    if (self->magic != EV_HIREDIS_MAGIC) return;

    was_intentional = self->intentional_disconnect;
    self->intentional_disconnect = 0;

    self->ac = NULL;
    self->ac_saved = NULL; /* disconnect callback fired normally */
    self->callback_depth++;

    if (REDIS_OK == status) {
        error_sv = err_disconnected;
    }
    else {
        error_sv = sv_2mortal(newSVpv(c->errstr, 0));
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

    remove_cb_queue_sv(self, error_sv);

    /* Clear waiting queue if:
     * - resume_waiting_on_reconnect is disabled, OR
     * - this was an intentional disconnect (no reconnect will happen)
     * When was_intentional is true, we won't reconnect, so waiting commands
     * must be cancelled. */
    if (!self->resume_waiting_on_reconnect || was_intentional) {
        clear_wait_queue_sv(self, error_sv);
        stop_waiting_timer(self);
    }

    if (should_reconnect) {
        schedule_reconnect(self);
    }

    self->callback_depth--;
    check_destroyed(self);
}

static void EV__hiredis_push_cb(redisAsyncContext* ac, void* reply_ptr) {
    EV__Hiredis self = (EV__Hiredis)ac->data;
    redisReply* reply = (redisReply*)reply_ptr;

    if (self == NULL) return;
    if (self->magic != EV_HIREDIS_MAGIC) return;
    if (NULL == self->push_handler) return;
    if (NULL == reply) return;

    self->callback_depth++;

    {
        dSP;

        ENTER;
        SAVETMPS;

        PUSHMARK(SP);
        XPUSHs(sv_2mortal(EV__hiredis_decode_reply(reply)));
        PUTBACK;

        call_sv(self->push_handler, G_DISCARD | G_EVAL);
        if (SvTRUE(ERRSV)) {
            warn("EV::Hiredis: exception in push handler: %s", SvPV_nolen(ERRSV));
        }

        FREETMPS;
        LEAVE;
    }

    self->callback_depth--;
    check_destroyed(self);
}

static void pre_connect_common(EV__Hiredis self, redisOptions* opts) {
    if (NULL != self->connect_timeout) {
        opts->connect_timeout = self->connect_timeout;
    }
    if (NULL != self->command_timeout) {
        opts->command_timeout = self->command_timeout;
    }
    if (self->prefer_ipv4) {
        opts->options |= REDIS_OPT_PREFER_IPV4;
    }
    else if (self->prefer_ipv6) {
        opts->options |= REDIS_OPT_PREFER_IPV6;
    }
    if (self->cloexec) {
        opts->options |= REDIS_OPT_SET_SOCK_CLOEXEC;
    }
    if (self->reuseaddr) {
        opts->options |= REDIS_OPT_REUSEADDR;
    }
    if (NULL != self->source_addr) {
        opts->endpoint.tcp.source_addr = self->source_addr;
    }
}

static void connect_common(EV__Hiredis self) {
    int r;

    self->ac_saved = NULL;
    self->ac->data = (void*)self;

#ifdef EV_HIREDIS_SSL
    if (NULL != self->ssl_ctx) {
        if (REDIS_OK != redisInitiateSSLWithContext(&self->ac->c, self->ssl_ctx)) {
            SV* sv_error = sv_2mortal(newSVpvf("connect error: SSL initiation failed: %s",
                self->ac->errstr[0] ? self->ac->errstr : "unknown error"));
            redisAsyncFree(self->ac);
            self->ac = NULL;
            emit_error(self, sv_error);
            return;
        }
    }
#endif

    if (self->keepalive > 0) {
        redisEnableKeepAliveWithInterval(&self->ac->c, self->keepalive);
    }
    if (self->tcp_user_timeout > 0) {
        redisSetTcpUserTimeout(&self->ac->c, self->tcp_user_timeout);
    }

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

    redisAsyncSetConnectCallbackNC(self->ac, EV__hiredis_connect_cb);
    redisAsyncSetDisconnectCallback(self->ac, (redisDisconnectCallback*)EV__hiredis_disconnect_cb);
    if (NULL != self->push_handler) {
        redisAsyncSetPushCallback(self->ac, EV__hiredis_push_cb);
    }

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

    /* Safety check: if self is NULL or memory is corrupted, skip callback */
    if (self == NULL) {
        /* Cannot access queue - just free the callback struct.
         * Always decrement refcount regardless of persist flag since
         * the callback will never be invoked again. */
        SvREFCNT_dec(cbt->cb);
        Safefree(cbt);
        return;
    }

    /* If self is marked as freed (during DESTROY), we still invoke the
     * callback with an error, but skip any self->field access afterward. */
    if (self->magic == EV_HIREDIS_FREED) {
        ngx_queue_remove(&cbt->queue);
        /* Call user callback with error */
        invoke_callback_error(cbt->cb, sv_2mortal(newSVpv(c->errstr[0] ? c->errstr : "disconnected", 0)));
        /* Always decrement refcount during destruction - even persistent callbacks
         * won't be called again after the object is freed. */
        SvREFCNT_dec(cbt->cb);
        Safefree(cbt);
        return;
    }

    /* Unknown magic - memory corruption, skip.
     * Always decrement refcount since callback will never be invoked again. */
    if (self->magic != EV_HIREDIS_MAGIC) {
        ngx_queue_remove(&cbt->queue);
        SvREFCNT_dec(cbt->cb);
        Safefree(cbt);
        return;
    }

    self->current_cb = cbt;

    if (NULL == reply) {
        sv_err = sv_2mortal(newSVpv(c->errstr, 0));
        invoke_callback_error(cbt->cb, sv_err);
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

    /* If DESTROY was called during our callback (e.g., user undef'd $redis),
     * self->magic is EV_HIREDIS_FREED but self is still valid (DESTROY defers
     * Safefree when current_cb is set). Complete cleanup here. */
    if (self->magic == EV_HIREDIS_FREED) {
        if (cbt->cb) SvREFCNT_dec(cbt->cb);
        Safefree(cbt);
        if (self->ac_saved != NULL) {
            self->ac_saved->data = NULL;
            self->ac_saved = NULL;
        }
        Safefree(self);
        return;
    }

    if (cbt->skipped) {
        /* Defensive check: handles edge case where callback is marked skipped
         * during its own execution (e.g., via reentrant event loop where a
         * nested callback overwrites current_cb, allowing skip_pending to
         * process this callback). ngx_queue_remove is safe here due to
         * ngx_queue_init in skip_pending. Don't decrement pending_count since
         * skip_pending already did when it set skipped=1. */
        ngx_queue_remove(&cbt->queue);
        Safefree(cbt);
        self->callback_depth++;
        send_next_waiting(self);
        self->callback_depth--;
        check_destroyed(self);
        return;
    }

    if (0 == cbt->persist) {
        /* Remove from queue BEFORE SvREFCNT_dec. The SvREFCNT_dec may free a
         * closure that holds the last reference to this object, triggering
         * DESTROY. If cbt is still in the queue, DESTROY's remove_cb_queue_sv
         * would double-free it. Wrapping in callback_depth defers DESTROY's
         * Safefree(self) so we can safely access self afterward. */
        ngx_queue_remove(&cbt->queue);
        self->pending_count--;
        self->callback_depth++;
        SvREFCNT_dec(cbt->cb);
        Safefree(cbt);
        send_next_waiting(self);
        self->callback_depth--;
        check_destroyed(self);
    }
}

/* Send waiting commands to Redis. Uses iterative loop instead of recursion
 * to avoid stack overflow when many commands fail consecutively. */
static void send_next_waiting(EV__Hiredis self) {
    ngx_queue_t* q;
    ev_hiredis_wait_t* wt;
    ev_hiredis_cb_t* cbt;
    int r;

    while (1) {
        /* Check preconditions each iteration - they may change after callbacks */
        if (NULL == self->ac) return;
        if (ngx_queue_empty(&self->wait_queue)) return;
        if (self->max_pending > 0 && self->pending_count >= self->max_pending) return;

        q = ngx_queue_head(&self->wait_queue);
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

            invoke_callback_error(cbt->cb, sv_2mortal(newSVpv(
                (self->ac && self->ac->errstr[0]) ? self->ac->errstr : "command failed", 0)));

            SvREFCNT_dec(cbt->cb);
            Safefree(cbt);
            free_wait_entry(wt);
            /* Continue loop to try next waiting command */
            continue;
        }

        free_wait_entry(wt);
        /* Success - command sent, exit loop */
        return;
    }
}

MODULE = EV::Hiredis PACKAGE = EV::Hiredis

BOOT:
{
    I_EV_API("EV::Hiredis");

    /* Pre-allocate common error message strings for reuse.
     * These are immortal (never freed) to avoid refcount management. */
    err_skipped = newSVpvs_share("skipped");
    SvREADONLY_on(err_skipped);

    err_waiting_timeout = newSVpvs_share("waiting timeout");
    SvREADONLY_on(err_waiting_timeout);

    err_disconnected = newSVpvs_share("disconnected");
    SvREADONLY_on(err_disconnected);
#ifdef EV_HIREDIS_SSL
    redisInitOpenSSL();
#endif
}

EV::Hiredis
_new(char* class, EV::Loop loop);
CODE:
{
    PERL_UNUSED_VAR(class);
    Newxz(RETVAL, 1, ev_hiredis_t);
    RETVAL->magic = EV_HIREDIS_MAGIC;
    ngx_queue_init(&RETVAL->cb_queue);
    ngx_queue_init(&RETVAL->wait_queue);
    RETVAL->loop = loop;
    RETVAL->cloexec = 1;
}
OUTPUT:
    RETVAL

void
DESTROY(EV::Hiredis self);
CODE:
{
    redisAsyncContext* ac_to_free;
    int skip_cb_cleanup = 0;

    /* Check for use-after-free: if magic number is wrong, this object
     * was already freed and memory is being reused. Skip cleanup. */
    if (self->magic != EV_HIREDIS_MAGIC) {
        if (self->magic == EV_HIREDIS_FREED) {
            /* Already destroyed - this is a double-free at Perl level */
            return;
        }
        /* Unknown magic - memory corruption or uninitialized */
        return;
    }

    /* Mark as freed FIRST to prevent re-entrant DESTROY */
    self->magic = EV_HIREDIS_FREED;

    /* Stop timers BEFORE PL_dirty check. Timer callbacks have self as data
     * pointer, so we must stop them before freeing self to prevent UAF.
     * The stop helpers check for NULL loop, so this is safe even if loop
     * is already destroyed. */
    stop_reconnect_timer(self);
    stop_waiting_timer(self);

    /* During global destruction (PL_dirty), the EV loop and other Perl
     * objects may already be destroyed. Clean up hiredis and our own memory
     * but don't invoke Perl-level handlers.
     * CRITICAL: We must call redisAsyncFree to stop the libev adapter's
     * watchers and free the redisAsyncContext. Without this, the adapter's
     * ev_io/ev_timer watchers remain registered in the EV loop with dangling
     * data pointers, causing SEGV during process cleanup. */
    if (PL_dirty) {
        if (NULL != self->ac) {
            self->ac->data = NULL;  /* prevent callbacks from accessing self */
            redisAsyncFree(self->ac);
            self->ac = NULL;
        }
        if (NULL != self->ac_saved) {
            self->ac_saved->data = NULL;
            self->ac_saved = NULL;
        }
        if (NULL != self->host) Safefree(self->host);
        if (NULL != self->path) Safefree(self->path);
        if (NULL != self->source_addr) Safefree(self->source_addr);
        if (NULL != self->connect_timeout) Safefree(self->connect_timeout);
        if (NULL != self->command_timeout) Safefree(self->command_timeout);
#ifdef EV_HIREDIS_SSL
        if (NULL != self->ssl_ctx) {
            redisFreeSSLContext(self->ssl_ctx);
            self->ssl_ctx = NULL;
        }
#endif
        Safefree(self);
        return;
    }

    self->reconnect = 0;

    /* CRITICAL: Set self->ac to NULL BEFORE calling redisAsyncFree.
     * redisAsyncFree triggers reply callbacks, which call send_next_waiting,
     * which checks self->ac != NULL before issuing commands. If we don't
     * clear self->ac first, send_next_waiting will try to call
     * redisAsyncCommandArgv during the teardown, causing heap corruption. */
    self->loop = NULL;
    ac_to_free = self->ac;
    self->ac = NULL;
    if (NULL != ac_to_free) {
        /* If inside a hiredis callback (REDIS_IN_CALLBACK), redisAsyncFree
         * will be deferred. hiredis will fire pending reply callbacks later
         * via __redisAsyncFree. NULL ac->data so those callbacks see NULL self
         * and handle cleanup without accessing freed memory. */
        if (ac_to_free->c.flags & REDIS_IN_CALLBACK) {
            ac_to_free->data = NULL;
            skip_cb_cleanup = 1;
        }
        redisAsyncFree(ac_to_free);
    }
    /* If disconnect() was called from inside a callback, ac_saved points to
     * the deferred async context. NULL its data pointer to prevent the
     * deferred disconnect callback from accessing freed self. */
    if (self->ac_saved != NULL) {
        self->ac_saved->data = NULL;
        self->ac_saved = NULL;
        skip_cb_cleanup = 1;
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
    if (NULL != self->push_handler) {
        SvREFCNT_dec(self->push_handler);
        self->push_handler = NULL;
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
    if (NULL != self->source_addr) {
        Safefree(self->source_addr);
        self->source_addr = NULL;
    }
#ifdef EV_HIREDIS_SSL
    if (NULL != self->ssl_ctx) {
        redisFreeSSLContext(self->ssl_ctx);
        self->ssl_ctx = NULL;
    }
#endif

    clear_wait_queue_sv(self, err_disconnected);
    if (!skip_cb_cleanup) {
        /* Safe to free cbts ourselves — hiredis has no deferred references. */
        remove_cb_queue_sv(self, NULL);
    }
    /* else: hiredis still holds references to our cbts (deferred free/disconnect).
     * reply_cb will handle cbt cleanup when called with self == NULL. */

    if (self->current_cb != NULL || self->callback_depth > 0) {
        /* Inside a C-level callback that invokes Perl code.
         * Defer Safefree — the callback will call check_destroyed()
         * after decrementing callback_depth to complete cleanup. */
    }
    else {
        if (self->ac_saved != NULL) {
            self->ac_saved->data = NULL;
            self->ac_saved = NULL;
        }
        Safefree(self);
    }
}

void
connect(EV::Hiredis self, char* hostname, int port = 6379);
CODE:
{
    size_t len;
    redisOptions opts;

    if (NULL != self->ac) {
        croak("already connected");
    }

    clear_connection_params(self);
    len = strlen(hostname);
    Newx(self->host, len + 1, char);
    Copy(hostname, self->host, len + 1, char);
    self->port = port;

    memset(&opts, 0, sizeof(opts));
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
    redisOptions opts;

    if (NULL != self->ac) {
        croak("already connected");
    }

    clear_connection_params(self);
    len = strlen(path);
    Newx(self->path, len + 1, char);
    Copy(path, self->path, len + 1, char);

    memset(&opts, 0, sizeof(opts));
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
        return;  /* Already disconnected - no-op */
    }

    self->intentional_disconnect = 1;
    /* Only save ac pointer for deferred disconnect. When REDIS_IN_CALLBACK is
     * set, redisAsyncDisconnect defers the actual free — we need ac_saved so
     * DESTROY can NULL ac->data to prevent use-after-free if the object is
     * destroyed before the deferred disconnect callback runs.
     *
     * When NOT in a callback, redisAsyncDisconnect fires synchronously:
     * disconnect_cb runs and handles all cleanup. Setting ac_saved in that
     * case is dangerous — if DESTROY fires during the synchronous reply
     * callback processing (e.g., SvREFCNT_dec of a closure drops the last
     * reference), DESTROY NULLs ac->data, causing disconnect_cb to return
     * early without clearing ac_saved. The ac is then freed, leaving
     * ac_saved as a dangling pointer. */
    if (self->ac->c.flags & REDIS_IN_CALLBACK) {
        self->ac_saved = self->ac;
    }
    redisAsyncDisconnect(self->ac);
    self->ac = NULL;
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
    RETVAL = timeout_accessor(&self->connect_timeout, timeout_ms);
}
OUTPUT:
    RETVAL

SV*
command_timeout(EV::Hiredis self, SV* timeout_ms = NULL);
CODE:
{
    RETVAL = timeout_accessor(&self->command_timeout, timeout_ms);
    /* Apply to active connection immediately */
    if (NULL != timeout_ms && SvOK(timeout_ms) && NULL != self->ac && NULL != self->command_timeout) {
        redisAsyncSetTimeout(self->ac, *self->command_timeout);
    }
}
OUTPUT:
    RETVAL

SV*
on_error(EV::Hiredis self, SV* handler = NULL);
CODE:
{
    RETVAL = handler_accessor(&self->error_handler, handler, items > 1);
}
OUTPUT:
    RETVAL

SV*
on_connect(EV::Hiredis self, SV* handler = NULL);
CODE:
{
    RETVAL = handler_accessor(&self->connect_handler, handler, items > 1);
}
OUTPUT:
    RETVAL

SV*
on_disconnect(EV::Hiredis self, SV* handler = NULL);
CODE:
{
    RETVAL = handler_accessor(&self->disconnect_handler, handler, items > 1);
}
OUTPUT:
    RETVAL

SV*
on_push(EV::Hiredis self, SV* handler = NULL);
CODE:
{
    RETVAL = handler_accessor(&self->push_handler, handler, items > 1);
    /* Register/unregister push callback with hiredis if connected */
    if (items > 1 && NULL != self->ac) {
        if (NULL != self->push_handler) {
            redisAsyncSetPushCallback(self->ac, EV__hiredis_push_cb);
        } else {
            redisAsyncSetPushCallback(self->ac, NULL);
        }
    }
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
        croak("last argument should be CODE reference");
    }

    if (NULL == self->ac) {
        croak("connection required before calling command");
    }

    argc = items - 2;
    Newx(argv, argc, char*);
    Newx(argvlen, argc, size_t);

    for (i = 0; i < argc; i++) {
        argv[i] = SvPV(ST(i + 1), len);
        argvlen[i] = len;
    }

    persist = is_persistent_command(argv[0]);

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

            invoke_callback_error(cbt->cb, sv_2mortal(newSVpv(
                (self->ac && self->ac->errstr[0]) ? self->ac->errstr : "command failed", 0)));

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
    if (delay_ms < 0) {
        croak("reconnect_delay must be non-negative");
    }
    if (delay_ms > MAX_TIMEOUT_MS) {
        croak("reconnect_delay too large (max %d ms)", MAX_TIMEOUT_MS);
    }
    self->reconnect = enable ? 1 : 0;
    self->reconnect_delay_ms = delay_ms > 0 ? delay_ms : 1000;
    self->max_reconnect_attempts = max_attempts >= 0 ? max_attempts : 0;

    if (!enable) {
        stop_reconnect_timer(self);
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

        /* When limit is increased or removed, send waiting commands.
         * callback_depth protects against DESTROY if a failed command's
         * error callback drops the last Perl reference to self. */
        self->callback_depth++;
        while (NULL != self->ac && self->waiting_count > 0 &&
               (self->max_pending == 0 || self->pending_count < self->max_pending)) {
            send_next_waiting(self);
        }
        self->callback_depth--;
        if (check_destroyed(self)) XSRETURN_IV(0);
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
        IV ms = SvIV(timeout_ms);
        if (ms < 0) {
            croak("waiting_timeout must be non-negative");
        }
        if (ms > MAX_TIMEOUT_MS) {
            croak("waiting_timeout too large (max %d ms)", MAX_TIMEOUT_MS);
        }
        self->waiting_timeout_ms = (int)ms;
        schedule_waiting_timer(self);
    }

    if (self->waiting_timeout_ms > 0) {
        RETVAL = newSViv((IV)self->waiting_timeout_ms);
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
        /* Clamp priority to libev valid range: EV_MINPRI (-2) to EV_MAXPRI (+2) */
        if (prio < -2) prio = -2;
        if (prio > 2) prio = 2;
        self->priority = prio;
        if (NULL != self->ac) {
            redisLibevSetPriority(self->ac, prio);
        }
    }
    RETVAL = self->priority;
}
OUTPUT:
    RETVAL

int
keepalive(EV::Hiredis self, SV* value = NULL);
CODE:
{
    if (NULL != value && SvOK(value)) {
        int interval = SvIV(value);
        if (interval < 0) croak("keepalive interval must be non-negative");
        self->keepalive = interval;
        if (NULL != self->ac && interval > 0) {
            redisEnableKeepAliveWithInterval(&self->ac->c, interval);
        }
    }
    RETVAL = self->keepalive;
}
OUTPUT:
    RETVAL

int
prefer_ipv4(EV::Hiredis self, SV* value = NULL);
CODE:
{
    if (NULL != value && SvOK(value)) {
        self->prefer_ipv4 = SvIV(value) ? 1 : 0;
        if (self->prefer_ipv4) self->prefer_ipv6 = 0;
    }
    RETVAL = self->prefer_ipv4;
}
OUTPUT:
    RETVAL

int
prefer_ipv6(EV::Hiredis self, SV* value = NULL);
CODE:
{
    if (NULL != value && SvOK(value)) {
        self->prefer_ipv6 = SvIV(value) ? 1 : 0;
        if (self->prefer_ipv6) self->prefer_ipv4 = 0;
    }
    RETVAL = self->prefer_ipv6;
}
OUTPUT:
    RETVAL

SV*
source_addr(EV::Hiredis self, SV* value = NULL);
CODE:
{
    if (items > 1) {
        if (NULL != self->source_addr) {
            Safefree(self->source_addr);
            self->source_addr = NULL;
        }
        if (NULL != value && SvOK(value)) {
            STRLEN len;
            const char* addr = SvPV(value, len);
            Newx(self->source_addr, len + 1, char);
            Copy(addr, self->source_addr, len + 1, char);
        }
    }
    if (NULL != self->source_addr) {
        RETVAL = newSVpv(self->source_addr, 0);
    } else {
        RETVAL = &PL_sv_undef;
    }
}
OUTPUT:
    RETVAL

unsigned int
tcp_user_timeout(EV::Hiredis self, SV* value = NULL);
CODE:
{
    if (NULL != value && SvOK(value)) {
        IV ms = SvIV(value);
        if (ms < 0) {
            croak("tcp_user_timeout must be non-negative");
        }
        if (ms > MAX_TIMEOUT_MS) {
            croak("tcp_user_timeout too large (max %d ms)", MAX_TIMEOUT_MS);
        }
        self->tcp_user_timeout = (unsigned int)ms;
    }
    RETVAL = self->tcp_user_timeout;
}
OUTPUT:
    RETVAL

int
cloexec(EV::Hiredis self, SV* value = NULL);
CODE:
{
    if (NULL != value && SvOK(value)) {
        self->cloexec = SvIV(value) ? 1 : 0;
    }
    RETVAL = self->cloexec;
}
OUTPUT:
    RETVAL

int
reuseaddr(EV::Hiredis self, SV* value = NULL);
CODE:
{
    if (NULL != value && SvOK(value)) {
        self->reuseaddr = SvIV(value) ? 1 : 0;
    }
    RETVAL = self->reuseaddr;
}
OUTPUT:
    RETVAL

void
skip_waiting(EV::Hiredis self);
CODE:
{
    /* If cleanup is already in progress (e.g., during expire_waiting_commands
     * or disconnect callback), don't modify the wait_queue. */
    if (self->in_cleanup) {
        return;
    }
    clear_wait_queue_sv(self, err_skipped);
    stop_waiting_timer(self);
}

void
skip_pending(EV::Hiredis self);
CODE:
{
    ngx_queue_t* q;
    ngx_queue_t* next;
    ev_hiredis_cb_t* cbt;

    /* If cleanup is already in progress (e.g., during disconnect callback),
     * don't modify the cb_queue - it's already being processed with error
     * callbacks. The waiting queue is safe to clear regardless. */
    if (self->in_cleanup) {
        clear_wait_queue_sv(self, err_skipped);
        stop_waiting_timer(self);
        return;
    }

    clear_wait_queue_sv(self, err_skipped);
    stop_waiting_timer(self);

    /* Protect cb_queue iteration from re-entrancy. If a user callback
     * calls skip_pending() again, the in_cleanup check above will return.
     * Use save/restore pattern consistent with other cleanup functions. */
    {
    int was_in_cleanup = self->in_cleanup;
    self->in_cleanup = 1;

    for (q = ngx_queue_head(&self->cb_queue);
         q != ngx_queue_sentinel(&self->cb_queue);
         q = next) {
        next = ngx_queue_next(q);
        cbt = ngx_queue_data(q, ev_hiredis_cb_t, queue);

        if (cbt == self->current_cb) {
            /* Don't skip the currently executing callback - it will complete
             * normally. User code can check error in the callback if needed. */
            continue;
        }

        /* Mark as skipped FIRST to prevent double callback invocation if
         * invoke_callback_error re-enters the event loop and causes
         * EV__hiredis_reply_cb to fire before we finish this loop. */
        cbt->skipped = 1;

        /* Remove from our tracking queue. The hiredis library still has a
         * reference to cbt and will call EV__hiredis_reply_cb when the
         * Redis reply arrives (or on disconnect). That callback will see
         * skipped=1 and free the cbt. */
        ngx_queue_remove(q);
        /* Re-initialize the queue node to make any subsequent remove safe.
         * This protects against re-entrant scenarios where a nested callback
         * overwrites current_cb, allowing skip_pending to process callbacks
         * that are still on the call stack. When those callbacks return,
         * they may try to remove themselves again at line 779. */
        ngx_queue_init(q);
        self->pending_count--;

        /* Save and clear callback BEFORE invoking. If the user callback
         * re-enters the event loop and a Redis reply arrives, the reply
         * handler will see skipped=1 and free cbt. By clearing cbt->cb
         * first, we avoid use-after-free when we decrement refcount. */
        {
            SV* cb_to_invoke = cbt->cb;
            cbt->cb = NULL;

            invoke_callback_error(cb_to_invoke, err_skipped);
            SvREFCNT_dec(cb_to_invoke);
        }
    }

    self->in_cleanup = was_in_cleanup;
    }
}

int
has_ssl(char* class);
CODE:
{
    PERL_UNUSED_VAR(class);
#ifdef EV_HIREDIS_SSL
    RETVAL = 1;
#else
    RETVAL = 0;
#endif
}
OUTPUT:
    RETVAL

#ifdef EV_HIREDIS_SSL

void
_setup_ssl_context(EV::Hiredis self, SV* cacert, SV* capath, SV* cert, SV* key, SV* server_name, int verify = 1);
CODE:
{
    redisSSLContextError ssl_error = REDIS_SSL_CTX_NONE;
    redisSSLOptions ssl_opts;

    memset(&ssl_opts, 0, sizeof(ssl_opts));
    ssl_opts.cacert_filename = (SvOK(cacert)) ? SvPV_nolen(cacert) : NULL;
    ssl_opts.capath = (SvOK(capath)) ? SvPV_nolen(capath) : NULL;
    ssl_opts.cert_filename = (SvOK(cert)) ? SvPV_nolen(cert) : NULL;
    ssl_opts.private_key_filename = (SvOK(key)) ? SvPV_nolen(key) : NULL;
    ssl_opts.server_name = (SvOK(server_name)) ? SvPV_nolen(server_name) : NULL;
    ssl_opts.verify_mode = verify ? REDIS_SSL_VERIFY_PEER : REDIS_SSL_VERIFY_NONE;

    /* Free existing SSL context if any */
    if (NULL != self->ssl_ctx) {
        redisFreeSSLContext(self->ssl_ctx);
        self->ssl_ctx = NULL;
    }

    self->ssl_ctx = redisCreateSSLContextWithOptions(&ssl_opts, &ssl_error);

    if (NULL == self->ssl_ctx) {
        croak("SSL context creation failed: %s", redisSSLContextGetError(ssl_error));
    }
}

#endif
