/* modified version of hiredis's libev adapter */
#ifndef __HIREDIS_LIBEV_H__
#define __HIREDIS_LIBEV_H__
#include "EXTERN.h"
#include "perl.h"
#include "XSUB.h"
#include "ppport.h"

#include <stdlib.h>
#include <sys/types.h>

#include "EVAPI.h"

#include "hiredis.h"
#include "async.h"

typedef struct redisLibevEvents {
    redisAsyncContext *context;
    struct ev_loop *loop;
    int reading, writing, timing;
    ev_io rev, wev;
    ev_timer timer;
    int priority;
} redisLibevEvents;

static void redisLibevReadEvent(EV_P_ ev_io *watcher, int revents) {
#if EV_MULTIPLICITY
    ((void)loop);
#endif
    ((void)revents);

    redisLibevEvents *e = (redisLibevEvents*)watcher->data;
    redisAsyncHandleRead(e->context);
}

static void redisLibevWriteEvent(EV_P_ ev_io *watcher, int revents) {
#if EV_MULTIPLICITY
    ((void)loop);
#endif
    ((void)revents);

    redisLibevEvents *e = (redisLibevEvents*)watcher->data;
    redisAsyncHandleWrite(e->context);
}

static void redisLibevAddRead(void *privdata) {
    redisLibevEvents *e = (redisLibevEvents*)privdata;
    struct ev_loop *loop = e->loop;
    ((void)loop);
    if (!e->reading) {
        e->reading = 1;
        ev_io_start(loop, &e->rev);
    }
}

static void redisLibevDelRead(void *privdata) {
    redisLibevEvents *e = (redisLibevEvents*)privdata;
    struct ev_loop *loop = e->loop;
    ((void)loop);
    if (e->reading) {
        e->reading = 0;
        ev_io_stop(loop, &e->rev);
    }
}

static void redisLibevAddWrite(void *privdata) {
    redisLibevEvents *e = (redisLibevEvents*)privdata;
    struct ev_loop *loop = e->loop;
    ((void)loop);
    if (!e->writing) {
        e->writing = 1;
        ev_io_start(loop, &e->wev);
    }
}

static void redisLibevDelWrite(void *privdata) {
    redisLibevEvents *e = (redisLibevEvents*)privdata;
    struct ev_loop *loop = e->loop;
    ((void)loop);
    if (e->writing) {
        e->writing = 0;
        ev_io_stop(loop, &e->wev);
    }
}

static void redisLibevStopTimer(void *privdata) {
    redisLibevEvents *e = (redisLibevEvents*)privdata;
    struct ev_loop *loop = e->loop;
    ((void)loop);
    if (e->timing) {
        e->timing = 0;
        ev_timer_stop(loop, &e->timer);
    }
}


static void redisLibevCleanup(void *privdata) {
    redisLibevEvents *e = (redisLibevEvents*)privdata;
    redisLibevDelRead(privdata);
    redisLibevDelWrite(privdata);
    redisLibevStopTimer(privdata);
    free(e);
}

static void redisLibevTimeout(EV_P_ ev_timer *timer, int revents) {
#if EV_MULTIPLICITY
    ((void)loop);
#endif
    ((void)revents);

    redisLibevEvents *e = (redisLibevEvents*)timer->data;
    redisAsyncHandleTimeout(e->context);
}

static void redisLibevSetTimeout(void *privdata, struct timeval tv) {
    redisLibevEvents *e = (redisLibevEvents*)privdata;
    struct ev_loop *loop = e->loop;
    ((void)loop);

    if (!e->timing) {
        e->timing = 1;
        ev_init(&e->timer, redisLibevTimeout);
        e->timer.data = (SV*)e;
    }

    e->timer.repeat = tv.tv_sec + tv.tv_usec / 1000000.00;
    ev_timer_again(loop, &e->timer);
}


static int redisLibevAttach(EV_P_ redisAsyncContext *ac) {
    redisContext *c = &(ac->c);
    redisLibevEvents *e;

    /* Nothing should be attached when something is already attached */
    if (ac->ev.data != NULL)
        return REDIS_ERR;

    /* Create container for context and r/w events */
    e = (redisLibevEvents*)malloc(sizeof(*e));
    if (e == NULL)
        return REDIS_ERR;
    e->context = ac;
#if EV_MULTIPLICITY
    e->loop = loop;
#else
    e->loop = NULL;
#endif
    e->reading = e->writing = e->timing = 0;
    e->priority = 0;
    e->rev.data = (SV*)e;
    e->wev.data = (SV*)e;

    /* Register functions to start/stop listening for events */
    ac->ev.addRead = redisLibevAddRead;
    ac->ev.delRead = redisLibevDelRead;
    ac->ev.addWrite = redisLibevAddWrite;
    ac->ev.delWrite = redisLibevDelWrite;
    ac->ev.cleanup = redisLibevCleanup;
    ac->ev.scheduleTimer = redisLibevSetTimeout;
    ac->ev.data = e;

    /* Initialize read/write events */
    ev_io_init(&e->rev,redisLibevReadEvent,c->fd,EV_READ);
    ev_io_init(&e->wev,redisLibevWriteEvent,c->fd,EV_WRITE);
    return REDIS_OK;
}

static void redisLibevSetPriority(redisAsyncContext *ac, int priority) {
    redisLibevEvents *e = (redisLibevEvents*)ac->ev.data;
    struct ev_loop *loop;
    if (e == NULL) return;

    loop = e->loop;
    ((void)loop);
    e->priority = priority;

    /* Stop watchers, set priority, restart if they were running */
    if (e->reading) {
        ev_io_stop(loop, &e->rev);
        ev_set_priority(&e->rev, priority);
        ev_io_start(loop, &e->rev);
    } else {
        ev_set_priority(&e->rev, priority);
    }

    if (e->writing) {
        ev_io_stop(loop, &e->wev);
        ev_set_priority(&e->wev, priority);
        ev_io_start(loop, &e->wev);
    } else {
        ev_set_priority(&e->wev, priority);
    }

    if (e->timing) {
        ev_timer_stop(loop, &e->timer);
        ev_set_priority(&e->timer, priority);
        ev_timer_start(loop, &e->timer);
    } else {
        ev_set_priority(&e->timer, priority);
    }
}

static int redisLibevGetPriority(redisAsyncContext *ac) {
    redisLibevEvents *e = (redisLibevEvents*)ac->ev.data;
    if (e == NULL) return 0;
    return e->priority;
}

#endif
