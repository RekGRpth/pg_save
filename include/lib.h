#ifndef _LIB_H_
#define _LIB_H_

#include "common.h"

#define D1(fmt, ...) ereport(DEBUG1, (errmsg(GET_FORMAT(fmt, ##__VA_ARGS__), ##__VA_ARGS__)))
#define D2(fmt, ...) ereport(DEBUG2, (errmsg(GET_FORMAT(fmt, ##__VA_ARGS__), ##__VA_ARGS__)))
#define D3(fmt, ...) ereport(DEBUG3, (errmsg(GET_FORMAT(fmt, ##__VA_ARGS__), ##__VA_ARGS__)))
#define D4(fmt, ...) ereport(DEBUG4, (errmsg(GET_FORMAT(fmt, ##__VA_ARGS__), ##__VA_ARGS__)))
#define D5(fmt, ...) ereport(DEBUG5, (errmsg(GET_FORMAT(fmt, ##__VA_ARGS__), ##__VA_ARGS__)))
#define E(fmt, ...) ereport(ERROR, (errmsg(GET_FORMAT(fmt, ##__VA_ARGS__), ##__VA_ARGS__)))
#define F(fmt, ...) ereport(FATAL, (errmsg(GET_FORMAT(fmt, ##__VA_ARGS__), ##__VA_ARGS__)))
#define I(fmt, ...) ereport(INFO, (errmsg(GET_FORMAT(fmt, ##__VA_ARGS__), ##__VA_ARGS__)))
#define L(fmt, ...) ereport(LOG, (errmsg(GET_FORMAT(fmt, ##__VA_ARGS__), ##__VA_ARGS__)))
#define N(fmt, ...) ereport(NOTICE, (errmsg(GET_FORMAT(fmt, ##__VA_ARGS__), ##__VA_ARGS__)))
#define W(fmt, ...) ereport(WARNING, (errmsg(GET_FORMAT(fmt, ##__VA_ARGS__), ##__VA_ARGS__)))

#include <postgres.h>

#include <access/xact.h>
#include <commands/async.h>
#include <executor/spi.h>
#include <funcapi.h>
#include <libpq/libpq-be.h>
#include <pgstat.h>
#include <postmaster/bgworker.h>
#include <postmaster/bgwriter.h>
#if PG_VERSION_NUM >= 130000
#include <postmaster/interrupt.h>
#else
extern PGDLLIMPORT volatile sig_atomic_t ShutdownRequestPending;
extern void SignalHandlerForConfigReload(SIGNAL_ARGS);
extern void SignalHandlerForShutdownRequest(SIGNAL_ARGS);
#endif
#include <replication/walsender_private.h>
#include <miscadmin.h>
#if PG_VERSION_NUM >= 140000
#include <storage/proc.h>
#endif
#include <sys/stat.h>
#include <tcop/utility.h>
#include <unistd.h>
#include <utils/builtins.h>
#include <utils/memutils.h>
#include <utils/snapmgr.h>
#include <utils/timeout.h>

#if PG_VERSION_NUM >= 100000
#else
#define WL_SOCKET_MASK (WL_SOCKET_READABLE | WL_SOCKET_WRITEABLE)
#endif

typedef enum state_t {
#define XX(name) state_##name,
    STATE_MAP(XX)
#undef XX
} state_t;

typedef struct Backend {
    char *host;
    dlist_node node;
    int attempt;
    int event;
    PGconn *conn;
    state_t state;
    void (*socket) (struct Backend *backend);
} Backend;

Backend *backend_host(const char *host);
Backend *backend_state(state_t state);
bool backend_busy(Backend *backend, int event);
bool backend_consume(Backend *backend);
bool backend_consume_flush_busy(Backend *backend);
bool backend_flush(Backend *backend);
char *TextDatumGetCStringMy(MemoryContext memoryContext, Datum datum);
const char *init_state2char(state_t state);
Datum SPI_getbinval_my(HeapTupleData *tuple, TupleDesc tupdesc, const char *fname, bool allow_null);
int backend_nevents(void);
SPIPlanPtr SPI_prepare_my(const char *src, int nargs, Oid *argtypes);
state_t init_char2state(const char *state);
state_t init_host(const char *host);
void backend_create(const char *host, state_t state);
void backend_event(WaitEventSet *set);
void backend_finish(Backend *backend);
void backend_fini(void);
void backend_idle(Backend *backend);
void backend_init(void);
void backend_readable(Backend *backend);
void backend_result(const char *host, state_t state);
void backend_timeout(void);
void backend_update(Backend *backend, state_t state);
void backend_writeable(Backend *backend);
void init_backend(void);
void init_debug(void);
void init_reload(void);
void init_set_host(const char *host, state_t state);
void init_set_state(state_t state);
void init_set_system(const char *name, const char *new);
void initStringInfoMy(MemoryContext memoryContext, StringInfoData *buf);
void _PG_init(void);
void primary_connected(Backend *backend);
void primary_created(Backend *backend);
void primary_failed(Backend *backend);
void primary_finished(Backend *backend);
void primary_fini(void);
void primary_init(void);
void primary_notify(Backend *backend, state_t state);
void primary_timeout(void);
void primary_updated(Backend *backend);
void save_worker(Datum main_arg);
void SPI_commit_my(void);
void SPI_connect_my(const char *src);
void SPI_execute_plan_my(SPIPlanPtr plan, Datum *values, const char *nulls, int res, bool commit);
void SPI_execute_with_args_my(const char *src, int nargs, Oid *argtypes, Datum *values, const char *nulls, int res, bool commit);
void SPI_finish_my(void);
void SPI_start_transaction_my(const char *src);
void standby_connected(Backend *backend);
void standby_created(Backend *backend);
void standby_failed(Backend *backend);
void standby_finished(Backend *backend);
void standby_fini(void);
void standby_init(void);
void standby_notify(Backend *backend, state_t state);
void standby_timeout(void);
void standby_updated(Backend *backend);
void standby_update(state_t state);

#endif // _LIB_H_
