#ifndef _INCLUDE_H_
#define _INCLUDE_H_

#define FORMAT_0(fmt, ...) "%s(%s:%d): %s", __func__, __FILE__, __LINE__, fmt
#define FORMAT_1(fmt, ...) "%s(%s:%d): " fmt,  __func__, __FILE__, __LINE__
#define GET_FORMAT(fmt, ...) GET_FORMAT_PRIVATE(fmt, 0, ##__VA_ARGS__, 1, \
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, \
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, \
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, \
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, \
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, \
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, \
    1, 1, 1, 1, 1, 1, 1, 1, 1, 0)
#define GET_FORMAT_PRIVATE(fmt, \
      _0,  _1,  _2,  _3,  _4,  _5,  _6,  _7,  _8,  _9, \
     _10, _11, _12, _13, _14, _15, _16, _17, _18, _19, \
     _20, _21, _22, _23, _24, _25, _26, _27, _28, _29, \
     _30, _31, _32, _33, _34, _35, _36, _37, _38, _39, \
     _40, _41, _42, _43, _44, _45, _46, _47, _48, _49, \
     _50, _51, _52, _53, _54, _55, _56, _57, _58, _59, \
     _60, _61, _62, _63, _64, _65, _66, _67, _68, _69, \
     _70, format, ...) FORMAT_ ## format(fmt)

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

#define countof(array) (sizeof(array)/sizeof(array[0]))

#include <postgres.h>

#include <access/xact.h>
#include <commands/async.h>
#include <executor/spi.h>
#include <funcapi.h>
#include <libpq-fe.h>
#include <libpq/libpq-be.h>
#include <pgstat.h>
#include <postmaster/bgworker.h>
#include <postmaster/bgwriter.h>
#include <postmaster/interrupt.h>
#include <replication/walsender_private.h>
#include <storage/ipc.h>
#include <sys/utsname.h>
#include <tcop/utility.h>
#include <utils/builtins.h>
#include <utils/timeout.h>

#include "queue.h"

#define STATE_MAP(XX) \
    XX(unknown) \
    XX(initial) \
    XX(single) \
    XX(wait_primary) \
    XX(primary) \
    XX(wait_standby) \
    XX(sync) \
    XX(potential) \
    XX(quorum) \
    XX(async)

typedef enum state_t {
#define XX(name) state_##name,
    STATE_MAP(XX)
#undef XX
} state_t;

typedef struct Backend {
    int attempt;
    int events;
    PGconn *conn;
    queue_t queue;
    state_t state;
    void (*socket) (struct Backend *backend);
} Backend;

typedef struct _SPI_plan SPI_plan;

Backend *backend_host(const char *host);
Backend *backend_state(state_t state);
char *TextDatumGetCStringMy(MemoryContext memoryContext, Datum datum);
const char *init_state2char(state_t state);
Datum SPI_getbinval_my(HeapTuple tuple, TupleDesc tupdesc, const char *fname, bool allow_null);
int backend_nevents(void);
size_t backend_size(void);
SPI_plan *SPI_prepare_my(const char *src, int nargs, Oid *argtypes);
state_t init_char2state(const char *state);
state_t init_host(const char *host);
void backend_create(const char *host, state_t state);
void backend_event(WaitEventSet *set);
void backend_finish(Backend *backend);
void backend_fini(void);
void backend_idle(Backend *backend);
void backend_init(void);
void backend_names(char **names);
void backend_reset(Backend *backend);
void backend_result(const char *host, state_t state);
void backend_socket(Backend *backend);
void backend_timeout(void);
void backend_update(Backend *backend, state_t state);
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
void SPI_execute_plan_my(SPI_plan *plan, Datum *values, const char *nulls, int res, bool commit);
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

#endif // _INCLUDE_H_
