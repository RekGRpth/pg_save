#ifndef _INCLUDE_H_
#define _INCLUDE_H_

#include <postgres.h>

//#include <access/printtup.h>
#include <access/xact.h>
//#include <catalog/heap.h>
//#include <catalog/namespace.h>
//#include <catalog/pg_type.h>
#include <commands/async.h>
//#include <commands/dbcommands.h>
#include <commands/extension.h>
#include <commands/prepare.h>
//#include <commands/user.h>
#include <common/ip.h>
#include <executor/spi.h>
#include <fe_utils/recovery_gen.h>
//#include <fe_utils/string_utils.h>
//#include <jit/jit.h>
#include <libpq-fe.h>
#include <libpq/libpq-be.h>
//#include <miscadmin.h>
//#include <nodes/makefuncs.h>
//#include <parser/analyze.h>
#include <parser/parse_func.h>
//#include <parser/parse_type.h>
#include <pgstat.h>
//#include <postgresql/internal/pqexpbuffer.h>
#include <postmaster/bgworker.h>
#include <replication/slot.h>
#include <replication/syncrep.h>
//#include <replication/syncrep.h>
#include <replication/walreceiver.h>
#include <replication/walsender_private.h>
//#include <tcop/pquery.h>
#include <tcop/utility.h>
//#include <utils/acl.h>
#include <utils/builtins.h>
//#include <utils/lsyscache.h>
//#include <utils/ps_status.h>
#include <utils/regproc.h>
//#include <utils/snapmgr.h>
#include <utils/timeout.h>

#include "queue.h"

typedef struct Backend {
    char *name;
    char *state;
    int events;
    int probe;
    PGconn *conn;
    queue_t queue;
    void (*connect) (struct Backend *backend);
    void (*finish) (struct Backend *backend);
    void (*reset) (struct Backend *backend);
    void (*socket) (struct Backend *backend);
} Backend;

typedef struct _SPI_plan SPI_plan;

bool save_etcd_kv_put(const char *key, const char *value, int ttl);
char *save_etcd_kv_range(const char *key);
char *TextDatumGetCStringMy(Datum datum);
const char *backend_state(Backend *backend);
Datum SPI_getbinval_my(HeapTuple tuple, TupleDesc tupdesc, const char *fname, bool allow_null);
SPI_plan *SPI_prepare_my(const char *src, int nargs, Oid *argtypes);
void appendConnStrVal(PQExpBuffer buf, const char *str);
void backend_alter_system_reset(const char *name);
void backend_alter_system_set(const char *name, const char *old, const char *new);
void backend_connect(Backend *backend, const char *host, const char *port, const char *user, const char *dbname);
void backend_finish(Backend *backend);
void backend_fini(void);
void backend_idle(Backend *backend);
void backend_reset(Backend *backend);
void backend_reset_state(Backend *backend);
void backend_set_state(const char *state, const char *host);
void init_kill(void);
void primary_fini(void);
void primary_init(void);
void primary_timeout(void);
void SPI_commit_my(void);
void SPI_connect_my(const char *src);
void SPI_execute_plan_my(SPI_plan *plan, Datum *values, const char *nulls, int res, bool commit);
void SPI_execute_with_args_my(const char *src, int nargs, Oid *argtypes, Datum *values, const char *nulls, int res, bool commit);
void SPI_finish_my(void);
void SPI_start_transaction_my(const char *src);
void standby_fini(void);
void standby_init(void);
void standby_timeout(void);

#define Q(name) #name
#define S(macro) Q(macro)

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

#define pg_log_error(...) E(__VA_ARGS__)

#define countof(array) (sizeof(array)/sizeof(array[0]))

#endif // _INCLUDE_H_
