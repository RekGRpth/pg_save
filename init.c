#include "include.h"

PG_MODULE_MAGIC;

char *init_policy;
char *init_primary;
char *init_state;
int init_attempt;
int init_timeout;
static char *init_async;
static char *init_potential;
static char *init_quorum;
static char *init_sync;
static int init_restart;

void init_alter_system_reset(const char *name, const char *old) {
    AlterSystemStmt *stmt;
    if (!old || old[0] == '\0') return;
    D1("name = %s, old = %s", name, old);
    stmt = makeNode(AlterSystemStmt);
    stmt->setstmt = makeNode(VariableSetStmt);
    stmt->setstmt->name = (char *)name;
    stmt->setstmt->kind = VAR_RESET;
    AlterSystemSetConfigFile(stmt);
    pfree(stmt->setstmt);
    pfree(stmt);
    init_reload();
}

static Node *makeStringConst(char *str, int location) {
    A_Const *n = makeNode(A_Const);
    n->val.type = T_String;
    n->val.val.str = str;
    n->location = location;
    return (Node *)n;
}

void init_alter_system_set(const char *name, const char *old, const char *new) {
    AlterSystemStmt *stmt;
    if (old && old[0] != '\0' && !strcmp(old, new)) return;
    D1("name = %s, old = %s, new = %s", name, old ? old : "(null)", new);
    stmt = makeNode(AlterSystemStmt);
    stmt->setstmt = makeNode(VariableSetStmt);
    stmt->setstmt->name = (char *)name;
    stmt->setstmt->kind = VAR_SET_VALUE;
    stmt->setstmt->args = list_make1(makeStringConst((char *)new, -1));
    AlterSystemSetConfigFile(stmt);
    list_free_deep(stmt->setstmt->args);
    pfree(stmt->setstmt);
    pfree(stmt);
    init_reload();
}

void init_connect(void) {
    if (init_primary && (!init_state || strcmp(init_state, "primary"))) backend_connect(init_primary, "primary");
    if (init_sync && (!init_state || strcmp(init_state, "sync"))) backend_connect(init_sync, "sync");
    if (init_quorum && (!init_state || strcmp(init_state, "quorum"))) backend_connect(init_quorum, "quorum");
    if (init_potential && (!init_state || strcmp(init_state, "potential"))) backend_connect(init_potential, "potential");
    if (init_async && (!init_state || strcmp(init_state, "async"))) backend_connect(init_async, "async");
}

void init_kill(void) {
#ifdef HAVE_SETSID
    if (kill(-PostmasterPid, SIGTERM))
#else
    if (kill(PostmasterPid, SIGTERM))
#endif
    E("kill");
}

#define DirectFunctionCall0(func) DirectFunctionCall0Coll(func, InvalidOid)
static Datum DirectFunctionCall0Coll(PGFunction func, Oid collation) {
    LOCAL_FCINFO(fcinfo, 0);
    Datum result;
    InitFunctionCallInfoData(*fcinfo, NULL, 0, collation, NULL, NULL);
    result = (*func)(fcinfo);
    if (fcinfo->isnull) E("function %p returned NULL", (void *)func);
    return result;
}

void init_reload(void) {
    if (!DatumGetBool(DirectFunctionCall0(pg_reload_conf))) W("!pg_reload_conf");
}

void init_reset_state(const char *host) {
    D1("host = %s", host);
    if (init_primary && !strcmp(init_primary, host)) init_alter_system_reset("pg_save.primary", init_primary);
    if (init_sync && !strcmp(init_sync, host)) init_alter_system_reset("pg_save.sync", init_sync);
    if (init_quorum && !strcmp(init_quorum, host)) init_alter_system_reset("pg_save.quorum", init_quorum);
    if (init_potential && !strcmp(init_potential, host)) init_alter_system_reset("pg_save.potential", init_potential);
    if (init_async && !strcmp(init_async, host)) init_alter_system_reset("pg_save.async", init_async);
}

void init_set_state(const char *host, const char *state) {
    char *old;
    StringInfoData buf;
    D1("host = %s, state = %s", host, state);
    initStringInfo(&buf);
    appendStringInfo(&buf, "pg_save.%s", state);
    old = GetConfigOptionByName(buf.data, NULL, false);
    init_alter_system_set(buf.data, old, host);
    if (old) pfree(old);
    pfree(buf.data);
}

static void init_work(void) {
    StringInfoData buf;
    BackgroundWorker worker;
    MemSet(&worker, 0, sizeof(worker));
    worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
    worker.bgw_restart_time = init_restart;
    worker.bgw_start_time = BgWorkerStart_ConsistentState;
    initStringInfo(&buf);
    appendStringInfoString(&buf, "pg_save");
    if (buf.len + 1 > BGW_MAXLEN) E("%i > BGW_MAXLEN", buf.len + 1);
    memcpy(worker.bgw_library_name, buf.data, buf.len);
    resetStringInfo(&buf);
    appendStringInfoString(&buf, "save_worker");
    if (buf.len + 1 > BGW_MAXLEN) E("%i > BGW_MAXLEN", buf.len + 1);
    memcpy(worker.bgw_function_name, buf.data, buf.len);
    resetStringInfo(&buf);
    appendStringInfoString(&buf, "pg_save");
    if (buf.len + 1 > BGW_MAXLEN) E("%i > BGW_MAXLEN", buf.len + 1);
    memcpy(worker.bgw_type, buf.data, buf.len);
    resetStringInfo(&buf);
    appendStringInfoString(&buf, "postgres postgres pg_save");
    if (buf.len + 1 > BGW_MAXLEN) E("%i > BGW_MAXLEN", buf.len + 1);
    memcpy(worker.bgw_name, buf.data, buf.len);
    pfree(buf.data);
    RegisterBackgroundWorker(&worker);
}

static void init_save(void) {
    DefineCustomIntVariable("pg_save.attempt", "pg_save attempt", NULL, &init_attempt, 30, 1, INT_MAX, PGC_SIGHUP, 0, NULL, NULL, NULL);
    D1("attempt = %i", init_attempt);
    DefineCustomIntVariable("pg_save.restart", "pg_save restart", NULL, &init_restart, 10, 1, INT_MAX, PGC_POSTMASTER, 0, NULL, NULL, NULL);
    D1("restart = %i", init_restart);
    DefineCustomIntVariable("pg_save.timeout", "pg_save timeout", NULL, &init_timeout, 1000, 1, INT_MAX, PGC_SIGHUP, 0, NULL, NULL, NULL);
    D1("timeout = %i", init_timeout);
    DefineCustomStringVariable("pg_save.async", "pg_save async", NULL, &init_async, NULL, PGC_SIGHUP, 0, NULL, NULL, NULL);
    if (init_async) D1("async = %s", init_async);
    DefineCustomStringVariable("pg_save.policy", "pg_save policy", NULL, &init_policy, "FIRST 1", PGC_POSTMASTER, 0, NULL, NULL, NULL);
    D1("policy = %s", init_policy);
    DefineCustomStringVariable("pg_save.potential", "pg_save potential", NULL, &init_potential, NULL, PGC_SIGHUP, 0, NULL, NULL, NULL);
    if (init_potential) D1("potential = %s", init_potential);
    DefineCustomStringVariable("pg_save.primary", "pg_save primary", NULL, &init_primary, NULL, PGC_SIGHUP, 0, NULL, NULL, NULL);
    if (init_primary) D1("primary = %s", init_primary);
    DefineCustomStringVariable("pg_save.quorum", "pg_save quorum", NULL, &init_quorum, NULL, PGC_SIGHUP, 0, NULL, NULL, NULL);
    if (init_quorum) D1("quorum = %s", init_quorum);
    DefineCustomStringVariable("pg_save.state", "pg_save state", NULL, &init_state, NULL, PGC_SIGHUP, 0, NULL, NULL, NULL);
    if (init_state) D1("state = %s", init_state);
    DefineCustomStringVariable("pg_save.sync", "pg_save sync", NULL, &init_sync, NULL, PGC_SIGHUP, 0, NULL, NULL, NULL);
    if (init_sync) D1("sync = %s", init_sync);
    init_work();
}

void _PG_init(void) {
    if (IsBinaryUpgrade) { W("IsBinaryUpgrade"); return; }
    if (!process_shared_preload_libraries_in_progress) F("!process_shared_preload_libraries_in_progress");
    init_save();
}
