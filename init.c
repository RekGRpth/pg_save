#include "include.h"

PG_MODULE_MAGIC;

char *init_policy;
char *init_primary;
char *init_state;
int init_attempt;
int init_timeout;
static bool reload = false;
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
    reload = true;
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
    D1("name = %s, old = %s, new = %s", name, (old && old[0] != '\0') ? old : "(null)", new);
    stmt = makeNode(AlterSystemStmt);
    stmt->setstmt = makeNode(VariableSetStmt);
    stmt->setstmt->name = (char *)name;
    stmt->setstmt->kind = VAR_SET_VALUE;
    stmt->setstmt->args = list_make1(makeStringConst((char *)new, -1));
    AlterSystemSetConfigFile(stmt);
    list_free_deep(stmt->setstmt->args);
    pfree(stmt->setstmt);
    pfree(stmt);
    reload = true;
}

void init_debug(void) {
    D1("attempt = %i", init_attempt);
    D1("restart = %i", init_restart);
    D1("timeout = %i", init_timeout);
    D1("policy = %s", init_policy);
    if (init_async) D1("async = %s", init_async);
    if (init_potential) D1("potential = %s", init_potential);
    if (init_primary) D1("primary = %s", init_primary);
    if (init_quorum) D1("quorum = %s", init_quorum);
    if (init_state) D1("state = %s", init_state);
    if (init_sync) D1("sync = %s", init_sync);
    if (PrimaryConnInfo && PrimaryConnInfo[0] != '\0') D1("PrimaryConnInfo = %s", PrimaryConnInfo);
    if (PrimarySlotName && PrimarySlotName[0] != '\0') D1("PrimarySlotName = %s", PrimarySlotName);
    if (SyncRepStandbyNames && SyncRepStandbyNames[0] != '\0') D1("SyncRepStandbyNames = %s", SyncRepStandbyNames);
}

void init_connect(void) {
    if (init_primary && (!init_state || strcmp(init_state, "primary"))) backend_connect(init_primary, "primary");
    if (init_sync && (!init_state || strcmp(init_state, "sync"))) backend_connect(init_sync, "sync");
    if (init_quorum && (!init_state || strcmp(init_state, "quorum"))) backend_connect(init_quorum, "quorum");
    if (init_potential && (!init_state || strcmp(init_state, "potential"))) backend_connect(init_potential, "potential");
    if (init_async && (!init_state || strcmp(init_state, "async"))) backend_connect(init_async, "async");
}

void init_reload(void) {
    if (!reload) return;
    ProcessConfigFile(PGC_SIGHUP);
    reload = false;
    init_debug();
}

void init_reset_state(const char *host, const char *state) {
    StringInfoData buf;
    if (ShutdownRequestPending) return;
    if (!state) return;
    D1("host = %s, state = %s", host, state);
    initStringInfo(&buf);
    appendStringInfo(&buf, "pg_save.%s", state);
    init_alter_system_reset(buf.data, host);
    pfree(buf.data);
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
    DefineCustomIntVariable("pg_save.restart", "pg_save restart", NULL, &init_restart, 10, 1, INT_MAX, PGC_POSTMASTER, 0, NULL, NULL, NULL);
    DefineCustomIntVariable("pg_save.timeout", "pg_save timeout", NULL, &init_timeout, 1000, 1, INT_MAX, PGC_SIGHUP, 0, NULL, NULL, NULL);
    DefineCustomStringVariable("pg_save.async", "pg_save async", NULL, &init_async, NULL, PGC_SIGHUP, 0, NULL, NULL, NULL);
    DefineCustomStringVariable("pg_save.policy", "pg_save policy", NULL, &init_policy, "FIRST 1", PGC_POSTMASTER, 0, NULL, NULL, NULL);
    DefineCustomStringVariable("pg_save.potential", "pg_save potential", NULL, &init_potential, NULL, PGC_SIGHUP, 0, NULL, NULL, NULL);
    DefineCustomStringVariable("pg_save.primary", "pg_save primary", NULL, &init_primary, NULL, PGC_SIGHUP, 0, NULL, NULL, NULL);
    DefineCustomStringVariable("pg_save.quorum", "pg_save quorum", NULL, &init_quorum, NULL, PGC_SIGHUP, 0, NULL, NULL, NULL);
    DefineCustomStringVariable("pg_save.state", "pg_save state", NULL, &init_state, NULL, PGC_SIGHUP, 0, NULL, NULL, NULL);
    DefineCustomStringVariable("pg_save.sync", "pg_save sync", NULL, &init_sync, NULL, PGC_SIGHUP, 0, NULL, NULL, NULL);
    init_debug();
    init_work();
}

void init_sighup(void) {
    if (!reload) return;
    if (kill(PostmasterPid, SIGHUP)) W("kill and %m");
}

void _PG_init(void) {
    if (IsBinaryUpgrade) { W("IsBinaryUpgrade"); return; }
    if (!process_shared_preload_libraries_in_progress) F("!process_shared_preload_libraries_in_progress");
    init_save();
}
