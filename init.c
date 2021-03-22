#include "include.h"

PG_MODULE_MAGIC;

char *init_policy;
char *init_sync;
int init_attempt;
int init_timeout;
STATE init_state = UNKNOWN;
static bool reload = false;
static char *init_async;
static char *init_potential;
static char *init_primary;
static char *init_quorum;
static int init_restart;

const char *init_state2char(STATE state) {
    switch (state) {
        case PRIMARY: return "primary";
        case SYNC: return "sync";
        case POTENTIAL: return "potential";
        case QUORUM: return "quorum";
        case ASYNC: return "async";
        default: return "unknown";
    }
}

STATE init_char2state(const char *state) {
    if (!state) return UNKNOWN;
    if (!strcmp(state, "primary")) return PRIMARY;
    if (!strcmp(state, "sync")) return SYNC;
    if (!strcmp(state, "potential")) return POTENTIAL;
    if (!strcmp(state, "quorum")) return QUORUM;
    if (!strcmp(state, "async")) return ASYNC;
    return UNKNOWN;
}

static Node *makeStringConst(char *str, int location) {
    A_Const *n = makeNode(A_Const);
    n->val.type = T_String;
    n->val.val.str = str;
    n->location = location;
    return (Node *)n;
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
    if (init_state != UNKNOWN) D1("state = %s", init_state2char(init_state));
    if (init_sync) D1("sync = %s", init_sync);
    if (PrimaryConnInfo && PrimaryConnInfo[0] != '\0') D1("PrimaryConnInfo = %s", PrimaryConnInfo);
    if (SyncRepStandbyNames && SyncRepStandbyNames[0] != '\0') D1("SyncRepStandbyNames = %s", SyncRepStandbyNames);
}

void init_reload(void) {
    if (!reload) return;
    if (kill(PostmasterPid, SIGHUP)) W("kill and %m");
    reload = false;
}

void init_set_host(const char *host, STATE state) {
    StringInfoData buf;
    if (state == UNKNOWN) return;
    D1("host = %s, state = %s", host, init_state2char(state));
    initStringInfoMy(TopMemoryContext, &buf);
    appendStringInfo(&buf, "pg_save.%s", init_state2char(state));
    init_set_system(buf.data, host);
    pfree(buf.data);
}

void init_set_state(STATE state) {
    D1("state = %s", init_state2char(state));
    init_set_system("pg_save.state", state != UNKNOWN ? init_state2char(state) : NULL);
    init_state = state;
    init_set_host(MyBgworkerEntry->bgw_type, state);
}

void init_set_system(const char *name, const char *new) {
    AlterSystemStmt *stmt;
    const char *old = GetConfigOption(name, false, true);
    bool old_isnull = !old || old[0] == '\0';
    bool new_isnull = !new || new[0] == '\0';
    if (!old_isnull && !new_isnull && !strcmp(old, new)) return;
    D1("name = %s, old = %s, new = %s", name, !old_isnull ? old : "(null)", !new_isnull ? new : "(null)");
    stmt = makeNode(AlterSystemStmt);
    stmt->setstmt = makeNode(VariableSetStmt);
    stmt->setstmt->name = (char *)name;
    stmt->setstmt->kind = !new_isnull ? VAR_SET_VALUE : VAR_RESET;
    if (!new_isnull) stmt->setstmt->args = list_make1(makeStringConst((char *)new, -1));
    AlterSystemSetConfigFile(stmt);
    if (!new_isnull) list_free_deep(stmt->setstmt->args);
    pfree(stmt->setstmt);
    pfree(stmt);
    reload = true;
}

static void init_work(void) {
    struct utsname uts;
    StringInfoData buf;
    BackgroundWorker worker;
    if (uname(&uts)) E("uname");
    MemSet(&worker, 0, sizeof(worker));
    worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
    worker.bgw_restart_time = init_restart;
    worker.bgw_start_time = BgWorkerStart_ConsistentState;
    initStringInfoMy(TopMemoryContext, &buf);
    appendStringInfoString(&buf, "pg_save");
    if (buf.len + 1 > BGW_MAXLEN) E("%i > BGW_MAXLEN", buf.len + 1);
    memcpy(worker.bgw_library_name, buf.data, buf.len);
    resetStringInfo(&buf);
    appendStringInfoString(&buf, "save_worker");
    if (buf.len + 1 > BGW_MAXLEN) E("%i > BGW_MAXLEN", buf.len + 1);
    memcpy(worker.bgw_function_name, buf.data, buf.len);
    resetStringInfo(&buf);
    appendStringInfoString(&buf, uts.nodename);
    if (buf.len + 1 > BGW_MAXLEN) E("%i > BGW_MAXLEN", buf.len + 1);
    memcpy(worker.bgw_type, buf.data, buf.len);
    resetStringInfo(&buf);
    appendStringInfo(&buf, "postgres postgres %s", uts.nodename);
    if (buf.len + 1 > BGW_MAXLEN) E("%i > BGW_MAXLEN", buf.len + 1);
    memcpy(worker.bgw_name, buf.data, buf.len);
    pfree(buf.data);
    RegisterBackgroundWorker(&worker);
}

static void init_save(void) {
    static const struct config_enum_entry init_state_options[] = {
        {"async", ASYNC, false},
        {"potential", POTENTIAL, false},
        {"primary", PRIMARY, false},
        {"quorum", QUORUM, false},
        {"sync", SYNC, false},
        {"unknown", UNKNOWN, false},
        {NULL, 0, false}
    };
    DefineCustomEnumVariable("pg_save.state", "pg_save state", NULL, (int *)&init_state, UNKNOWN, init_state_options, PGC_SIGHUP, 0, NULL, NULL, NULL);
    DefineCustomIntVariable("pg_save.attempt", "pg_save attempt", NULL, &init_attempt, 30, 1, INT_MAX, PGC_SIGHUP, 0, NULL, NULL, NULL);
    DefineCustomIntVariable("pg_save.restart", "pg_save restart", NULL, &init_restart, 10, 1, INT_MAX, PGC_POSTMASTER, 0, NULL, NULL, NULL);
    DefineCustomIntVariable("pg_save.timeout", "pg_save timeout", NULL, &init_timeout, 1000, 1, INT_MAX, PGC_SIGHUP, 0, NULL, NULL, NULL);
    DefineCustomStringVariable("pg_save.async", "pg_save async", NULL, &init_async, NULL, PGC_SIGHUP, 0, NULL, NULL, NULL);
    DefineCustomStringVariable("pg_save.policy", "pg_save policy", NULL, &init_policy, "FIRST 1", PGC_POSTMASTER, 0, NULL, NULL, NULL);
    DefineCustomStringVariable("pg_save.potential", "pg_save potential", NULL, &init_potential, NULL, PGC_SIGHUP, 0, NULL, NULL, NULL);
    DefineCustomStringVariable("pg_save.primary", "pg_save primary", NULL, &init_primary, NULL, PGC_SIGHUP, 0, NULL, NULL, NULL);
    DefineCustomStringVariable("pg_save.quorum", "pg_save quorum", NULL, &init_quorum, NULL, PGC_SIGHUP, 0, NULL, NULL, NULL);
    DefineCustomStringVariable("pg_save.sync", "pg_save sync", NULL, &init_sync, NULL, PGC_SIGHUP, 0, NULL, NULL, NULL);
    init_debug();
    init_work();
}

void _PG_init(void) {
    if (IsBinaryUpgrade) { W("IsBinaryUpgrade"); return; }
    if (!process_shared_preload_libraries_in_progress) F("!process_shared_preload_libraries_in_progress");
    init_save();
}
