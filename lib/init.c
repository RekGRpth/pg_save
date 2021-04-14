#include "lib.h"

PG_MODULE_MAGIC;

int init_attempt;
int init_timeout;
state_t init_state = state_unknown;
static bool init_sighup = false;
static bool standby_signal_file_found = false;
static int init_restart;
#define XX(name) static char *init_##name;
STATE_MAP(XX)
#undef XX

const char *init_state2char(state_t state) {
    switch (state) {
#define XX(name) case state_##name: return #name;
        STATE_MAP(XX)
#undef XX
    }
    E("state = %i", state);
}

state_t init_char2state(const char *state) {
#define XX(name) if (!strcmp(state, #name)) return state_##name;
    STATE_MAP(XX)
#undef XX
    E("state = %s", state);
}

state_t init_host(const char *host) {
#define XX(name) if (init_##name && !strcmp(host, init_##name)) return state_##name;
    STATE_MAP(XX)
#undef XX
    return state_unknown;
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
    D1("HOSTNAME = %s", getenv("HOSTNAME"));
    D1("restart = %i", init_restart);
    D1("state = %s", init_state2char(init_state));
    D1("timeout = %i", init_timeout);
#define XX(name) if (init_##name) D1(#name" = %s", init_##name);
    STATE_MAP(XX)
#undef XX
    D1("standby_signal_file_found = %s", standby_signal_file_found ? "true" : "false");
    if (IsBackgroundWorker) D1("RecoveryInProgress = %s", RecoveryInProgress() ? "true" : "false");
    if (PrimaryConnInfo && PrimaryConnInfo[0] != '\0') D1("PrimaryConnInfo = %s", PrimaryConnInfo);
    if (SyncRepStandbyNames && SyncRepStandbyNames[0] != '\0') D1("SyncRepStandbyNames = %s", SyncRepStandbyNames);
}

void init_reload(void) {
    if (!init_sighup) return;
    if (kill(PostmasterPid, SIGHUP)) W("kill(%i, %i)", PostmasterPid, SIGHUP);
    init_sighup = false;
}

void init_set_host(const char *host, state_t state) {
    StringInfoData buf;
    if (state == state_unknown) return;
    D1("host = %s, state = %s", host, init_state2char(state));
#define XX(name) if (state != state_##name && init_##name && !strcmp(init_##name, host)) init_set_system("pg_save."#name, NULL);
    STATE_MAP(XX)
#undef XX
    initStringInfoMy(TopMemoryContext, &buf);
    appendStringInfo(&buf, "pg_save.%s", init_state2char(state));
    init_set_system(buf.data, host);
    pfree(buf.data);
}

static void init_notify(state_t state) {
    const char *channel = getenv("HOSTNAME");
    const char *payload = init_state2char(state);
    const char *channel_quote = quote_identifier(channel);
    const char *payload_quote = quote_literal_cstr(payload);
    bool idle = !IsTransactionOrTransactionBlock();
    StringInfoData buf;
    PlannedStmt *pstmt = makeNode(PlannedStmt);
    NotifyStmt *n = makeNode(NotifyStmt);
    pstmt->commandType = CMD_UTILITY;
    pstmt->canSetTag = false;
    pstmt->utilityStmt = (Node *)n;
    n->conditionname = (char *)channel;
    n->payload = (char *)payload;
    initStringInfoMy(TopMemoryContext, &buf);
    appendStringInfo(&buf, "NOTIFY %s, %s", channel_quote, payload_quote);
    if (channel_quote != channel) pfree((void *)channel_quote);
    pfree((void *)payload_quote);
    if (idle) StartTransactionCommand();
    ProcessUtility(pstmt, buf.data, PROCESS_UTILITY_TOPLEVEL, NULL, NULL, None_Receiver, NULL);
    if (idle) CommitTransactionCommand();
    MemoryContextSwitchTo(TopMemoryContext);
    pfree(buf.data);
}

void init_set_state(state_t state) {
    D1("state = %s", init_state2char(state));
    init_set_system("pg_save.state", init_state2char(state));
    init_state = state;
    init_set_host(getenv("HOSTNAME"), state);
    init_notify(state);
    switch (state) {
        case state_async:
        case state_potential:
        case state_primary:
        case state_quorum:
        case state_single:
        case state_sync: RequestCheckpoint(CHECKPOINT_IMMEDIATE | CHECKPOINT_WAIT | (RecoveryInProgress() ? 0 : CHECKPOINT_FORCE)); break;
        default: break;
    }
    if (state == state_primary && getenv("SYNCHRONOUS_STANDBY_NAMES")) init_set_system("synchronous_standby_names", getenv("SYNCHRONOUS_STANDBY_NAMES"));
}

void init_set_system(const char *name, const char *new) {
    AlterSystemStmt *stmt;
    const char *old = GetConfigOption(name, false, true);
    bool old_isnull = !old || old[0] == '\0';
    bool new_isnull = !new || new[0] == '\0';
    if (old_isnull && new_isnull) return;
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
    init_sighup = true;
}

static void init_work(void) {
    StringInfoData buf;
    BackgroundWorker worker;
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
    appendStringInfoString(&buf, getenv("HOSTNAME"));
    if (buf.len + 1 > BGW_MAXLEN) E("%i > BGW_MAXLEN", buf.len + 1);
    memcpy(worker.bgw_type, buf.data, buf.len);
    resetStringInfo(&buf);
    appendStringInfo(&buf, "postgres postgres %s", getenv("HOSTNAME"));
    if (buf.len + 1 > BGW_MAXLEN) E("%i > BGW_MAXLEN", buf.len + 1);
    memcpy(worker.bgw_name, buf.data, buf.len);
    pfree(buf.data);
    RegisterBackgroundWorker(&worker);
}

static void readRecoverySignalFile(void) {
    struct stat stat_buf;
    //if (IsBootstrapProcessingMode()) return;
    if (!stat(STANDBY_SIGNAL_FILE, &stat_buf)) {
        int fd = BasicOpenFilePerm(STANDBY_SIGNAL_FILE, O_RDWR | PG_BINARY /*| get_sync_bit(sync_method)*/, S_IRUSR | S_IWUSR);
        if (fd >= 0) {
            (void) pg_fsync(fd);
            close(fd);
        }
        standby_signal_file_found = true;
    }
}

static void init_save(void) {
    static const struct config_enum_entry init_state_options[] = {
#define XX(name) {#name, state_##name, false},
        STATE_MAP(XX)
#undef XX
    };
    DefineCustomEnumVariable("pg_save.state", "pg_save state", NULL, (int *)&init_state, state_unknown, init_state_options, PGC_SIGHUP, 0, NULL, NULL, NULL);
    DefineCustomIntVariable("pg_save.attempt", "pg_save attempt", NULL, &init_attempt, 10, 1, INT_MAX, PGC_SIGHUP, 0, NULL, NULL, NULL);
    DefineCustomIntVariable("pg_save.restart", "pg_save restart", NULL, &init_restart, 10, 1, INT_MAX, PGC_POSTMASTER, 0, NULL, NULL, NULL);
    DefineCustomIntVariable("pg_save.timeout", "pg_save timeout", NULL, &init_timeout, 1000, 1, INT_MAX, PGC_SIGHUP, 0, NULL, NULL, NULL);
#define XX(name) DefineCustomStringVariable("pg_save."#name, "pg_save "#name, NULL, &init_##name, NULL, PGC_SIGHUP, 0, NULL, NULL, NULL);
    STATE_MAP(XX)
#undef XX
    readRecoverySignalFile();
    init_debug();
    init_work();
}

void _PG_init(void) {
    if (IsBinaryUpgrade) { W("IsBinaryUpgrade"); return; }
    if (!process_shared_preload_libraries_in_progress) F("!process_shared_preload_libraries_in_progress");
    init_save();
}