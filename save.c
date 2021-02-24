#include "include.h"

extern char *data;
extern char *schema;
extern char *table;
extern char *user;
extern int timeout;
static char *schema_table;
volatile sig_atomic_t sighup = false;
volatile sig_atomic_t sigterm = false;

static void init_sighup(SIGNAL_ARGS) {
    int save_errno = errno;
    sighup = true;
    SetLatch(MyLatch);
    errno = save_errno;
}

static void init_sigterm(SIGNAL_ARGS) {
    int save_errno = errno;
    sigterm = true;
    SetLatch(MyLatch);
    errno = save_errno;
}

static void save_user(void) {
    StringInfoData buf;
    const char *user_quote = quote_identifier(user);
    List *names;
    D1("user = %s", user);
    initStringInfo(&buf);
    appendStringInfo(&buf, "CREATE ROLE %s WITH LOGIN", user_quote);
    names = stringToQualifiedNameList(user_quote);
    SPI_start_transaction_my(buf.data);
    if (!OidIsValid(get_role_oid(strVal(linitial(names)), true))) {
        CreateRoleStmt *stmt = makeNode(CreateRoleStmt);
        ParseState *pstate = make_parsestate(NULL);
        stmt->role = (char *)user;
        stmt->options = lappend(stmt->options, makeDefElem("canlogin", (Node *)makeInteger(1), -1));
        pstate->p_sourcetext = buf.data;
        CreateRole(pstate, stmt);
        list_free_deep(stmt->options);
        free_parsestate(pstate);
        pfree(stmt);
    }
    SPI_commit_my();
    list_free_deep(names);
    if (user_quote != user) pfree((void *)user_quote);
    pfree(buf.data);
}

static void save_data(void) {
    StringInfoData buf;
    const char *user_quote = quote_identifier(user);
    const char *data_quote = quote_identifier(data);
    List *names;
    D1("user = %s, data = %s", user, data);
    initStringInfo(&buf);
    appendStringInfo(&buf, "CREATE DATABASE %s WITH OWNER = %s", data_quote, user_quote);
    names = stringToQualifiedNameList(data_quote);
    SPI_start_transaction_my(buf.data);
    if (!OidIsValid(get_database_oid(strVal(linitial(names)), true))) {
        CreatedbStmt *stmt = makeNode(CreatedbStmt);
        ParseState *pstate = make_parsestate(NULL);
        stmt->dbname = (char *)data;
        stmt->options = lappend(stmt->options, makeDefElem("owner", (Node *)makeString((char *)user), -1));
        pstate->p_sourcetext = buf.data;
        createdb(pstate, stmt);
        list_free_deep(stmt->options);
        free_parsestate(pstate);
        pfree(stmt);
    }
    SPI_commit_my();
    list_free_deep(names);
    if (user_quote != user) pfree((void *)user_quote);
    if (data_quote != data) pfree((void *)data_quote);
    pfree(buf.data);
}

static void save_timeout(void) {
}

static void save_socket(void *data) {
}

static void save_schema(void) {
    StringInfoData buf;
    List *names;
    const char *schema_quote = quote_identifier(schema);
    D1("user = %s, data = %s, schema = %s, table = %s", user, data, schema, table);
    initStringInfo(&buf);
    appendStringInfo(&buf, "CREATE SCHEMA %s", schema_quote);
    names = stringToQualifiedNameList(schema_quote);
    SPI_connect_my(buf.data);
    if (!OidIsValid(get_namespace_oid(strVal(linitial(names)), true))) SPI_execute_with_args_my(buf.data, 0, NULL, NULL, NULL, SPI_OK_UTILITY, false);
    SPI_commit_my();
    SPI_finish_my();
    list_free_deep(names);
    if (schema_quote != schema) pfree((void *)schema_quote);
    pfree(buf.data);
}

static void save_type(void) {
    StringInfoData buf, name;
    Oid type = InvalidOid;
    int32 typmod;
    const char *schema_quote = schema ? quote_identifier(schema) : NULL;
    D1("user = %s, data = %s, schema = %s, table = %s", user, data, schema ? schema : "(null)", table);
    initStringInfo(&name);
    if (schema_quote) appendStringInfo(&name, "%s.", schema_quote);
    appendStringInfoString(&name, "state");
    initStringInfo(&buf);
    appendStringInfo(&buf, "CREATE TYPE %s AS ENUM ('MAIN', 'SYNC', 'ASYNC', 'FAIL')", name.data);
    SPI_connect_my(buf.data);
    parseTypeString(name.data, &type, &typmod, true);
    if (!OidIsValid(type)) SPI_execute_with_args_my(buf.data, 0, NULL, NULL, NULL, SPI_OK_UTILITY, false);
    SPI_commit_my();
    SPI_finish_my();
    if (schema && schema_quote && schema != schema_quote) pfree((void *)schema_quote);
    pfree(name.data);
    pfree(buf.data);
}

static void save_table(void) {
    StringInfoData buf;
    List *names;
    const RangeVar *relation;
    D1("user = %s, data = %s, schema = %s, table = %s, schema_table = %s", user, data, schema ? schema : "(null)", table, schema_table);
    initStringInfo(&buf);
    appendStringInfo(&buf,
        "CREATE TABLE %1$s (\n"
        "    id bigserial NOT NULL PRIMARY KEY,\n"
        "    dt timestamptz NOT NULL DEFAULT current_timestamp,\n"
        "    parent text,\n"
        "    child text,\n"
        "    state state NOT NULL\n"
        ")", schema_table);
    names = stringToQualifiedNameList(schema_table);
    relation = makeRangeVarFromNameList(names);
    SPI_connect_my(buf.data);
    if (!OidIsValid(RangeVarGetRelid(relation, NoLock, true))) SPI_execute_with_args_my(buf.data, 0, NULL, NULL, NULL, SPI_OK_UTILITY, false);
    SPI_commit_my();
    SPI_finish_my();
    pfree((void *)relation);
    list_free_deep(names);
    pfree(buf.data);
}

static void save_index(const char *index) {
    StringInfoData buf, name;
    List *names;
    const RangeVar *relation;
    const char *name_quote;
    const char *index_quote = quote_identifier(index);
    D1("user = %s, data = %s, schema = %s, table = %s, index = %s, schema_table = %s", user, data, schema ? schema : "(null)", table, index, schema_table);
    initStringInfo(&name);
    appendStringInfo(&name, "%s_%s_idx", table, index);
    name_quote = quote_identifier(name.data);
    initStringInfo(&buf);
    appendStringInfo(&buf, "CREATE INDEX %s ON %s USING btree (%s)", name_quote, schema_table, index_quote);
    names = stringToQualifiedNameList(name_quote);
    relation = makeRangeVarFromNameList(names);
    SPI_connect_my(buf.data);
    if (!OidIsValid(RangeVarGetRelid(relation, NoLock, true))) SPI_execute_with_args_my(buf.data, 0, NULL, NULL, NULL, SPI_OK_UTILITY, false);
    SPI_commit_my();
    SPI_finish_my();
    pfree((void *)relation);
    list_free_deep(names);
    pfree(buf.data);
    pfree(name.data);
    if (name_quote != name.data) pfree((void *)name_quote);
    if (index_quote != index) pfree((void *)index_quote);
}

#define SyncStandbysDefined() (SyncRepStandbyNames != NULL && SyncRepStandbyNames[0] != '\0')
static void save_check(void) {
    const char *schema_quote = schema ? quote_identifier(schema) : NULL;
    const char *table_quote = quote_identifier(table);
    StringInfoData buf;
    MemoryContext oldMemoryContext = MemoryContextSwitchTo(TopMemoryContext);
    initStringInfo(&buf);
    MemoryContextSwitchTo(oldMemoryContext);
    if (schema) appendStringInfo(&buf, "%s.", schema_quote);
    appendStringInfoString(&buf, table_quote);
    schema_table = buf.data;
    if (schema && schema_quote && schema != schema_quote) pfree((void *)schema_quote);
    if (table != table_quote) pfree((void *)table_quote);
    D1("user = %s, data = %s, schema = %s, table = %s, timeout = %i, schema_table = %s", user, data, schema ? schema : "(null)", table, timeout, schema_table);
    if (SyncStandbysDefined()) {
    }
    if (!StandbyMode) {
        save_user();
        save_data();
        if (schema) save_schema();
        save_type();
        save_table();
        save_index("dt");
        save_index("state");
    }
}

static void save_init(void) {
    D1("save_init");
    if (!EnableHotStandby) E("!EnableHotStandby");
    if (!MyProcPort && !(MyProcPort = (Port *)calloc(1, sizeof(Port)))) E("!calloc");
    if (!MyProcPort->user_name) MyProcPort->user_name = user;
    if (!MyProcPort->database_name) MyProcPort->database_name = data;
    if (!MyProcPort->remote_host) MyProcPort->remote_host = "[local]";
    set_config_option("application_name", MyBgworkerEntry->bgw_type, PGC_USERSET, PGC_S_SESSION, GUC_ACTION_SET, true, ERROR, false);
    pqsignal(SIGHUP, init_sighup);
    pqsignal(SIGTERM, init_sigterm);
    BackgroundWorkerUnblockSignals();
    BackgroundWorkerInitializeConnection(data, user, 0);
    pgstat_report_appname(MyBgworkerEntry->bgw_type);
    process_session_preload_libraries();
}

static void save_reload(void) {
    sighup = false;
    ProcessConfigFile(PGC_SIGHUP);
}

static void save_latch(void) {
    ResetLatch(MyLatch);
    CHECK_FOR_INTERRUPTS();
    if (sighup) save_reload();
}

void save_worker(Datum main_arg); void save_worker(Datum main_arg) {
    TimestampTz stop = GetCurrentTimestamp(), start = stop;
    save_init();
    save_check();
    while (!sigterm) {
        int nevents = 2;
        WaitEvent *events = palloc0(nevents * sizeof(*events));
        WaitEventSet *set = CreateWaitEventSet(TopMemoryContext, nevents);
        AddWaitEventToSet(set, WL_LATCH_SET, PGINVALID_SOCKET, MyLatch, NULL);
        AddWaitEventToSet(set, WL_EXIT_ON_PM_DEATH, PGINVALID_SOCKET, NULL, NULL);
        nevents = WaitEventSetWait(set, timeout, events, nevents, PG_WAIT_EXTENSION);
        for (int i = 0; i < nevents; i++) {
            WaitEvent *event = &events[i];
            if (event->events & WL_LATCH_SET) D1("WL_LATCH_SET");
            if (event->events & WL_SOCKET_READABLE) D1("WL_SOCKET_READABLE");
            if (event->events & WL_SOCKET_WRITEABLE) D1("WL_SOCKET_WRITEABLE");
            if (event->events & WL_POSTMASTER_DEATH) D1("WL_POSTMASTER_DEATH");
            if (event->events & WL_EXIT_ON_PM_DEATH) D1("WL_EXIT_ON_PM_DEATH");
            if (event->events & WL_LATCH_SET) save_latch();
            if (event->events & WL_SOCKET_MASK) save_socket(event->user_data);
        }
        stop = GetCurrentTimestamp();
        if (timeout > 0 && (TimestampDifferenceExceeds(start, stop, timeout) || !nevents)) {
            save_timeout();
            start = stop;
        }
        FreeWaitEventSet(set);
        pfree(events);
    }
}
