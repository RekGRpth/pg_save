#include "include.h"

char *save_schema_type;
extern int init_timeout;
queue_t save_queue;

static void save_fini(void) {
    backend_fini();
}

static void save_type(const char *schema, const char *name) {
    StringInfoData buf;
    int32 typmod;
    Oid type;
    const char *schema_quote = schema ? quote_identifier(schema) : NULL;
    const char *name_quote = quote_identifier(name);
    D1("schema = %s, name = %s", schema ? schema : "(null)", name);
    initStringInfoMy(TopMemoryContext, &buf);
    if (schema) appendStringInfo(&buf, "%s.", schema_quote);
    appendStringInfoString(&buf, name);
    save_schema_type = buf.data;
    initStringInfoMy(TopMemoryContext, &buf);
    appendStringInfo(&buf, "CREATE TYPE %s AS (application_name text, sync_state text)", save_schema_type);
    SPI_connect_my(buf.data);
    parseTypeString(save_schema_type, &type, &typmod, true);
    if (OidIsValid(type)) D1("type %s already exists", save_schema_type); else {
        if (RecoveryInProgress()) E("!OidIsValid and RecoveryInProgress");
        else SPI_execute_with_args_my(buf.data, 0, NULL, NULL, NULL, SPI_OK_UTILITY, false);
    }
    SPI_commit_my();
    SPI_finish_my();
    if (schema && schema_quote != schema) pfree((void *)schema_quote);
    if (name_quote != name) pfree((void *)name_quote);
    pfree(buf.data);
}

static void save_init(void) {
    if (!EnableHotStandby) E("!EnableHotStandby");
    if (!MyProcPort && !(MyProcPort = (Port *)calloc(1, sizeof(Port)))) E("!calloc");
    if (!MyProcPort->user_name) MyProcPort->user_name = "postgres";
    if (!MyProcPort->database_name) MyProcPort->database_name = "postgres";
    if (!MyProcPort->remote_host) MyProcPort->remote_host = "[local]";
    queue_init(&save_queue);
    D1("hostname = %s, timeout = %i", MyBgworkerEntry->bgw_type, init_timeout);
    set_config_option("application_name", MyBgworkerEntry->bgw_type, PGC_USERSET, PGC_S_SESSION, GUC_ACTION_SET, true, ERROR, false);
    pqsignal(SIGHUP, SignalHandlerForConfigReload);
    pqsignal(SIGTERM, SignalHandlerForShutdownRequest);
    BackgroundWorkerUnblockSignals();
    BackgroundWorkerInitializeConnection("postgres", "postgres", 0);
    pgstat_report_appname(MyBgworkerEntry->bgw_type);
    process_session_preload_libraries();
    backend_init();
    save_type("save", "save");
}

static void save_reload(void) {
    ConfigReloadPending = false;
    ProcessConfigFile(PGC_SIGHUP);
    init_debug();
}

static void save_latch(void) {
    ResetLatch(MyLatch);
    CHECK_FOR_INTERRUPTS();
    if (ConfigReloadPending) save_reload();
}

static void save_socket(Backend *backend) {
    if (PQstatus(backend->conn) == CONNECTION_OK) {
        if (!PQconsumeInput(backend->conn)) { W("%s:%s !PQconsumeInput and %.*s", PQhost(backend->conn), init_state2char(backend->state), (int)strlen(PQerrorMessage(backend->conn)) - 1, PQerrorMessage(backend->conn)); return; }
        if (PQisBusy(backend->conn)) { backend->events = WL_SOCKET_READABLE; return; }
    }
    backend->socket(backend);
}

static void save_timeout(void) {
    backend_timeout();
}

void save_worker(Datum main_arg) {
    instr_time cur_time;
    instr_time start_time;
    long cur_timeout = -1;
    save_init();
    while (!ShutdownRequestPending) {
        int nevents = 2;
        WaitEvent *events;
        WaitEventSet *set;
        if (init_timeout >= 0 && cur_timeout <= 0) {
            INSTR_TIME_SET_CURRENT(start_time);
            cur_timeout = init_timeout;
        }
        queue_each(&save_queue, queue) {
            Backend *backend = queue_data(queue, Backend, queue);
            if (PQstatus(backend->conn) == CONNECTION_BAD) continue;
            if (PQsocket(backend->conn) < 0) continue;
            nevents++;
        }
        events = MemoryContextAllocZero(TopMemoryContext, nevents * sizeof(*events));
        set = CreateWaitEventSet(TopMemoryContext, nevents);
        AddWaitEventToSet(set, WL_LATCH_SET, PGINVALID_SOCKET, MyLatch, NULL);
        AddWaitEventToSet(set, WL_EXIT_ON_PM_DEATH, PGINVALID_SOCKET, NULL, NULL);
        queue_each(&save_queue, queue) {
            int fd;
            Backend *backend = queue_data(queue, Backend, queue);
            if (PQstatus(backend->conn) == CONNECTION_BAD) continue;
            if ((fd = PQsocket(backend->conn)) < 0) continue;
            if (backend->events & WL_SOCKET_WRITEABLE) switch (PQflush(backend->conn)) {
                case 0: /*D1("PQflush = 0");*/ break;
                case 1: D1("PQflush = 1"); break;
                default: D1("PQflush = default"); break;
            }
            AddWaitEventToSet(set, backend->events & WL_SOCKET_MASK, fd, NULL, backend);
        }
        nevents = WaitEventSetWait(set, cur_timeout, events, nevents, PG_WAIT_EXTENSION);
        for (int i = 0; i < nevents; i++) {
            WaitEvent *event = &events[i];
            if (event->events & WL_LATCH_SET) save_latch();
            if (event->events & WL_SOCKET_MASK) save_socket(event->user_data);
        }
        if (init_timeout >= 0) {
            INSTR_TIME_SET_CURRENT(cur_time);
            INSTR_TIME_SUBTRACT(cur_time, start_time);
            cur_timeout = init_timeout - (long)INSTR_TIME_GET_MILLISEC(cur_time);
            if (cur_timeout <= 0) save_timeout();
        }
        FreeWaitEventSet(set);
        pfree(events);
    }
    save_fini();
}
