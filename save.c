#include "include.h"

extern int init_timeout;

static void save_fini(void) {
    backend_fini();
}

static void save_exit(int code, Datum arg) {
    D1("code = %i", code);
    save_fini();
}

static void save_init(void) {
    if (!EnableHotStandby) E("!EnableHotStandby");
    if (!MyProcPort && !(MyProcPort = (Port *)calloc(1, sizeof(Port)))) E("!calloc");
    if (!MyProcPort->user_name) MyProcPort->user_name = "postgres";
    if (!MyProcPort->database_name) MyProcPort->database_name = "postgres";
    if (!MyProcPort->remote_host) MyProcPort->remote_host = "[local]";
    set_config_option("application_name", MyBgworkerEntry->bgw_type, PGC_USERSET, PGC_S_SESSION, GUC_ACTION_SET, true, ERROR, false);
    pqsignal(SIGHUP, SignalHandlerForConfigReload);
    on_proc_exit(save_exit, PointerGetDatum(NULL));
    BackgroundWorkerUnblockSignals();
    BackgroundWorkerInitializeConnection("postgres", "postgres", 0);
    pgstat_report_appname(MyBgworkerEntry->bgw_type);
    process_session_preload_libraries();
    backend_init();
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
    for (;;) {
        int nevents = 2 + backend_nevents();
        WaitEvent *events = MemoryContextAllocZero(TopMemoryContext, nevents * sizeof(*events));
        WaitEventSet *set = CreateWaitEventSet(TopMemoryContext, nevents);
        if (init_timeout >= 0 && cur_timeout <= 0) {
            INSTR_TIME_SET_CURRENT(start_time);
            cur_timeout = init_timeout;
        }
        AddWaitEventToSet(set, WL_LATCH_SET, PGINVALID_SOCKET, MyLatch, NULL);
        AddWaitEventToSet(set, WL_EXIT_ON_PM_DEATH, PGINVALID_SOCKET, NULL, NULL);
        backend_event(set);
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
    proc_exit(1);
}
