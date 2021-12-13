#include "lib.h"

extern char *hostname;
extern int init_timeout;

static void save_init(void) {
    if (!EnableHotStandby) elog(ERROR, "hot standby is not set");
    set_config_option("application_name", hostname, PGC_USERSET, PGC_S_SESSION, GUC_ACTION_SET, true, ERROR, false);
    pqsignal(SIGHUP, SignalHandlerForConfigReload);
    pqsignal(SIGTERM, SignalHandlerForShutdownRequest);
    BackgroundWorkerUnblockSignals();
#if PG_VERSION_NUM >= 110000
    BackgroundWorkerInitializeConnection("postgres", "postgres", 0);
#else
    BackgroundWorkerInitializeConnection("postgres", "postgres");
#endif
    pgstat_report_appname(hostname);
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

void save_worker(Datum main_arg) {
    instr_time cur_time;
    instr_time start_time;
    long cur_timeout = -1;
    save_init();
    while (!ShutdownRequestPending) {
        int nevents = 2 + backend_nevents();
        WaitEvent *events = MemoryContextAllocZero(TopMemoryContext, nevents * sizeof(*events));
        WaitEventSet *set = CreateWaitEventSet(TopMemoryContext, nevents);
        backend_event(set);
        if (init_timeout >= 0 && cur_timeout <= 0) {
            INSTR_TIME_SET_CURRENT(start_time);
            cur_timeout = init_timeout;
        }
#if PG_VERSION_NUM >= 100000
        nevents = WaitEventSetWait(set, cur_timeout, events, nevents, PG_WAIT_EXTENSION);
#else
        nevents = WaitEventSetWait(set, cur_timeout, events, nevents);
#endif
        for (int i = 0; i < nevents; i++) {
            WaitEvent *event = &events[i];
            if (event->events & WL_LATCH_SET) save_latch();
            if (event->events & WL_POSTMASTER_DEATH) ShutdownRequestPending = true;
            if (event->events & WL_SOCKET_READABLE) backend_readable(event->user_data);
            if (event->events & WL_SOCKET_WRITEABLE) backend_writeable(event->user_data);
        }
        if (init_timeout >= 0) {
            INSTR_TIME_SET_CURRENT(cur_time);
            INSTR_TIME_SUBTRACT(cur_time, start_time);
            cur_timeout = init_timeout - (long)INSTR_TIME_GET_MILLISEC(cur_time);
            if (cur_timeout <= 0) backend_timeout();
        }
        FreeWaitEventSet(set);
        pfree(events);
    }
    backend_fini();
}
