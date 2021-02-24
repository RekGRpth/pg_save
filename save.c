#include "include.h"

extern char *data;
extern char *schema;
extern char *table;
extern char *user;
extern int timeout;
extern volatile sig_atomic_t sighup;
extern volatile sig_atomic_t sigterm;

static void save_timeout(void) {
}

static void save_socket(void *data) {
}

static void save_schema(void) {
}

static void save_type(void) {
}

static void save_table(void) {
    if (schema) save_schema();
    save_type();
}

#define SyncStandbysDefined() (SyncRepStandbyNames != NULL && SyncRepStandbyNames[0] != '\0')
static void save_check(void) {
    if (SyncStandbysDefined()) {
    }
    if (!StandbyMode) save_table();
}

static void save_init(void) {
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
    save_check();
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
