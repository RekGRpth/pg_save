#include "include.h"
#include <sys/utsname.h>

char *hostname;
extern int init_timeout;
queue_t backend_queue;
TimestampTz start;

static void save_fini(void) {
    RecoveryInProgress() ? standby_fini() : primary_fini();
}

static void save_init(void) {
    struct utsname buf;
    if (!EnableHotStandby) E("!EnableHotStandby");
    if (uname(&buf)) E("uname");
    queue_init(&backend_queue);
    hostname = pstrdup(buf.nodename);
    D1("hostname = %s, timeout = %i", hostname, init_timeout);
    if (!MyProcPort && !(MyProcPort = (Port *)calloc(1, sizeof(Port)))) E("!calloc");
    if (!MyProcPort->user_name) MyProcPort->user_name = "postgres";
    if (!MyProcPort->database_name) MyProcPort->database_name = "postgres";
    if (!MyProcPort->remote_host) MyProcPort->remote_host = "[local]";
    set_config_option("application_name", MyBgworkerEntry->bgw_type, PGC_USERSET, PGC_S_SESSION, GUC_ACTION_SET, true, ERROR, false);
    pqsignal(SIGHUP, SignalHandlerForConfigReload);
    pqsignal(SIGTERM, SignalHandlerForShutdownRequest);
    BackgroundWorkerUnblockSignals();
    BackgroundWorkerInitializeConnection("postgres", "postgres", 0);
    pgstat_report_appname(MyBgworkerEntry->bgw_type);
    process_session_preload_libraries();
    RecoveryInProgress() ? standby_init() : primary_init();
    init_connect();
    etcd_init();
    init_reload();
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
        if (!PQconsumeInput(backend->conn)) { W("%s:%s !PQconsumeInput and %s", backend->host, backend->state, PQerrorMessage(backend->conn)); return; }
        if (PQisBusy(backend->conn)) { backend->events = WL_SOCKET_READABLE; return; }
    }
    backend->socket(backend);
}

static void save_timeout(void) {
    etcd_timeout();
    RecoveryInProgress() ? standby_timeout() : primary_timeout();
}

void save_worker(Datum main_arg) {
    TimestampTz stop = (start = GetCurrentTimestamp());
    save_init();
    while (!ShutdownRequestPending) {
        WaitEvent *events;
        WaitEventSet *set;
        int nevents = 2;
        queue_each(&backend_queue, queue) {
            Backend *backend = queue_data(queue, Backend, queue);
            if (PQsocket(backend->conn) < 0) continue;
            nevents++;
        }
        events = MemoryContextAllocZero(TopMemoryContext, nevents * sizeof(*events));
        set = CreateWaitEventSet(TopMemoryContext, nevents);
        AddWaitEventToSet(set, WL_LATCH_SET, PGINVALID_SOCKET, MyLatch, NULL);
        AddWaitEventToSet(set, WL_EXIT_ON_PM_DEATH, PGINVALID_SOCKET, NULL, NULL);
        queue_each(&backend_queue, queue) {
            Backend *backend = queue_data(queue, Backend, queue);
            int fd = PQsocket(backend->conn);
            if (fd < 0) continue;
            if (backend->events & WL_SOCKET_WRITEABLE) switch (PQflush(backend->conn)) {
                case 0: /*D1("PQflush = 0");*/ break;
                case 1: D1("PQflush = 1"); break;
                default: D1("PQflush = default"); break;
            }
            AddWaitEventToSet(set, backend->events & WL_SOCKET_MASK, fd, NULL, backend);
        }
        nevents = WaitEventSetWait(set, init_timeout, events, nevents, PG_WAIT_EXTENSION);
        for (int i = 0; i < nevents; i++) {
            WaitEvent *event = &events[i];
            if (event->events & WL_LATCH_SET) save_latch();
            if (event->events & WL_SOCKET_MASK) save_socket(event->user_data);
        }
        stop = GetCurrentTimestamp();
        if (init_timeout > 0 && (TimestampDifferenceExceeds(start, stop, init_timeout) || !nevents)) {
            save_timeout();
            start = stop;
        }
        FreeWaitEventSet(set);
        pfree(events);
    }
    save_fini();
}
