#include "include.h"
#include <unistd.h>

extern int timeout;
static char *hostname;
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

static void save_set(const char *state) {
}

static void save_timeout(void) {
    if (!RecoveryInProgress()) {
        save_set("main");
    }
}

static void save_socket(void *data) {
}

/*#define SyncStandbysDefined() (SyncRepStandbyNames != NULL && SyncRepStandbyNames[0] != '\0')
static void save_check(void) {
    char name[1024];
    StringInfoData buf;
    MemoryContext oldMemoryContext = MemoryContextSwitchTo(TopMemoryContext);
    initStringInfo(&buf);
    name[sizeof(name) - 1] = '\0';
    if (gethostname(name, sizeof(name) - 1)) E("gethostname");
    hostname = pstrdup(name);
    MemoryContextSwitchTo(oldMemoryContext);
    D1("hostname = %s, timeout = %i", hostname, timeout);
    if (SyncStandbysDefined()) {
    }
    if (!RecoveryInProgress()) {
    }
}*/

static void save_schema(const char *schema) {
    StringInfoData buf;
    List *names;
    const char *schema_quote = quote_identifier(schema);
    D1("schema = %s", schema);
    initStringInfo(&buf);
    appendStringInfo(&buf, "CREATE SCHEMA %s", schema_quote);
    names = stringToQualifiedNameList(schema_quote);
    SPI_connect_my(buf.data);
    if (!OidIsValid(get_namespace_oid(strVal(linitial(names)), true))) SPI_execute_with_args_my(buf.data, 0, NULL, NULL, NULL, SPI_OK_UTILITY, false);
    else D1("schema %s already exists", schema_quote);
    SPI_commit_my();
    SPI_finish_my();
    list_free_deep(names);
    if (schema_quote != schema) pfree((void *)schema_quote);
    pfree(buf.data);
}

static void save_extension(const char *schema, const char *extension) {
    StringInfoData buf;
    List *names;
    const char *schema_quote = schema ? quote_identifier(schema) : NULL;
    const char *extension_quote = quote_identifier(extension);
    D1("schema = %s, extension = %s", schema ? schema : "(null)", extension);
    initStringInfo(&buf);
    appendStringInfo(&buf, "CREATE EXTENSION %s", extension_quote);
    if (schema) appendStringInfo(&buf, " SCHEMA %s", schema_quote);
    names = stringToQualifiedNameList(extension_quote);
    SPI_connect_my(buf.data);
    if (!OidIsValid(get_extension_oid(strVal(linitial(names)), true))) SPI_execute_with_args_my(buf.data, 0, NULL, NULL, NULL, SPI_OK_UTILITY, false);
    else D1("extension %s already exists", extension_quote);
    SPI_commit_my();
    SPI_finish_my();
    list_free_deep(names);
    if (schema && schema_quote != schema) pfree((void *)schema_quote);
    if (extension_quote != extension) pfree((void *)extension_quote);
    pfree(buf.data);
}

static void save_curl(void) {
    save_schema("curl");
    save_extension("curl", "pg_curl");
    save_schema("save");
    save_extension("save", "pg_save");
}

static void save_init(void) {
    char name[1024];
    name[sizeof(name) - 1] = '\0';
    if (!EnableHotStandby) E("!EnableHotStandby");
    if (gethostname(name, sizeof(name) - 1)) E("gethostname");
    hostname = pstrdup(name);
    D1("hostname = %s, timeout = %i", hostname, timeout);
    if (!MyProcPort && !(MyProcPort = (Port *)calloc(1, sizeof(Port)))) E("!calloc");
    if (!MyProcPort->user_name) MyProcPort->user_name = "postgres";
    if (!MyProcPort->database_name) MyProcPort->database_name = "postgres";
    if (!MyProcPort->remote_host) MyProcPort->remote_host = "[local]";
    set_config_option("application_name", MyBgworkerEntry->bgw_type, PGC_USERSET, PGC_S_SESSION, GUC_ACTION_SET, true, ERROR, false);
    pqsignal(SIGHUP, init_sighup);
    pqsignal(SIGTERM, init_sigterm);
    BackgroundWorkerUnblockSignals();
    BackgroundWorkerInitializeConnection("postgres", "postgres", 0);
    pgstat_report_appname(MyBgworkerEntry->bgw_type);
    process_session_preload_libraries();
    if (!RecoveryInProgress()) {
        save_curl();
    }
}

static void save_fini(void) {
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
    save_fini();
}
