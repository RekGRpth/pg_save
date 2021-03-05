#include "include.h"
#include <sys/utsname.h>

char *hostname;
extern int timeout;
queue_t backend_queue;
static Oid etcd_kv_put;
static Oid etcd_kv_range;
TimestampTz start;
volatile sig_atomic_t sighup = false;
volatile sig_atomic_t sigterm = false;

static void save_sighup(SIGNAL_ARGS) {
    int save_errno = errno;
    sighup = true;
    SetLatch(MyLatch);
    errno = save_errno;
}

static void save_sigterm(SIGNAL_ARGS) {
    int save_errno = errno;
    sigterm = true;
    SetLatch(MyLatch);
    errno = save_errno;
}

static Oid save_get_function_oid(const char *schema, const char *function, int nargs, const Oid *argtypes) {
    Oid oid;
    const char *schema_quote = schema ? quote_identifier(schema) : NULL;
    const char *function_quote = quote_identifier(function);
    List *funcname;
    StringInfoData buf;
    initStringInfo(&buf);
    if (schema) appendStringInfo(&buf, "%s.", schema_quote);
    appendStringInfoString(&buf, function_quote);
    funcname = stringToQualifiedNameList(buf.data);
    SPI_connect_my(buf.data);
    oid = LookupFuncName(funcname, nargs, argtypes, false);
    SPI_commit_my();
    SPI_finish_my();
    list_free_deep(funcname);
    if (schema && schema_quote != schema) pfree((void *)schema_quote);
    if (function_quote != function) pfree((void *)function_quote);
    return oid;
}

bool save_etcd_kv_put(const char *key, const char *value, int ttl) {
    Datum key_datum = CStringGetTextDatum(key);
    Datum value_datum = CStringGetTextDatum(value);
    Datum ok;
    SPI_connect_my("etcd_kv_put");
    ok = OidFunctionCall3(etcd_kv_put, key_datum, value_datum, Int32GetDatum(ttl));
    SPI_commit_my();
    SPI_finish_my();
    pfree((void *)key_datum);
    pfree((void *)value_datum);
    return DatumGetBool(ok);
}

char *save_etcd_kv_range(const char *key) {
    Datum key_datum = CStringGetTextDatum(key);
    Datum value;
    SPI_connect_my("etcd_kv_range");
    value = OidFunctionCall1(etcd_kv_range, key_datum);
    SPI_commit_my();
    SPI_finish_my();
    pfree((void *)key_datum);
    return TextDatumGetCStringMy(value);
}

static void save_timeout(void) {
    if (RecoveryInProgress()) standby_timeout();
    else primary_timeout();
}

static void save_init(void) {
    struct utsname buf;
    if (!EnableHotStandby) E("!EnableHotStandby");
    if (uname(&buf)) E("uname");
    queue_init(&backend_queue);
    hostname = pstrdup(buf.nodename);
    D1("hostname = %s, timeout = %i", hostname, timeout);
    if (!MyProcPort && !(MyProcPort = (Port *)calloc(1, sizeof(Port)))) E("!calloc");
    if (!MyProcPort->user_name) MyProcPort->user_name = "postgres";
    if (!MyProcPort->database_name) MyProcPort->database_name = "postgres";
    if (!MyProcPort->remote_host) MyProcPort->remote_host = "[local]";
    set_config_option("application_name", MyBgworkerEntry->bgw_type, PGC_USERSET, PGC_S_SESSION, GUC_ACTION_SET, true, ERROR, false);
    pqsignal(SIGHUP, save_sighup);
    pqsignal(SIGTERM, save_sigterm);
    BackgroundWorkerUnblockSignals();
    BackgroundWorkerInitializeConnection("postgres", "postgres", 0);
    pgstat_report_appname(MyBgworkerEntry->bgw_type);
    process_session_preload_libraries();
    if (RecoveryInProgress()) standby_init();
    else primary_init();
    etcd_kv_put = save_get_function_oid("save", "etcd_kv_put", 3, (Oid []){TEXTOID, TEXTOID, INT4OID});
    etcd_kv_range = save_get_function_oid("save", "etcd_kv_range", 1, (Oid []){TEXTOID});
}

static void save_fini(void) {
    if (RecoveryInProgress()) standby_fini();
    else primary_fini();
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

static void save_socket(void *data) {
    Backend *backend = data;
    backend->callback(backend);
}

void save_worker(Datum main_arg); void save_worker(Datum main_arg) {
    TimestampTz stop = (start = GetCurrentTimestamp());
    save_init();
    while (!sigterm) {
        int nevents = queue_size(&backend_queue) + 2;
        WaitEvent *events = palloc0(nevents * sizeof(*events));
        WaitEventSet *set = CreateWaitEventSet(TopMemoryContext, nevents);
        AddWaitEventToSet(set, WL_LATCH_SET, PGINVALID_SOCKET, MyLatch, NULL);
        AddWaitEventToSet(set, WL_EXIT_ON_PM_DEATH, PGINVALID_SOCKET, NULL, NULL);
        queue_each(&backend_queue, queue) {
            Backend *backend = queue_data(queue, Backend, queue);
            int fd = PQsocket(backend->conn);
            if (fd < 0) { W("%s:%s PQsocket < 0 and %s", PQhost(backend->conn), PQport(backend->conn), PQerrorMessage(backend->conn)); continue; }
            if (backend->events & WL_SOCKET_WRITEABLE) switch (PQflush(backend->conn)) {
                case 0: /*D1("PQflush = 0");*/ break;
                case 1: D1("PQflush = 1"); break;
                default: D1("PQflush = default"); break;
            }
            AddWaitEventToSet(set, backend->events & WL_SOCKET_MASK, fd, NULL, backend);
        }
        nevents = WaitEventSetWait(set, timeout, events, nevents, PG_WAIT_EXTENSION);
        for (int i = 0; i < nevents; i++) {
            WaitEvent *event = &events[i];
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
