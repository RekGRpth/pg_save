#include "include.h"
#include <sys/utsname.h>

typedef struct Backend {
    bool connected;
    int events;
    int fd;
    int pid;
    PGconn *conn;
    queue_t queue;
} Backend;

extern int timeout;
static char *hostname;
static Oid etcd_kv_put;
static queue_t save_queue;
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

static bool save_etcd_kv_put(const char *key, const char *value, int ttl) {
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

static void save_timeout(void) {
    if (!RecoveryInProgress()) {
        if (!save_etcd_kv_put("main", hostname, 60)) E("!save_etcd_kv_put");
    }
}

static void save_free(Backend *backend) {
    pfree(backend);
}

static void save_finish(Backend *backend) {
    queue_remove(&backend->queue);
    PQfinish(backend->conn);
    save_free(backend);
}

static void save_error(Backend *backend, const char *msg) {
    char *err = PQerrorMessage(backend->conn);
    int len = strlen(err);
    StringInfoData buf;
    initStringInfo(&buf);
    appendStringInfoString(&buf, msg);
    if (len) appendStringInfo(&buf, " and %.*s", len - 1, err);
    W(buf.data);
    pfree(buf.data);
    save_finish(backend);
}

static void save_query(Backend *backend) {
/*    StringInfoData buf;
    List *list;
    if (task_work(task)) { work_finish(task); return; }
    D1("id = %li, timeout = %i, input = %s, count = %i", task->id, task->timeout, task->input, task->count);
    PG_TRY();
        list = pg_parse_query(task->input);
        task->length = list_length(list);
        list_free_deep(list);
    PG_CATCH();
        FlushErrorState();
    PG_END_TRY();
    initStringInfo(&buf);
    task->skip = 0;
    appendStringInfo(&buf, "SET \"pg_task.id\" = %li;\n", task->id);
    task->skip++;
    if (task->timeout) {
        appendStringInfo(&buf, "SET \"statement_timeout\" = %i;\n", task->timeout);
        task->skip++;
    }
    if (task->append) {
        appendStringInfoString(&buf, "SET \"config.append_type_to_column_name\" = true;\n");
        task->skip++;
    }
    appendStringInfoString(&buf, task->input);
    pfree(task->input);
    task->input = buf.data;
    if (!PQsendQuery(task->conn, task->input)) work_error(task, "!PQsendQuery"); else {
        pfree(task->input);
        task->input = NULL;
        task->events = WL_SOCKET_WRITEABLE;
    }*/
}

static void save_result(Backend *backend) {
    if (!PQconsumeInput(backend->conn)) save_error(backend, "!PQconsumeInput"); else
    if (PQisBusy(backend->conn)) backend->events = WL_SOCKET_READABLE; else {
        for (PGresult *result; (result = PQgetResult(backend->conn)); PQclear(result)) switch (PQresultStatus(result)) {
//            case PGRES_COMMAND_OK: save_command(backend, result); break;
//            case PGRES_FATAL_ERROR: save_fail(backend, result); break;
//            case PGRES_TUPLES_OK: save_success(backend, result); break;
            default: D1(PQresStatus(PQresultStatus(result))); break;
        }
//        save_repeat(backend);
    }
}

static void save_connect(Backend *backend) {
    switch (PQstatus(backend->conn)) {
        case CONNECTION_AUTH_OK: D1("PQstatus == CONNECTION_AUTH_OK"); break;
        case CONNECTION_AWAITING_RESPONSE: D1("PQstatus == CONNECTION_AWAITING_RESPONSE"); break;
        case CONNECTION_BAD: D1("PQstatus == CONNECTION_BAD"); save_error(backend, "PQstatus == CONNECTION_BAD"); return;
#if (PG_VERSION_NUM >= 130000)
        case CONNECTION_CHECK_TARGET: D1("PQstatus == CONNECTION_CHECK_TARGET"); break;
#endif
        case CONNECTION_CHECK_WRITABLE: D1("PQstatus == CONNECTION_CHECK_WRITABLE"); break;
        case CONNECTION_CONSUME: D1("PQstatus == CONNECTION_CONSUME"); break;
        case CONNECTION_GSS_STARTUP: D1("PQstatus == CONNECTION_GSS_STARTUP"); break;
        case CONNECTION_MADE: D1("PQstatus == CONNECTION_MADE"); break;
        case CONNECTION_NEEDED: D1("PQstatus == CONNECTION_NEEDED"); break;
        case CONNECTION_OK: D1("PQstatus == CONNECTION_OK"); backend->connected = true; break;
        case CONNECTION_SETENV: D1("PQstatus == CONNECTION_SETENV"); break;
        case CONNECTION_SSL_STARTUP: D1("PQstatus == CONNECTION_SSL_STARTUP"); break;
        case CONNECTION_STARTED: D1("PQstatus == CONNECTION_STARTED"); break;
    }
    switch (PQconnectPoll(backend->conn)) {
        case PGRES_POLLING_ACTIVE: D1("PQconnectPoll == PGRES_POLLING_ACTIVE"); break;
        case PGRES_POLLING_FAILED: D1("PQconnectPoll == PGRES_POLLING_FAILED"); save_error(backend, "PQconnectPoll == PGRES_POLLING_FAILED"); return;
        case PGRES_POLLING_OK: D1("PQconnectPoll == PGRES_POLLING_OK"); backend->connected = true; break;
        case PGRES_POLLING_READING: D1("PQconnectPoll == PGRES_POLLING_READING"); backend->events = WL_SOCKET_READABLE; break;
        case PGRES_POLLING_WRITING: D1("PQconnectPoll == PGRES_POLLING_WRITING"); backend->events = WL_SOCKET_WRITEABLE; break;
    }
    if ((backend->fd = PQsocket(backend->conn)) < 0) save_error(backend, "PQsocket < 0");
    if (backend->connected) {
        if (!(backend->pid = PQbackendPID(backend->conn))) save_error(backend, "!PQbackendPID"); else save_query(backend);
    }
}

static void save_socket(Backend *backend) {
    D1("connected = %s", backend->connected ? "true" : "false");
    if (backend->connected) save_result(backend); else save_connect(backend);
}

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

static char *int2char(int number) {
    StringInfoData buf;
    initStringInfo(&buf);
    appendStringInfo(&buf, "%i", number);
    return buf.data;
}

static void save_backend(const char *host, int port) {
    char *cport = int2char(port);
    const char *keywords[] = {"host", "port", "application_name", NULL};
    const char *values[] = {host, cport, "pg_save", NULL};
    Backend *backend = palloc0(sizeof(*backend));
    queue_insert_tail(&save_queue, &backend->queue);
    backend->events = WL_SOCKET_WRITEABLE;
    if (!(backend->conn = PQconnectStartParams(keywords, values, false))) save_error(backend, "!PQconnectStartParams"); else
    if (PQstatus(backend->conn) == CONNECTION_BAD) save_error(backend, "PQstatus == CONNECTION_BAD"); else
    if (!PQisnonblocking(backend->conn) && PQsetnonblocking(backend->conn, true) == -1) save_error(backend, "PQsetnonblocking == -1"); else
    if ((backend->fd = PQsocket(backend->conn)) < 0) save_error(backend, "PQsocket < 0"); else
    if (PQclientEncoding(backend->conn) != GetDatabaseEncoding()) PQsetClientEncoding(backend->conn, GetDatabaseEncodingName());
    pfree(cport);
}

static void save_standby_init(void) {
    bool ready_to_display;
    char sender_host[NI_MAXHOST];
    int pid;
    int sender_port = 0;
    SpinLockAcquire(&WalRcv->mutex);
    pid = (int)WalRcv->pid;
    ready_to_display = WalRcv->ready_to_display;
    sender_port = WalRcv->sender_port;
    strlcpy(sender_host, (char *)WalRcv->sender_host, sizeof(sender_host));
    SpinLockRelease(&WalRcv->mutex);
    if (!pid) E("!pid");
    if (!ready_to_display) E("!ready_to_display");
    D1("sender_host = %s, sender_port = %i", sender_host, sender_port);
    save_backend(sender_host, sender_port);
}

static void save_primary_init(void) {
    save_schema("curl");
    save_extension("curl", "pg_curl");
    save_schema("save");
    save_extension("save", "pg_save");
}

static void save_init(void) {
    struct utsname buf;
    if (!EnableHotStandby) E("!EnableHotStandby");
    if (uname(&buf)) E("uname");
    hostname = pstrdup(buf.nodename);
    D1("hostname = %s, timeout = %i", hostname, timeout);
    if (!MyProcPort && !(MyProcPort = (Port *)calloc(1, sizeof(Port)))) E("!calloc");
    if (!MyProcPort->user_name) MyProcPort->user_name = "postgres";
    if (!MyProcPort->database_name) MyProcPort->database_name = "postgres";
    if (!MyProcPort->remote_host) MyProcPort->remote_host = "[local]";
    queue_init(&save_queue);
    set_config_option("application_name", MyBgworkerEntry->bgw_type, PGC_USERSET, PGC_S_SESSION, GUC_ACTION_SET, true, ERROR, false);
    pqsignal(SIGHUP, init_sighup);
    pqsignal(SIGTERM, init_sigterm);
    BackgroundWorkerUnblockSignals();
    BackgroundWorkerInitializeConnection("postgres", "postgres", 0);
    pgstat_report_appname(MyBgworkerEntry->bgw_type);
    process_session_preload_libraries();
    if (RecoveryInProgress()) save_standby_init();
    else save_primary_init();
    etcd_kv_put = save_get_function_oid("save", "etcd_kv_put", 3, (Oid []){TEXTOID, TEXTOID, INT4OID});
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
        int nevents = queue_size(&save_queue) + 2;
        WaitEvent *events = palloc0(nevents * sizeof(*events));
        WaitEventSet *set = CreateWaitEventSet(TopMemoryContext, nevents);
        AddWaitEventToSet(set, WL_LATCH_SET, PGINVALID_SOCKET, MyLatch, NULL);
        AddWaitEventToSet(set, WL_EXIT_ON_PM_DEATH, PGINVALID_SOCKET, NULL, NULL);
        queue_each(&save_queue, queue) {
            Backend *backend = queue_data(queue, Backend, queue);
            if (backend->events & WL_SOCKET_WRITEABLE) switch (PQflush(backend->conn)) {
                case 0: /*D1("PQflush = 0");*/ break;
                case 1: D1("PQflush = 1"); break;
                default: D1("PQflush = default"); break;
            }
            AddWaitEventToSet(set, backend->events & WL_SOCKET_MASK, backend->fd, NULL, backend);
        }
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
