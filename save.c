#include "include.h"
#include <sys/utsname.h>

typedef enum STATE {MAIN, ASYNC, POTENTIAL, SYNC, QUORUM} STATE;

typedef struct Backend {
    PGconn *conn;
    queue_t queue;
    STATE state;
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

static char *int2char(int number) {
    StringInfoData buf;
    initStringInfo(&buf);
    appendStringInfo(&buf, "%i", number);
    return buf.data;
}

static void save_main_init(Backend *backend) {
    char *cluster_name = GetConfigOptionByName("cluster_name", NULL, false);
    const char *cluster_name_quote = quote_identifier(cluster_name);
    PGresult *result;
    StringInfoData buf;
    initStringInfo(&buf);
    appendStringInfo(&buf, "ALTER SYSTEM SET synchronous_standby_names TO 'FIRST 1 (%s)'", cluster_name_quote);
    if (cluster_name_quote != cluster_name) pfree((void *)cluster_name_quote);
    pfree(cluster_name);
    if (!(result = PQexec(backend->conn, buf.data))) E("!PQexec and %s", PQerrorMessage(backend->conn));
    pfree(buf.data);
    if (PQresultStatus(result) != PGRES_COMMAND_OK) E("%s != PGRES_COMMAND_OK and %s", PQresStatus(PQresultStatus(result)), PQresultErrorMessage(result));
    PQclear(result);
    if (!(result = PQexec(backend->conn, "SELECT pg_reload_conf()"))) E("!PQexec and %s", PQerrorMessage(backend->conn));
    if (PQresultStatus(result) != PGRES_TUPLES_OK) E("%s != PGRES_TUPLES_OK and %s", PQresStatus(PQresultStatus(result)), PQresultErrorMessage(result));
    PQclear(result);
}

static void save_backend(const char *host, int port, const char *user, const char *dbname, STATE state) {
    char *cport = int2char(port);
    const char *keywords[] = {"host", "port", "user", "dbname", "application_name", NULL};
    const char *values[] = {host, cport, user, dbname, "pg_save", NULL};
    Backend *backend = palloc0(sizeof(*backend));
    backend->state = state;
    StaticAssertStmt(countof(keywords) == countof(values), "countof(keywords) == countof(values)");
    queue_insert_tail(&save_queue, &backend->queue);
    if (!(backend->conn = PQconnectdbParams(keywords, values, false))) E("!PQconnectdbParams and %s", PQerrorMessage(backend->conn));
    pfree(cport);
    if (PQstatus(backend->conn) == CONNECTION_BAD) E("PQstatus == CONNECTION_BAD and %s", PQerrorMessage(backend->conn));
    if (PQclientEncoding(backend->conn) != GetDatabaseEncoding()) PQsetClientEncoding(backend->conn, GetDatabaseEncodingName());
    if (state == MAIN) save_main_init(backend);
}

static void save_main(Backend *backend) {
    PGresult *result;
    int nParams = queue_size(&save_queue);
    Oid *paramTypes = nParams ? palloc(nParams * sizeof(*paramTypes)) : NULL;
    char **paramValues = nParams ? palloc(nParams * sizeof(**paramValues)) : NULL;
    StringInfoData buf;
    initStringInfo(&buf);
    appendStringInfoString(&buf, "SELECT client_addr AS addr, coalesce(client_hostname, client_addr::text) AS host, sync_state AS state FROM pg_stat_replication WHERE client_addr IS DISTINCT FROM (SELECT client_addr FROM pg_stat_activity WHERE pid = pg_backend_pid())");
    nParams = 0;
    queue_each(&save_queue, queue) {
        Backend *backend = queue_data(queue, Backend, queue);
        if (backend->state == MAIN) continue;
        if (nParams) appendStringInfoString(&buf, ", ");
        else appendStringInfoString(&buf, " AND client_addr NOT IN (");
        paramTypes[nParams] = INETOID;
        paramValues[nParams] = PQhostaddr(backend->conn);
        nParams++;
        appendStringInfo(&buf, "$%i", nParams);
    }
    if (nParams) appendStringInfoString(&buf, ")");
    if (!(result = PQexecParams(backend->conn, buf.data, nParams, paramTypes, (const char * const*)paramValues, NULL, NULL, false))) E("!PQexecParams and %s", PQerrorMessage(backend->conn));
    if (PQresultStatus(result) != PGRES_TUPLES_OK) E("%s != PGRES_TUPLES_OK and %s", PQresStatus(PQresultStatus(result)), PQresultErrorMessage(result));
    for (int row = 0; row < PQntuples(result); row++) {
        const char *addr = PQgetvalue(result, row, PQfnumber(result, "addr"));
        const char *host = PQgetvalue(result, row, PQfnumber(result, "host"));
        const char *cstate = PQgetvalue(result, row, PQfnumber(result, "state"));
        STATE state;
        D1("addr = %s, host = %s, state = %s", addr, host, cstate);
        if (pg_strcasecmp(cstate, "async")) state = ASYNC;
        else if (pg_strcasecmp(cstate, "potential")) state = POTENTIAL;
        else if (pg_strcasecmp(cstate, "sync")) state = SYNC;
        else if (pg_strcasecmp(cstate, "quorum")) state = QUORUM;
        else E("unknown state = %s", cstate);
        save_backend(host, 5432, MyProcPort->user_name, MyProcPort->database_name, state);
    }
    PQclear(result);
    if (paramTypes) pfree(paramTypes);
    if (paramValues) pfree(paramValues);
    pfree(buf.data);
}

static void save_timeout(void) {
    if (RecoveryInProgress()) {
        queue_each(&save_queue, queue) {
            Backend *backend = queue_data(queue, Backend, queue);
            if (backend->state == MAIN) save_main(backend);
        }
    } else {
        if (!save_etcd_kv_put("main", hostname, 60)) E("!save_etcd_kv_put");
    }
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
    save_backend(sender_host, sender_port, MyProcPort->user_name, MyProcPort->database_name, MAIN);
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
    while (!queue_empty(&save_queue)) {
        queue_t *queue = queue_head(&save_queue);
        Backend *backend = queue_data(queue, Backend, queue);
        queue_remove(&backend->queue);
        PQfinish(backend->conn);
        pfree(backend);
    }
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
    save_init();
    while (!sigterm) {
        int events = WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH, timeout, PG_WAIT_EXTENSION);
        if (events & WL_LATCH_SET) save_latch();
        if (events & WL_TIMEOUT) save_timeout();
    }
    save_fini();
}
