#include "include.h"

typedef enum STATE {ASYNC, POTENTIAL, SYNC, QUORUM} STATE;

typedef struct Standby {
    int reset;
    PGconn *conn;
    queue_t queue;
    STATE state;
} Standby;

typedef struct Primary {
    int reset;
    PGconn *conn;
} Primary;

extern char *hostname;
extern int reset;
static Primary primary;
static queue_t save_queue;

static char *standby_int2char(int number) {
    StringInfoData buf;
    initStringInfo(&buf);
    appendStringInfo(&buf, "%i", number);
    return buf.data;
}

static PGconn *standby_connect(const char *host, int port, const char *user, const char *dbname) {
    char *cport = standby_int2char(port);
    const char *keywords[] = {"host", "port", "user", "dbname", "application_name", NULL};
    const char *values[] = {host, cport, user, dbname, "pg_save", NULL};
    PGconn *conn;
    StaticAssertStmt(countof(keywords) == countof(values), "countof(keywords) == countof(values)");
    if (!(conn = PQconnectdbParams(keywords, values, false))) E("!PQconnectdbParams and %s", PQerrorMessage(conn));
    pfree(cport);
    if (PQstatus(conn) == CONNECTION_BAD) E("PQstatus == CONNECTION_BAD and %s", PQerrorMessage(conn));
    if (PQclientEncoding(conn) != GetDatabaseEncoding()) PQsetClientEncoding(conn, GetDatabaseEncodingName());
    return conn;
}

static void standby_primary_init(const char *host, int port, const char *user, const char *dbname) {
    char *cluster_name = GetConfigOptionByName("cluster_name", NULL, false);
    const char *cluster_name_quote = quote_identifier(cluster_name);
    PGresult *result;
    StringInfoData buf;
    primary.conn = standby_connect(host, port, user, dbname);
    primary.reset = reset;
    initStringInfo(&buf);
    appendStringInfo(&buf, "ALTER SYSTEM SET synchronous_standby_names TO 'FIRST 1 (%s)'", cluster_name_quote);
    if (cluster_name_quote != cluster_name) pfree((void *)cluster_name_quote);
    pfree(cluster_name);
    if (!(result = PQexec(primary.conn, buf.data))) E("!PQexec and %s", PQerrorMessage(primary.conn));
    pfree(buf.data);
    if (PQresultStatus(result) != PGRES_COMMAND_OK) E("%s != PGRES_COMMAND_OK and %s", PQresStatus(PQresultStatus(result)), PQresultErrorMessage(result));
    PQclear(result);
    if (!(result = PQexec(primary.conn, "SELECT pg_reload_conf()"))) E("!PQexec and %s", PQerrorMessage(primary.conn));
    if (PQresultStatus(result) != PGRES_TUPLES_OK) E("%s != PGRES_TUPLES_OK and %s", PQresStatus(PQresultStatus(result)), PQresultErrorMessage(result));
    PQclear(result);
}

static void standby_standby_init(const char *host, int port, const char *user, const char *dbname, STATE state) {
    Standby *standby = palloc0(sizeof(*standby));
    standby->state = state;
    standby->conn = standby_connect(host, port, user, dbname);
    queue_insert_tail(&save_queue, &standby->queue);
}

static void standby_primary(void) {
    PGresult *result;
    int nParams = queue_size(&save_queue);
    Oid *paramTypes = nParams ? palloc(nParams * sizeof(*paramTypes)) : NULL;
    char **paramValues = nParams ? palloc(nParams * sizeof(**paramValues)) : NULL;
    StringInfoData buf;
    initStringInfo(&buf);
    appendStringInfoString(&buf, "SELECT client_addr AS addr, coalesce(client_hostname, client_addr::text) AS host, sync_state AS state FROM pg_stat_replication WHERE client_addr IS DISTINCT FROM (SELECT client_addr FROM pg_stat_activity WHERE pid = pg_backend_pid())");
    nParams = 0;
    queue_each(&save_queue, queue) {
        Standby *standby = queue_data(queue, Standby, queue);
        if (nParams) appendStringInfoString(&buf, ", ");
        else appendStringInfoString(&buf, " AND client_addr NOT IN (");
        paramTypes[nParams] = INETOID;
        paramValues[nParams] = PQhostaddr(standby->conn);
        nParams++;
        appendStringInfo(&buf, "$%i", nParams);
    }
    if (nParams) appendStringInfoString(&buf, ")");
    if (!(result = PQexecParams(primary.conn, buf.data, nParams, paramTypes, (const char * const*)paramValues, NULL, NULL, false))) E("!PQexecParams and %s", PQerrorMessage(primary.conn));
    if (PQresultStatus(result) != PGRES_TUPLES_OK) {
        if (PQstatus(primary.conn) == CONNECTION_BAD) {
            PQreset(primary.conn);
            if (!--primary.reset) init_kill();
            W("%i < %i", primary.reset, reset);
            goto PQclear;
        }
        E("%s != PGRES_TUPLES_OK and %s", PQresStatus(PQresultStatus(result)), PQresultErrorMessage(result));
    }
    primary.reset = reset;
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
        standby_standby_init(host, 5432, MyProcPort->user_name, MyProcPort->database_name, state);
    }
PQclear:
    PQclear(result);
    if (paramTypes) pfree(paramTypes);
    if (paramValues) pfree(paramValues);
    pfree(buf.data);
}

static void standby_finish(Standby *standby) {
    queue_remove(&standby->queue);
    PQfinish(standby->conn);
    pfree(standby);
}

void standby_init(void) {
    bool ready_to_display;
    char sender_host[NI_MAXHOST];
    int pid;
    int sender_port = 0;
    queue_init(&save_queue);
    SpinLockAcquire(&WalRcv->mutex);
    pid = (int)WalRcv->pid;
    ready_to_display = WalRcv->ready_to_display;
    sender_port = WalRcv->sender_port;
    strlcpy(sender_host, (char *)WalRcv->sender_host, sizeof(sender_host));
    SpinLockRelease(&WalRcv->mutex);
    if (!pid) E("!pid");
    if (!ready_to_display) E("!ready_to_display");
    D1("sender_host = %s, sender_port = %i", sender_host, sender_port);
    standby_primary_init(sender_host, sender_port, MyProcPort->user_name, MyProcPort->database_name);
}

void standby_timeout(void) {
    if (!save_etcd_kv_put(hostname, timestamptz_to_str(GetCurrentTimestamp()), 0)) {
        W("!save_etcd_kv_put");
        init_kill();
    }
    standby_primary();
}

void standby_fini(void) {
    PQfinish(primary.conn);
    queue_each(&save_queue, queue) {
        Standby *standby = queue_data(queue, Standby, queue);
        standby_finish(standby);
    }
}

//WriteRecoveryConfig(pgconn, target_dir, GenerateRecoveryConfig(pgconn, replication_slot));
