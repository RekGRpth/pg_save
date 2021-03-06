#include "include.h"

extern Backend *primary;
extern char *hostname;
extern queue_t backend_queue;
extern TimestampTz start;
static char *sender_host;
static char *slot_name;
static int sender_port;
static STATE my_state;

static void standby_reprimary(void) {
    D1("hi");
    //WriteRecoveryConfig(pgconn, target_dir, GenerateRecoveryConfig(pgconn, replication_slot));
}

static void standby_promote(void) {
    D1("hi");
//    if (!DatumGetBool(DirectFunctionCall2(pg_promote, BoolGetDatum(true), Int32GetDatum(30)))) E("!pg_promote");
//    else standby_fini();
}

static void standby_reset(Backend *backend) {
    if (backend->state != PRIMARY) { backend_finish(backend); return; }
    D1("my_state = %i", my_state);
    if (my_state != SYNC) standby_reprimary();
    else if (queue_size(&backend_queue) > 1) standby_promote();
    else init_kill();
}

static void standby_reload_conf_socket(Backend *backend) {
    if (!PQconsumeInput(backend->conn)) { W("%s:%s !PQconsumeInput and %s", PQhost(backend->conn), PQport(backend->conn), PQerrorMessage(backend->conn)); return; }
    if (PQisBusy(backend->conn)) { backend->events = WL_SOCKET_READABLE; return; }
    for (PGresult *result; (result = PQgetResult(backend->conn)); PQclear(result)) switch (PQresultStatus(result)) {
        case PGRES_TUPLES_OK: break;
        default: E("%s:%s PQresultStatus = %s and %s", PQhost(backend->conn), PQport(backend->conn), PQresStatus(PQresultStatus(result)), PQresultErrorMessage(result)); break;
    }
    backend_idle(backend);
}

static void standby_reload_conf(Backend *backend) {
    if (PQisBusy(backend->conn)) { backend->events = WL_SOCKET_READABLE; return; }
    if (!PQsendQuery(backend->conn, "SELECT pg_reload_conf()")) E("%s:%s !PQsendQuery and %s", PQhost(backend->conn), PQport(backend->conn), PQerrorMessage(backend->conn));
    backend->socket = standby_reload_conf_socket;
    backend->events = WL_SOCKET_WRITEABLE;
}

static void standby_set_synchronous_standby_names_socket(Backend *backend) {
    if (!PQconsumeInput(backend->conn)) { W("%s:%s !PQconsumeInput and %s", PQhost(backend->conn), PQport(backend->conn), PQerrorMessage(backend->conn)); return; }
    if (PQisBusy(backend->conn)) { backend->events = WL_SOCKET_READABLE; return; }
    for (PGresult *result; (result = PQgetResult(backend->conn)); PQclear(result)) switch (PQresultStatus(result)) {
        case PGRES_COMMAND_OK: break;
        default: E("%s:%s PQresultStatus = %s and %s", PQhost(backend->conn), PQport(backend->conn), PQresStatus(PQresultStatus(result)), PQresultErrorMessage(result)); break;
    }
    standby_reload_conf(backend);
}

static void standby_set_synchronous_standby_names(Backend *backend) {
    char *cluster_name_;
    const char *cluster_name_quote;
    StringInfoData buf;
    if (backend->state != PRIMARY) return;
    if (PQisBusy(backend->conn)) { backend->events = WL_SOCKET_READABLE; return; }
    cluster_name_ = cluster_name ? cluster_name : "walreceiver";
    cluster_name_quote = quote_identifier(cluster_name_);
    initStringInfo(&buf);
    appendStringInfo(&buf, "ALTER SYSTEM SET synchronous_standby_names TO 'FIRST 1 (%s)'", cluster_name_quote);
    if (!PQsendQuery(backend->conn, buf.data)) E("%s:%s !PQsendQuery and %s", PQhost(backend->conn), PQport(backend->conn), PQerrorMessage(backend->conn));
    backend->socket = standby_set_synchronous_standby_names_socket;
    backend->events = WL_SOCKET_WRITEABLE;
    pfree(buf.data);
    if (cluster_name_quote != cluster_name_) pfree((void *)cluster_name_quote);
}

static void standby_standby_connect(PGresult *result) {
    for (int row = 0; row < PQntuples(result); row++) {
        Backend *backend;
        const char *addr = PQgetvalue(result, row, PQfnumber(result, "addr"));
        const char *host = PQgetvalue(result, row, PQfnumber(result, "host"));
        const char *cstate = PQgetvalue(result, row, PQfnumber(result, "state"));
        const char *cme = PQgetvalue(result, row, PQfnumber(result, "me"));
        bool me = cme[0] == 't' || cme[0] == 'T';
        STATE state;
        /*if (!me) */D1("addr = %s, host = %s, state = %s", addr, host, cstate);
        if (pg_strcasecmp(cstate, "async")) state = ASYNC;
        else if (pg_strcasecmp(cstate, "potential")) state = POTENTIAL;
        else if (pg_strcasecmp(cstate, "sync")) state = SYNC;
        else if (pg_strcasecmp(cstate, "quorum")) state = QUORUM;
        else E("unknown state = %s", cstate);
        if (me) { my_state = state; continue; }
        backend = palloc0(sizeof(*backend));
        backend->state = state;
        backend_connect(backend, host, 5432, MyProcPort->user_name, MyProcPort->database_name, NULL);
    }
}

static void standby_primary_socket(Backend *backend) {
    if (!PQconsumeInput(backend->conn)) { W("%s:%s !PQconsumeInput and %s", PQhost(backend->conn), PQport(backend->conn), PQerrorMessage(backend->conn)); return; }
    if (PQisBusy(backend->conn)) { backend->events = WL_SOCKET_READABLE; return; }
    for (PGresult *result; (result = PQgetResult(backend->conn)); PQclear(result)) switch (PQresultStatus(result)) {
        case PGRES_TUPLES_OK: standby_standby_connect(result); break;
        default: E("%s:%s PQresultStatus = %s and %s", PQhost(backend->conn), PQport(backend->conn), PQresStatus(PQresultStatus(result)), PQresultErrorMessage(result)); break;
    }
    backend_idle(backend);
}

static void standby_primary_connect(const char *host, int port, const char *user, const char *dbname) {
    Backend *backend = palloc0(sizeof(*backend));
    D1("host = %s, port = %i, user = %s, dbname = %s", host, port, user, dbname);
    backend->state = PRIMARY;
    backend_connect(backend, host, port, user, dbname, standby_set_synchronous_standby_names);
}

static void standby_primary(Backend *primary) {
    int nParams = queue_size(&backend_queue);
    Oid *paramTypes = nParams ? palloc(nParams * sizeof(*paramTypes)) : NULL;
    char **paramValues = nParams ? palloc(nParams * sizeof(**paramValues)) : NULL;
    StringInfoData buf;
    initStringInfo(&buf);
    appendStringInfoString(&buf, "SELECT client_addr AS addr, coalesce(client_hostname, client_addr::text) AS host, sync_state AS state, client_addr IS NOT DISTINCT FROM (SELECT client_addr FROM pg_stat_activity WHERE pid = pg_backend_pid()) AS me FROM pg_stat_replication");
    nParams = 0;
    queue_each(&backend_queue, queue) {
        Backend *backend = queue_data(queue, Backend, queue);
        if (backend->state == PRIMARY) continue;
        if (nParams) appendStringInfoString(&buf, ", ");
        else appendStringInfoString(&buf, " WHERE client_addr NOT IN (");
        paramTypes[nParams] = INETOID;
        paramValues[nParams] = PQhostaddr(backend->conn);
        nParams++;
        appendStringInfo(&buf, "$%i", nParams);
    }
    if (nParams) appendStringInfoString(&buf, ")");
    if (!PQsendQueryParams(primary->conn, buf.data, nParams, paramTypes, (const char * const*)paramValues, NULL, NULL, false)) E("%s:%s !PQsendQueryParams and %s", PQhost(primary->conn), PQport(primary->conn), PQerrorMessage(primary->conn));
    primary->socket = standby_primary_socket;
    primary->events = WL_SOCKET_WRITEABLE;
    if (paramTypes) pfree(paramTypes);
    if (paramValues) pfree(paramValues);
    pfree(buf.data);
}

void standby_init(void) {
    MemoryContext oldMemoryContext;
    SPI_connect_my("SELECT * FROM pg_stat_wal_receiver");
    SPI_execute_with_args_my("SELECT * FROM pg_stat_wal_receiver", 0, NULL, NULL, NULL, SPI_OK_SELECT, true);
    if (SPI_processed != 1) E("SPI_processed != 1");
    oldMemoryContext = MemoryContextSwitchTo(TopMemoryContext);
    sender_host = TextDatumGetCStringMy(SPI_getbinval_my(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, "sender_host", false));
    slot_name = TextDatumGetCStringMy(SPI_getbinval_my(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, "slot_name", false));
    MemoryContextSwitchTo(oldMemoryContext);
    sender_port = DatumGetInt32(SPI_getbinval_my(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, "sender_port", false));
    SPI_finish_my();
    D1("sender_host = %s, sender_port = %i, slot_name = %s", sender_host, sender_port, slot_name);
}

void standby_timeout(void) {
    if (!save_etcd_kv_put(hostname, timestamptz_to_str(start), 0)) {
        W("!save_etcd_kv_put");
        init_kill();
    }
    queue_each(&backend_queue, queue) {
        Backend *backend = queue_data(queue, Backend, queue);
        if (PQstatus(backend->conn) == CONNECTION_BAD) { backend_reset(backend, standby_reset); continue; }
    }
    if (!primary) standby_primary_connect(sender_host, sender_port, MyProcPort->user_name, MyProcPort->database_name);
    else if (PQstatus(primary->conn) != CONNECTION_OK) ;
    else if (PQisBusy(primary->conn)) primary->events = WL_SOCKET_READABLE;
    else standby_primary(primary);
}

void standby_fini(void) {
    backend_fini();
}

// pg_rewind --target-pgdata /home/pg_data --source-server application_name=pgautofailover_standby_12 host=postgres-docker-bill-02 port=5432 user=pgautofailover_replicator dbname=postgres sslmode=prefer --progress
