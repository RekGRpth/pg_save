#include "include.h"

extern Backend *primary;
extern char *hostname;
extern char *default_state;
extern queue_t backend_queue;
extern TimestampTz start;
static char *sender_host;
static char *slot_name;
static int sender_port;

static void standby_reprimary(Backend *backend) {
    D1("hi");
    //WriteRecoveryConfig(pgconn, target_dir, GenerateRecoveryConfig(pgconn, replication_slot));
    backend_finish(backend);
}

static void standby_promote(Backend *backend) {
    D1("hi");
//    if (!DatumGetBool(DirectFunctionCall2(pg_promote, BoolGetDatum(true), Int32GetDatum(30)))) E("!pg_promote");
//    else standby_fini();
    backend_finish(backend);
}

static void standby_reset(Backend *backend) {
    if (backend->state) { backend_finish(backend); return; }
    D1("state = %s", default_state);
    if (pg_strcasecmp(default_state, "sync")) standby_reprimary(backend);
    else if (queue_size(&backend_queue) > 1) standby_promote(backend);
    else init_kill();
}

static void standby_standby_connect(PGresult *result) {
    for (int row = 0; row < PQntuples(result); row++) {
        Backend *backend = NULL;
        const char *name = PQgetvalue(result, row, PQfnumber(result, "name"));
        const char *host = PQgetvalue(result, row, PQfnumber(result, "host"));
        const char *state = PQgetvalue(result, row, PQfnumber(result, "state"));
        const char *cme = PQgetvalue(result, row, PQfnumber(result, "me"));
        bool me = cme[0] == 't' || cme[0] == 'T';
        if (!me) D1("name = %s, host = %s, state = %s", name, host, state);
        if (me) { backend_alter_system_set("pg_save.state", default_state, state); continue; }
        queue_each(&backend_queue, queue) {
            Backend *backend_ = queue_data(queue, Backend, queue);
            if (!pg_strcasecmp(host, PQhost(backend_->conn))) { backend = backend_; break; }
        }
        if (backend) {
            pfree(backend->state);
            backend->state = pstrdup(state);
        } else {
            backend = palloc0(sizeof(*backend));
            backend->name = pstrdup(name);
            backend->state = pstrdup(state);
            backend_connect(backend, host, 5432, MyProcPort->user_name, MyProcPort->database_name, backend_idle);
        }
    }
}

static void standby_primary_socket(Backend *backend) {
    for (PGresult *result; (result = PQgetResult(backend->conn)); PQclear(result)) switch (PQresultStatus(result)) {
        case PGRES_TUPLES_OK: standby_standby_connect(result); break;
        default: E("%s:%s/%s PQresultStatus = %s and %s", PQhost(backend->conn), PQport(backend->conn), backend->state ? backend->state : "primary", PQresStatus(PQresultStatus(result)), PQresultErrorMessage(result)); break;
    }
    backend_idle(backend);
}

static void standby_primary_connect(const char *host, int port, const char *user, const char *dbname) {
    Backend *backend = palloc0(sizeof(*backend));
    D1("host = %s, port = %i, user = %s, dbname = %s", host, port, user, dbname);
    backend_connect(backend, host, port, user, dbname, backend_idle);
}

static void standby_primary(Backend *primary) {
    int nParams = queue_size(&backend_queue);
    Oid *paramTypes = nParams ? palloc(2 * nParams * sizeof(*paramTypes)) : NULL;
    char **paramValues = nParams ? palloc(2 * nParams * sizeof(**paramValues)) : NULL;
    StringInfoData buf;
    initStringInfo(&buf);
    appendStringInfoString(&buf, "SELECT application_name AS name, coalesce(client_hostname, client_addr::text) AS host, sync_state AS state, client_addr IS NOT DISTINCT FROM (SELECT client_addr FROM pg_stat_activity WHERE pid = pg_backend_pid()) AS me FROM pg_stat_replication");
    nParams = 0;
    queue_each(&backend_queue, queue) {
        Backend *backend = queue_data(queue, Backend, queue);
        if (!backend->state) continue;
        if (nParams) appendStringInfoString(&buf, ", ");
        else appendStringInfoString(&buf, " WHERE (client_addr, sync_state) NOT IN (");
        paramTypes[nParams] = INETOID;
        paramValues[nParams] = PQhostaddr(backend->conn);
        nParams++;
        appendStringInfo(&buf, "($%i", nParams);
        paramTypes[nParams] = TEXTOID;
        paramValues[nParams] = backend->state;
        nParams++;
        appendStringInfo(&buf, ", $%i)", nParams);
    }
    if (nParams) appendStringInfoString(&buf, ")");
    if (!PQsendQueryParams(primary->conn, buf.data, nParams, paramTypes, (const char * const*)paramValues, NULL, NULL, false)) E("%s:%s/%s !PQsendQueryParams and %s", PQhost(primary->conn), PQport(primary->conn), primary->state ? primary->state : "primary", PQerrorMessage(primary->conn));
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
    if (!save_etcd_kv_put(default_state, hostname, 0)) {
        W("!save_etcd_kv_put");
        init_kill();
    }
    if (!save_etcd_kv_put(hostname, timestamptz_to_str(start), 0)) {
        W("!save_etcd_kv_put");
        init_kill();
    }
    queue_each(&backend_queue, queue) {
        Backend *backend = queue_data(queue, Backend, queue);
        if (PQstatus(backend->conn) == CONNECTION_BAD) backend_reset(backend, backend_idle, standby_reset);
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
