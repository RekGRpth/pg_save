#include "include.h"

Backend *primary = NULL;
extern char *hostname;
extern char *init_state;
extern int init_probe;
extern queue_t backend_queue;
extern TimestampTz start;

static void standby_reprimary(Backend *backend) {
    D1("state = %s", init_state);
    queue_each(&backend_queue, queue) {
        Backend *backend = queue_data(queue, Backend, queue);
        if (strcmp(backend->state, "sync")) continue;
        WriteRecoveryConfig(backend->conn, getenv("PGDATA"), GenerateRecoveryConfig(backend->conn, PrimarySlotName));
    }
    backend_finish(backend);
}

static void standby_promote(Backend *backend) {
    D1("state = %s", init_state);
    if (!DatumGetBool(DirectFunctionCall2(pg_promote, BoolGetDatum(true), Int32GetDatum(30)))) E("!pg_promote");
    backend_finish(backend);
}

static void standby_connect(Backend *backend) {
    backend->probe = 0;
    init_set_state(backend_state(backend), PQhost(backend->conn));
    backend_idle(backend);
}

static void standby_reset(Backend *backend) {
    if (backend->probe++ < init_probe) return;
    if (backend->state) backend_finish(backend);
    else if (queue_size(&backend_queue) <= 1) init_kill();
    else if (strcmp(init_state, "sync")) standby_reprimary(backend);
    else standby_promote(backend);
}

static void standby_finish(Backend *backend) {
    if (!backend->state) primary = NULL;
}

static void standby_state(const char *state) {
    init_alter_system_set("pg_save.state", init_state, state);
    init_set_state(state, hostname);
}

static void standby_result(PGresult *result) {
    for (int row = 0; row < PQntuples(result); row++) {
        Backend *backend = NULL;
        const char *name = PQgetvalue(result, row, PQfnumber(result, "name"));
        const char *host = PQgetvalue(result, row, PQfnumber(result, "host"));
        const char *state = PQgetvalue(result, row, PQfnumber(result, "state"));
        const char *cme = PQgetvalue(result, row, PQfnumber(result, "me"));
        bool me = cme[0] == 't' || cme[0] == 'T';
        if (me) { standby_state(state); continue; }
        D1("name = %s, host = %s, state = %s", name, host, state);
        queue_each(&backend_queue, queue) {
            Backend *backend_ = queue_data(queue, Backend, queue);
            if (!strcmp(host, PQhost(backend_->conn))) { backend = backend_; break; }
        }
        if (backend) {
            init_reset(backend);
            if (backend->name) pfree(backend->name);
            pfree(backend->state);
            backend->name = pstrdup(name);
            backend->state = pstrdup(state);
            init_set_state(backend_state(backend), PQhost(backend->conn));
        } else {
            backend = palloc0(sizeof(*backend));
            backend->name = pstrdup(name);
            backend->state = pstrdup(state);
            backend->connect = standby_connect;
            backend->reset = standby_reset;
            backend->finish = standby_finish;
            backend_connect(backend, host, "5432", MyProcPort->user_name, MyProcPort->database_name);
        }
    }
}

static void standby_primary_socket(Backend *backend) {
    for (PGresult *result; (result = PQgetResult(backend->conn)); PQclear(result)) switch (PQresultStatus(result)) {
        case PGRES_TUPLES_OK: standby_result(result); break;
        default: E("%s:%s/%s PQresultStatus = %s and %s", PQhost(backend->conn), PQport(backend->conn), backend_state(backend), PQresStatus(PQresultStatus(result)), PQresultErrorMessage(result)); break;
    }
    backend_idle(backend);
}

static void standby_primary(Backend *backend) {
    int nParams = queue_size(&backend_queue);
    Oid *paramTypes = nParams ? palloc(2 * nParams * sizeof(*paramTypes)) : NULL;
    char **paramValues = nParams ? palloc(2 * nParams * sizeof(*paramValues)) : NULL;
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
    if (!PQsendQueryParams(backend->conn, buf.data, nParams, paramTypes, (const char * const*)paramValues, NULL, NULL, false)) E("%s:%s/%s !PQsendQueryParams and %s", PQhost(backend->conn), PQport(backend->conn), backend_state(backend), PQerrorMessage(backend->conn));
    backend->socket = standby_primary_socket;
    backend->events = WL_SOCKET_WRITEABLE;
    if (paramTypes) pfree(paramTypes);
    if (paramValues) pfree(paramValues);
    pfree(buf.data);
}

static void standby_primary_connect(void) {
    const char *primary_host = NULL;
    const char *primary_port = NULL;
    char *err;
    PQconninfoOption *opts;
    if (!(opts = PQconninfoParse(PrimaryConnInfo, &err))) E("!PQconninfoParse and %s", err);
    for (PQconninfoOption *opt = opts; opt->keyword; opt++) {
        if (!opt->val) continue;
        D1("%s = %s", opt->keyword, opt->val);
        if (!strcmp(opt->keyword, "host")) { primary_host = opt->val; continue; }
        if (!strcmp(opt->keyword, "port")) { primary_port = opt->val; continue; }
    }
    if (err) PQfreemem(err);
    if (primary_port && primary_host) {
        D1("primary_host = %s, primary_port = %s", primary_host, primary_port);
        primary = palloc0(sizeof(*primary));
        primary->connect = standby_connect;
        primary->reset = standby_reset;
        primary->finish = standby_finish;
        backend_connect(primary, primary_host, primary_port, MyProcPort->user_name, MyProcPort->database_name);
    }
    PQconninfoFree(opts);
}

void standby_init(void) {
    init_alter_system_reset("synchronous_standby_names");
    init_connect(standby_connect, standby_reset, standby_finish);
}

void standby_timeout(void) {
    if (!save_etcd_kv_put(init_state, hostname, 0)) {
        W("!save_etcd_kv_put");
        init_kill();
    }
    if (!save_etcd_kv_put(hostname, timestamptz_to_str(start), 0)) {
        W("!save_etcd_kv_put");
        init_kill();
    }
    queue_each(&backend_queue, queue) {
        Backend *backend = queue_data(queue, Backend, queue);
        if (!primary && !backend->state) primary = backend;
        if (PQstatus(backend->conn) == CONNECTION_BAD) backend_reset(backend);
    }
    if (!primary) standby_primary_connect();
    else if (PQstatus(primary->conn) != CONNECTION_OK) ;
    else if (PQisBusy(primary->conn)) primary->events = WL_SOCKET_READABLE;
    else standby_primary(primary);
}

void standby_fini(void) {
    backend_fini();
}
