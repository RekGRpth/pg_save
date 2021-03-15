#include "include.h"

extern char *hostname;
extern int init_attempt;
extern queue_t backend_queue;
extern STATE init_state;

void standby_connected(Backend *backend) {
    backend->attempt = 0;
    init_set_remote_state(backend->state, PQhost(backend->conn));
    backend_idle(backend);
}

static void standby_promote(Backend *backend) {
    D1("state = %s", init_state2char(init_state));
    if (!DatumGetBool(DirectFunctionCall2(pg_promote, BoolGetDatum(true), Int32GetDatum(30)))) W("!pg_promote");
    backend_finish(backend);
}

static void standby_reprimary(Backend *backend) {
    D1("state = %s", init_state2char(init_state));
    queue_each(&backend_queue, queue) {
        Backend *backend = queue_data(queue, Backend, queue);
        StringInfoData buf;
        if (backend->state != SYNC) continue;
        initStringInfo(&buf);
        appendStringInfo(&buf, "host=%s application_name=%s", PQhost(backend->conn), hostname);
        init_alter_system_set("primary_conninfo", buf.data);
        pfree(buf.data);
    }
    backend_finish(backend);
}

void standby_failed(Backend *backend) {
    if (backend->state != PRIMARY) backend_finish(backend);
    else if (!queue_size(&backend_queue)) { if (kill(PostmasterPid, SIGTERM)) W("kill and %m"); }
    else if (init_state != SYNC) standby_reprimary(backend);
    else standby_promote(backend);
}

void standby_finished(Backend *backend) {
}

void standby_fini(void) {
    backend_fini();
}

void standby_init(void) {
    init_alter_system_reset("synchronous_standby_names");
    if (init_state == PRIMARY) init_reset_local_state(init_state);
    if (init_state != UNKNOWN) init_set_remote_state(init_state, hostname);
}

static void standby_state(STATE state) {
    init_reset_remote_state(init_state);
    init_set_local_state(state);
    init_set_remote_state(state, hostname);
    init_reload();
}

static void standby_result(PGresult *result) {
    for (int row = 0; row < PQntuples(result); row++) {
        Backend *backend = NULL;
        const char *host = PQgetvalue(result, row, PQfnumber(result, "host"));
        const char *state = PQgetvalue(result, row, PQfnumber(result, "state"));
        const char *cme = PQgetvalue(result, row, PQfnumber(result, "me"));
        bool me = cme[0] == 't' || cme[0] == 'T';
        if (me) { standby_state(init_char2state(state)); continue; }
        D1("host = %s, state = %s", host, state);
        queue_each(&backend_queue, queue) {
            Backend *backend_ = queue_data(queue, Backend, queue);
            if (!strcmp(host, PQhost(backend_->conn))) { backend = backend_; break; }
        }
        backend ? backend_update(backend, init_char2state(state)) : backend_connect(host, init_char2state(state));
    }
}

static void standby_primary_socket(Backend *backend) {
    for (PGresult *result; (result = PQgetResult(backend->conn)); PQclear(result)) switch (PQresultStatus(result)) {
        case PGRES_TUPLES_OK: standby_result(result); break;
        default: W("%s:%s PQresultStatus = %s and %.*s", PQhost(backend->conn), init_state2char(backend->state), PQresStatus(PQresultStatus(result)), (int)strlen(PQresultErrorMessage(result)), PQresultErrorMessage(result)); break;
    }
    backend_idle(backend);
}

static void standby_primary(Backend *backend) {
    int nParams = 2 * queue_size(&backend_queue) + (init_state != UNKNOWN ? 1 : 0);
    Oid *paramTypes = nParams ? MemoryContextAlloc(TopMemoryContext, nParams * sizeof(*paramTypes)) : NULL;
    char **paramValues = nParams ? MemoryContextAlloc(TopMemoryContext, nParams * sizeof(*paramValues)) : NULL;
    StringInfoData buf;
    initStringInfo(&buf);
    appendStringInfoString(&buf, "SELECT coalesce(client_hostname, client_addr::text) AS host, sync_state AS state, client_addr IS NOT DISTINCT FROM (SELECT client_addr FROM pg_stat_activity WHERE pid = pg_backend_pid()) AS me FROM pg_stat_get_wal_senders() AS w INNER JOIN pg_stat_get_activity(pid) AS a USING (pid) WHERE w.state = 'streaming'");
    nParams = 0;
    queue_each(&backend_queue, queue) {
        Backend *backend = queue_data(queue, Backend, queue);
        if (backend->state == PRIMARY) continue;
        appendStringInfoString(&buf, nParams ? ", " : " AND (client_addr, sync_state) NOT IN (");
        paramTypes[nParams] = INETOID;
        paramValues[nParams] = (char *)PQhostaddr(backend->conn);
        nParams++;
        appendStringInfo(&buf, "($%i", nParams);
        paramTypes[nParams] = TEXTOID;
        paramValues[nParams] = (char *)init_state2char(backend->state);
        nParams++;
        appendStringInfo(&buf, ", $%i)", nParams);
    }
    if (nParams) appendStringInfoString(&buf, ")");
    if (init_state != UNKNOWN) {
        appendStringInfoString(&buf, " AND (client_addr, sync_state) IS DISTINCT FROM ((SELECT client_addr FROM pg_stat_activity WHERE pid = pg_backend_pid())");
        paramTypes[nParams] = TEXTOID;
        paramValues[nParams] = (char *)init_state2char(init_state);
        nParams++;
        appendStringInfo(&buf, ", $%i)", nParams);
    }
    if (!PQsendQueryParams(backend->conn, buf.data, nParams, paramTypes, (const char * const*)paramValues, NULL, NULL, false)) {
        W("%s:%s !PQsendQueryParams and %.*s", PQhost(backend->conn), init_state2char(backend->state), (int)strlen(PQerrorMessage(backend->conn)) - 1, PQerrorMessage(backend->conn));
        backend_finish(backend);
    } else {
        backend->socket = standby_primary_socket;
        backend->events = WL_SOCKET_WRITEABLE;
    }
    if (paramTypes) pfree(paramTypes);
    if (paramValues) pfree(paramValues);
    pfree(buf.data);
}

static void standby_primary_connect(void) {
    const char *host = NULL;
    char *err;
    PQconninfoOption *opts;
    if (!(opts = PQconninfoParse(PrimaryConnInfo, &err))) E("!PQconninfoParse and %s", err);
    for (PQconninfoOption *opt = opts; opt->keyword; opt++) {
        if (!opt->val) continue;
        D1("%s = %s", opt->keyword, opt->val);
        if (!strcmp(opt->keyword, "host")) { host = opt->val; continue; }
    }
    if (err) PQfreemem(err);
    if (host) {
        D1("host = %s", host);
        backend_connect(host, PRIMARY);
    }
    PQconninfoFree(opts);
}

void standby_timeout(void) {
    Backend *primary = NULL;
    queue_each(&backend_queue, queue) {
        Backend *backend = queue_data(queue, Backend, queue);
        if (backend->state == PRIMARY) primary = backend;
        if (PQstatus(backend->conn) == CONNECTION_BAD) backend_reset(backend);
    }
    if (!primary) standby_primary_connect();
    else if (PQstatus(primary->conn) != CONNECTION_OK) ;
    else if (PQisBusy(primary->conn)) primary->events = WL_SOCKET_READABLE;
    else standby_primary(primary);
}

void standby_updated(Backend *backend) {
}
