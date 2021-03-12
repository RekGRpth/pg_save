#include "include.h"

extern char *hostname;
extern char *init_state;
extern int init_attempt;
extern queue_t backend_queue;
static Backend *primary = NULL;

void standby_connected(Backend *backend) {
    backend->attempt = 0;
    init_set_state(backend->host, backend->state);
    backend_idle(backend);
}

void standby_finished(Backend *backend) {
    if (!strcmp(backend->state, "primary")) primary = NULL;
}

void standby_fini(void) {
    backend_fini();
}

void standby_init(void) {
    init_alter_system_reset("synchronous_standby_names", SyncRepStandbyNames);
    if (init_state && !strcmp(init_state, "primary")) init_alter_system_reset("pg_save.state", init_state);
    else if (init_state) init_set_state(hostname, init_state);
}

static void standby_promote(Backend *backend) {
    D1("state = %s", init_state);
    if (!DatumGetBool(DirectFunctionCall2(pg_promote, BoolGetDatum(true), Int32GetDatum(30)))) W("!pg_promote");
    backend_finish(backend);
}

static void standby_reprimary(Backend *backend) {
    D1("state = %s", init_state);
    queue_each(&backend_queue, queue) {
        Backend *backend = queue_data(queue, Backend, queue);
        StringInfoData buf;
        if (strcmp(backend->state, "sync")) continue;
        initStringInfo(&buf);
        appendStringInfo(&buf, "host=%s application_name=%s", backend->host, hostname);
        init_alter_system_set("primary_conninfo", PrimaryConnInfo, buf.data);
        pfree(buf.data);
    }
    backend_finish(backend);
}

void standby_reseted(Backend *backend) {
    if (strcmp(backend->state, "primary")) backend_finish(backend);
    else if (!queue_size(&backend_queue)) init_kill();
    else if (strcmp(init_state, "sync")) standby_reprimary(backend);
    else standby_promote(backend);
}

static void standby_state(const char *state) {
    init_reset_state(hostname);
    init_alter_system_set("pg_save.state", init_state, state);
    init_set_state(hostname, state);
}

static void standby_result(PGresult *result) {
    for (int row = 0; row < PQntuples(result); row++) {
        Backend *backend = NULL;
        const char *host = PQgetvalue(result, row, PQfnumber(result, "host"));
        const char *state = PQgetvalue(result, row, PQfnumber(result, "state"));
        const char *cme = PQgetvalue(result, row, PQfnumber(result, "me"));
        bool me = cme[0] == 't' || cme[0] == 'T';
        if (me) { standby_state(state); continue; }
        D1("host = %s, state = %s", host, state);
        queue_each(&backend_queue, queue) {
            Backend *backend_ = queue_data(queue, Backend, queue);
            if (!strcmp(host, backend_->host)) { backend = backend_; break; }
        }
        backend ? backend_update(backend, state) : backend_connect(host, getenv("PGPORT") ? getenv("PGPORT") : DEF_PGPORT_STR, MyProcPort->user_name, MyProcPort->database_name, state);
    }
}

static void standby_primary_socket(Backend *backend) {
    for (PGresult *result; (result = PQgetResult(backend->conn)); PQclear(result)) switch (PQresultStatus(result)) {
        case PGRES_TUPLES_OK: standby_result(result); break;
        default: W("%s:%s/%s PQresultStatus = %s and %s", backend->host, backend->port, backend->state, PQresStatus(PQresultStatus(result)), PQresultErrorMessage(result)); break;
    }
    backend_idle(backend);
}

static void standby_primary(Backend *backend) {
    int nParams = 2 * queue_size(&backend_queue) + (init_state ? 1 : 0);
    Oid *paramTypes = nParams ? MemoryContextAlloc(TopMemoryContext, nParams * sizeof(*paramTypes)) : NULL;
    char **paramValues = nParams ? MemoryContextAlloc(TopMemoryContext, nParams * sizeof(*paramValues)) : NULL;
    StringInfoData buf;
    initStringInfo(&buf);
    appendStringInfoString(&buf, "SELECT coalesce(client_hostname, client_addr::text) AS host, sync_state AS state, client_addr IS NOT DISTINCT FROM (SELECT client_addr FROM pg_stat_activity WHERE pid = pg_backend_pid()) AS me FROM pg_stat_get_wal_senders() AS w INNER JOIN pg_stat_get_activity(pid) AS a USING (pid) WHERE w.state = 'streaming'");
    nParams = 0;
    queue_each(&backend_queue, queue) {
        Backend *backend = queue_data(queue, Backend, queue);
        if (!strcmp(backend->state, "primary")) continue;
        appendStringInfoString(&buf, nParams ? ", " : " AND (client_addr, sync_state) NOT IN (");
        paramTypes[nParams] = INETOID;
        paramValues[nParams] = (char *)PQhostaddr(backend->conn);
        nParams++;
        appendStringInfo(&buf, "($%i", nParams);
        paramTypes[nParams] = TEXTOID;
        paramValues[nParams] = backend->state;
        nParams++;
        appendStringInfo(&buf, ", $%i)", nParams);
    }
    if (nParams) appendStringInfoString(&buf, ")");
    if (init_state) {
        appendStringInfoString(&buf, " AND (client_addr, sync_state) IS DISTINCT FROM ((SELECT client_addr FROM pg_stat_activity WHERE pid = pg_backend_pid())");
        paramTypes[nParams] = TEXTOID;
        paramValues[nParams] = init_state;
        nParams++;
        appendStringInfo(&buf, ", $%i)", nParams);
    }
    if (!PQsendQueryParams(backend->conn, buf.data, nParams, paramTypes, (const char * const*)paramValues, NULL, NULL, false)) {
        W("%s:%s/%s !PQsendQueryParams and %s", backend->host, backend->port, backend->state, PQerrorMessage(backend->conn));
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
    const char *data = MyProcPort->database_name;
    const char *host = NULL;
    const char *port = getenv("PGPORT") ? getenv("PGPORT") : DEF_PGPORT_STR;
    const char *user = MyProcPort->user_name;
    char *err;
    PQconninfoOption *opts;
    if (!(opts = PQconninfoParse(PrimaryConnInfo, &err))) E("!PQconninfoParse and %s", err);
    for (PQconninfoOption *opt = opts; opt->keyword; opt++) {
        if (!opt->val) continue;
        D1("%s = %s", opt->keyword, opt->val);
        if (!strcmp(opt->keyword, "dbname")) { data = opt->val; continue; }
        if (!strcmp(opt->keyword, "host")) { host = opt->val; continue; }
        if (!strcmp(opt->keyword, "port")) { port = opt->val; continue; }
        if (!strcmp(opt->keyword, "user")) { user = opt->val; continue; }
    }
    if (err) PQfreemem(err);
    if (host) {
        D1("host = %s, port = %s, user = %s, data = %s", host, port, user, data);
        backend_connect(host, port, user, data, "primary");
    }
    PQconninfoFree(opts);
}

void standby_timeout(void) {
    queue_each(&backend_queue, queue) {
        Backend *backend = queue_data(queue, Backend, queue);
        if (!primary && !strcmp(backend->state, "primary")) primary = backend;
        if (PQstatus(backend->conn) == CONNECTION_BAD) backend_reset(backend);
    }
    if (!primary) standby_primary_connect();
    else if (PQstatus(primary->conn) != CONNECTION_OK) ;
    else if (PQisBusy(primary->conn)) primary->events = WL_SOCKET_READABLE;
    else standby_primary(primary);
}

void standby_updated(Backend *backend) {
    init_set_state(backend->host, backend->state);
}
