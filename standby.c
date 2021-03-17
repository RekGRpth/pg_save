#include "include.h"

extern char *hostname;
extern char *save;
extern char *schema_type;
extern int init_attempt;
extern queue_t backend_queue;
extern STATE init_state;
static bool prepared = false;

void standby_connected(Backend *backend) {
    backend->attempt = 0;
    init_set_remote_state(backend->state, PQhost(backend->conn));
    backend_idle(backend);
    prepared = false;
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
        initStringInfoMy(TopMemoryContext, &buf);
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
        const char *host = PQgetvalue(result, row, PQfnumber(result, "application_name"));
        const char *state = PQgetvalue(result, row, PQfnumber(result, "sync_state"));
        const char *cme = PQgetvalue(result, row, PQfnumber(result, "me"));
        bool me = cme[0] == 't' || cme[0] == 'T';
        if (me) { standby_state(init_char2state(state)); continue; }
        backend_result(state, host);
    }
}

static void standby_query_socket(Backend *backend) {
    for (PGresult *result; (result = PQgetResult(backend->conn)); PQclear(result)) switch (PQresultStatus(result)) {
        case PGRES_TUPLES_OK: standby_result(result); break;
        default: W("%s:%s PQresultStatus = %s and %.*s", PQhost(backend->conn), init_state2char(backend->state), PQresStatus(PQresultStatus(result)), (int)strlen(PQresultErrorMessage(result)) - 1, PQresultErrorMessage(result)); break;
    }
    backend_idle(backend);
}

static void standby_query(Backend *backend) {
    const char *paramValues[] = {save};
    if (!PQsendQueryPrepared(backend->conn, "standby_prepare", countof(paramValues), paramValues, NULL, NULL, false)) {
        W("%s:%s !PQsendQueryPrepared and %.*s", PQhost(backend->conn), init_state2char(backend->state), (int)strlen(PQerrorMessage(backend->conn)) - 1, PQerrorMessage(backend->conn));
        backend_finish(backend);
    } else {
        prepared = true;
        backend->socket = standby_query_socket;
        backend->events = WL_SOCKET_WRITEABLE;
    }
}

static void standby_prepare_socket(Backend *backend) {
    for (PGresult *result; (result = PQgetResult(backend->conn)); PQclear(result)) switch (PQresultStatus(result)) {
        case PGRES_COMMAND_OK: break;
        default: W("%s:%s PQresultStatus = %s and %.*s", PQhost(backend->conn), init_state2char(backend->state), PQresStatus(PQresultStatus(result)), (int)strlen(PQresultErrorMessage(result)) - 1, PQresultErrorMessage(result)); break;
    }
    standby_query(backend);
}

static void standby_prepare(Backend *backend) {
    static Oid paramTypes[] = {TEXTOID};
    static char *command = NULL;
    if (!command) {
        StringInfoData buf;
        initStringInfoMy(TopMemoryContext, &buf);
        appendStringInfo(&buf,
            "SELECT application_name, s.sync_state, client_addr IS NOT DISTINCT FROM (SELECT client_addr FROM pg_stat_activity WHERE pid = pg_backend_pid()) AS me\n"
            "FROM pg_stat_replication AS s FULL OUTER JOIN unnest($1::%1$s[]) AS v USING (application_name)\n"
            "WHERE state = 'streaming' AND s.sync_state IS DISTINCT FROM v.sync_state", schema_type);
        command = buf.data;
    }
    if (!PQsendPrepare(backend->conn, "standby_prepare", command, countof(paramTypes), paramTypes)) {
        W("%s:%s !PQsendPrepare and %.*s", PQhost(backend->conn), init_state2char(backend->state), (int)strlen(PQerrorMessage(backend->conn)) - 1, PQerrorMessage(backend->conn));
        backend_finish(backend);
    } else {
        backend->socket = standby_prepare_socket;
        backend->events = WL_SOCKET_WRITEABLE;
    }
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
    else if (prepared) standby_query(primary);
    else standby_prepare(primary);
}

void standby_updated(Backend *backend) {
}
