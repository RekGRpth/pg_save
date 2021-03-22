#include "include.h"

extern char *backend_save;
extern char *init_sync;
extern char *save_schema_type;
extern int init_attempt;
extern queue_t save_queue;
extern STATE init_state;
static Backend *standby_primary = NULL;

static void standby_update(STATE state) {
    init_set_host_state(NULL, init_state);
    init_set_state(state);
    init_set_host_state(MyBgworkerEntry->bgw_type, state);
    init_reload();
    backend_array();
}

static void standby_result(PGresult *result) {
    for (int row = 0; row < PQntuples(result); row++) {
        const char *host = PQgetvalue(result, row, PQfnumber(result, "application_name"));
        const char *state = PQgetvalue(result, row, PQfnumber(result, "sync_state"));
        const char *me = PQgetvalue(result, row, PQfnumber(result, "me"));
        (me[0] == 't' || me[0] == 'T') ? standby_update(init_char2state(state)) : backend_result(host, init_char2state(state));
    }
}

static void standby_query_socket(Backend *backend) {
    bool ok = false;
    for (PGresult *result; (result = PQgetResult(backend->conn)); PQclear(result)) switch (PQresultStatus(result)) {
        case PGRES_TUPLES_OK: ok = true; standby_result(result); break;
        default: W("%s:%s PQresultStatus = %s and %.*s", PQhost(backend->conn), init_state2char(backend->state), PQresStatus(PQresultStatus(result)), (int)strlen(PQresultErrorMessage(result)) - 1, PQresultErrorMessage(result)); break;
    }
    ok ? backend_idle(backend) : backend_finish(backend);
}

static void standby_query(Backend *backend) {
    const char *paramValues[] = {backend_save};
    if (PQisBusy(backend->conn)) backend->events = WL_SOCKET_READABLE; else if (!PQsendQueryPrepared(backend->conn, "standby_prepare", countof(paramValues), paramValues, NULL, NULL, false)) {
        W("%s:%s !PQsendQueryPrepared and %.*s", PQhost(backend->conn), init_state2char(backend->state), (int)strlen(PQerrorMessage(backend->conn)) - 1, PQerrorMessage(backend->conn));
        backend_finish(backend);
    } else {
        backend->events = WL_SOCKET_WRITEABLE;
        backend->socket = standby_query_socket;
    }
}

static void standby_prepare_socket(Backend *backend) {
    bool ok = false;
    for (PGresult *result; (result = PQgetResult(backend->conn)); PQclear(result)) switch (PQresultStatus(result)) {
        case PGRES_COMMAND_OK: ok = true; break;
        default: W("%s:%s PQresultStatus = %s and %.*s", PQhost(backend->conn), init_state2char(backend->state), PQresStatus(PQresultStatus(result)), (int)strlen(PQresultErrorMessage(result)) - 1, PQresultErrorMessage(result)); break;
    }
    ok ? standby_query(backend) : backend_finish(backend);
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
            "WHERE state = 'streaming' AND s.sync_state IS DISTINCT FROM v.sync_state", save_schema_type);
        command = buf.data;
    }
    if (PQisBusy(backend->conn)) backend->events = WL_SOCKET_READABLE; else if (!PQsendPrepare(backend->conn, "standby_prepare", command, countof(paramTypes), paramTypes)) {
        W("%s:%s !PQsendPrepare and %.*s", PQhost(backend->conn), init_state2char(backend->state), (int)strlen(PQerrorMessage(backend->conn)) - 1, PQerrorMessage(backend->conn));
        backend_finish(backend);
    } else {
        backend->events = WL_SOCKET_WRITEABLE;
        backend->socket = standby_prepare_socket;
    }
}

void standby_connected(Backend *backend) {
    backend->state == PRIMARY ? standby_prepare(backend) : backend_idle(backend);
}

void standby_created(Backend *backend) {
    if (backend->state == PRIMARY) standby_primary = backend;
}

static void standby_create(const char *conninfo) {
    char *err;
    PQconninfoOption *opts;
    if (!(opts = PQconninfoParse(conninfo, &err))) E("!PQconninfoParse and %s", err);
    for (PQconninfoOption *opt = opts; opt->keyword; opt++) {
        if (!opt->val) continue;
        D1("%s = %s", opt->keyword, opt->val);
        if (!strcmp(opt->keyword, "host")) backend_create(opt->val, PRIMARY);
    }
    if (err) PQfreemem(err);
    PQconninfoFree(opts);
}

static void standby_promote(void) {
    D1("state = %s", init_state2char(init_state));
    if (DatumGetBool(DirectFunctionCall2(pg_promote, BoolGetDatum(true), Int32GetDatum(30)))) primary_init();
    else W("!pg_promote");
}

static void standby_reprimary(void) {
    StringInfoData buf;
    Backend *backend = backend_host(init_sync);
    D1("state = %s, found = %s", init_state2char(init_state), backend ? "true" : "false");
    if (!backend) E("!backend_host");
    initStringInfoMy(TopMemoryContext, &buf);
    appendStringInfo(&buf, "host=%s application_name=%s", PQhost(backend->conn), MyBgworkerEntry->bgw_type);
    init_alter_system_set("primary_conninfo", buf.data);
    standby_create(buf.data);
    pfree(buf.data);
}

void standby_failed(Backend *backend) {
    backend_finish(backend);
    if (backend->state != PRIMARY);
    else if (!queue_size(&save_queue)) { if (kill(PostmasterPid, SIGTERM)) W("kill and %m"); }
    else if (init_state != SYNC) standby_reprimary();
    else standby_promote();
}

void standby_finished(Backend *backend) {
    if (backend->state == PRIMARY) standby_primary = NULL;
}

void standby_fini(void) {
}

void standby_init(void) {
    init_alter_system_set("synchronous_standby_names", NULL);
    init_set_state(UNKNOWN);
    standby_create(PrimaryConnInfo);
}

void standby_timeout(void) {
    if (!standby_primary) standby_create(PrimaryConnInfo);
    else if (PQstatus(standby_primary->conn) == CONNECTION_OK) standby_query(standby_primary);
}

void standby_updated(Backend *backend) {
}
