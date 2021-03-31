#include "include.h"

extern char *backend_save;
extern int init_attempt;
extern queue_t save_queue;
extern STATE init_state;
static Backend *standby_primary = NULL;
static int standby_attempt = 0;

void standby_connected(Backend *backend) {
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

static void standby_promote(Backend *backend) {
    D1("state = %s", init_state2char(init_state));
    backend_finish(backend);
    if (!DatumGetBool(DirectFunctionCall2(pg_promote, BoolGetDatum(true), Int32GetDatum(30)))) W("!pg_promote"); else {
        primary_init();
        init_notify(MyBgworkerEntry->bgw_type, "reprimary");
    }
}

void standby_failed(Backend *backend) {
    if (backend->state != PRIMARY) { backend_finish(backend); return; }
    if (!queue_size(&save_queue)) { if (kill(PostmasterPid, SIGKILL)) W("kill(%i ,%i)", PostmasterPid, SIGKILL); return; }
    if (init_state == SYNC) standby_promote(backend);
}

void standby_finished(Backend *backend) {
    if (backend->state == PRIMARY) standby_primary = NULL;
}

void standby_fini(void) {
}

void standby_init(void) {
    init_set_system("synchronous_standby_names", NULL);
    standby_create(PrimaryConnInfo);
}

static void standby_reprimary(Backend *backend) {
    StringInfoData buf;
    initStringInfoMy(TopMemoryContext, &buf);
    appendStringInfo(&buf, "host=%s application_name=%s target_session_attrs=read-write", PQhost(backend->conn), MyBgworkerEntry->bgw_type);
    backend_finish(backend);
    init_set_system("primary_conninfo", buf.data);
    standby_create(buf.data);
    pfree(buf.data);
}

void standby_notify(Backend *backend, const char *channel, const char *payload, int32 srcPid) {
    if (backend->state == SYNC && !strcmp(payload, "reprimary")) standby_reprimary(backend);
}

static void standby_demote(Backend *backend) {
    backend_idle(backend);
    if (init_state != SYNC) return;
    if (queue_size(&save_queue) < 2) return;
    if (strcmp(MyBgworkerEntry->bgw_type, PQhost(backend->conn)) > 0) return;
    W("%i < %i", standby_attempt, init_attempt);
    if (standby_attempt++ < init_attempt) return;
    init_notify(MyBgworkerEntry->bgw_type, "demote");
}

static void standby_update(STATE state) {
    if (init_state == state) return;
    init_set_state(state);
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
    ok ? standby_demote(backend) : backend_finish(backend);
}

static void standby_query(Backend *backend) {
    static Oid paramTypes[] = {TEXTOID};
    const char *paramValues[] = {backend_save};
    static char *command =
        "SELECT application_name, s.sync_state, client_addr IS NOT DISTINCT FROM (SELECT client_addr FROM pg_stat_activity WHERE pid = pg_backend_pid()) AS me\n"
        "FROM pg_stat_replication AS s FULL OUTER JOIN json_populate_recordset(NULL::record, $1::json) AS v (application_name text, sync_state text) USING (application_name)\n"
        "WHERE state = 'streaming' AND s.sync_state IS DISTINCT FROM v.sync_state";
    if (PQisBusy(backend->conn)) backend->events = WL_SOCKET_READABLE; else if (!PQsendQueryParams(backend->conn, command, countof(paramTypes), paramTypes, paramValues, NULL, NULL, false)) {
        W("%s:%s !PQsendQueryParams and %.*s", PQhost(backend->conn), init_state2char(backend->state), (int)strlen(PQerrorMessage(backend->conn)) - 1, PQerrorMessage(backend->conn));
        backend_finish(backend);
    } else {
        backend->events = WL_SOCKET_WRITEABLE;
        backend->socket = standby_query_socket;
    }
}

void standby_timeout(void) {
    if (!standby_primary) standby_create(PrimaryConnInfo);
    else if (PQstatus(standby_primary->conn) == CONNECTION_OK) standby_query(standby_primary);
}

void standby_updated(Backend *backend) {
}
