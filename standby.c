#include "include.h"

extern int init_attempt;
extern queue_t save_queue;
extern state_t init_state;
static Backend *standby_primary = NULL;
static int standby_attempt = 0;

void standby_connected(Backend *backend) {
}

void standby_created(Backend *backend) {
    if (backend->state <= state_primary) standby_primary = backend;
}

static void standby_create(const char *conninfo) {
    char *err;
    PQconninfoOption *opts;
    if (!(opts = PQconninfoParse(conninfo, &err))) E("!PQconninfoParse and %s", err);
    for (PQconninfoOption *opt = opts; opt->keyword; opt++) {
//        state_t state;
        if (!opt->val) continue;
        D1("%s = %s", opt->keyword, opt->val);
        if (strcmp(opt->keyword, "host")) continue;
//        state = init_host(opt->val);
//        backend_create(opt->val, state == state_unknown ? state_wait_primary : state);
        backend_create(opt->val, state_wait_primary);
    }
    if (err) PQfreemem(err);
    PQconninfoFree(opts);
}

static void standby_promote(Backend *backend) {
    D1("state = %s", init_state2char(init_state));
    backend_finish(backend);
//    init_notify(MyBgworkerEntry->bgw_type, "reprimary");
    if (!DatumGetBool(DirectFunctionCall2(pg_promote, BoolGetDatum(true), Int32GetDatum(30)))) W("!pg_promote");
    else primary_init();
}

void standby_failed(Backend *backend) {
    if (backend->state > state_primary) { backend_update(backend, state_wait_standby); backend_finish(backend); return; }
    if (!queue_size(&save_queue)) { backend_update(backend, state_wait_primary); init_set_state(state_wait_standby); if (kill(PostmasterPid, SIGKILL)) W("kill(%i ,%i)", PostmasterPid, SIGKILL); return; }
    if (init_state == state_sync) standby_promote(backend);
}

void standby_finished(Backend *backend) {
    if (backend->state <= state_primary) standby_primary = NULL;
}

void standby_fini(void) {
}

void standby_init(void) {
    init_set_system("synchronous_standby_names", NULL);
    if (init_state <= state_primary) init_set_state(state_initial);
    standby_create(PrimaryConnInfo);
}

static void standby_reprimary(Backend *backend) {
    StringInfoData buf;
    if (standby_primary) backend_finish(standby_primary);
    initStringInfoMy(TopMemoryContext, &buf);
    appendStringInfo(&buf, "host=%s application_name=%s target_session_attrs=read-write", PQhost(backend->conn), MyBgworkerEntry->bgw_type);
    backend_finish(backend);
    init_set_system("primary_conninfo", buf.data);
    standby_create(buf.data);
    pfree(buf.data);
}

void standby_notify(Backend *backend, const char *state) {
    if (backend->state == state_sync && !strcmp(state, "reprimary")) standby_reprimary(backend);
}

static void standby_demote(Backend *backend) {
    backend_idle(backend);
    if (init_state != state_sync) return;
    if (queue_size(&save_queue) < 2) return;
    if (strcmp(MyBgworkerEntry->bgw_type, PQhost(backend->conn)) > 0) return;
    W("%i < %i", standby_attempt, init_attempt);
    if (standby_attempt++ < init_attempt) return;
//    init_notify(MyBgworkerEntry->bgw_type, "demote");
}

static void standby_result(PGresult *result) {
    if (!PQntuples(result)) switch (init_state) {
//        case state_async: init_set_state(state_wait_standby); break;
//        case state_initial: init_set_state(state_wait_standby); break;
//        case state_potential: init_set_state(state_wait_standby); break;
//        case state_quorum: init_set_state(state_wait_standby); break;
//        case state_sync: init_set_state(state_wait_standby); break;
//        case state_wait_standby: break;
        default: E("init_state = %s", init_state2char(init_state)); break;
    } else switch (init_state) {
        case state_async: backend_update(standby_primary, state_primary); break;
        case state_initial: backend_update(standby_primary, state_primary); break;
        case state_potential: backend_update(standby_primary, state_primary); break;
        case state_quorum: backend_update(standby_primary, state_primary); break;
        case state_sync: backend_update(standby_primary, state_primary); break;
//        case state_wait_standby: break;
        default: E("init_state = %s", init_state2char(init_state)); break;
    }
    for (int row = 0; row < PQntuples(result); row++) {
        const char *host = PQgetvalue(result, row, PQfnumber(result, "application_name"));
        const char *state = PQgetvalue(result, row, PQfnumber(result, "sync_state"));
        backend_result(host, init_char2state(state));
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
    static char *command = "SELECT * FROM pg_stat_replication WHERE state = 'streaming'";
    if (PQisBusy(backend->conn)) backend->events = WL_SOCKET_READABLE; else if (!PQsendQuery(backend->conn, command)) {
        W("%s:%s !PQsendQuery and %.*s", PQhost(backend->conn), init_state2char(backend->state), (int)strlen(PQerrorMessage(backend->conn)) - 1, PQerrorMessage(backend->conn));
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

void standby_update(state_t state) {
    if (init_state == state) return;
    init_set_state(state);
    init_reload();
}
