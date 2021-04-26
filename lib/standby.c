#include "lib.h"

extern char *hostname;
extern int init_attempt;
extern state_t init_state;
static Backend *standby_primary = NULL;

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
        if (!opt->val) continue;
        D1("%s = %s", opt->keyword, opt->val);
        if (strcmp(opt->keyword, "host")) continue;
        backend_create(opt->val, state_wait_primary);
    }
    if (err) PQfreemem(err);
    PQconninfoFree(opts);
}

static void standby_promote(Backend *backend) {
    D1("state = %s", init_state2char(init_state));
    init_set_host(backend->host, state_wait_standby);
    backend_finish(backend);
    init_set_state(state_wait_primary);
    if (!DatumGetBool(DirectFunctionCall2(pg_promote, BoolGetDatum(true), Int32GetDatum(30)))) W("!pg_promote");
    else primary_init();
}

static void standby_reprimary(Backend *backend) {
    StringInfoData buf;
    if (standby_primary) { init_set_host(standby_primary->host, state_wait_standby); backend_finish(standby_primary); }
    initStringInfoMy(TopMemoryContext, &buf);
    appendStringInfo(&buf, "host=%s application_name=%s target_session_attrs=read-write", backend->host, hostname);
    init_set_host(backend->host, state_wait_primary);
    backend_finish(backend);
    init_set_system("primary_conninfo", buf.data);
    standby_create(buf.data);
    pfree(buf.data);
}

void standby_failed(Backend *backend) {
    if (backend->state > state_primary) { backend_finish(backend); return; }
    if (!backend_nevents()) { init_set_host(backend->host, state_wait_primary); if (kill(PostmasterPid, SIGKILL)) W("kill(%i, %i)", PostmasterPid, SIGKILL); return; }
    switch (init_state) {
        case state_sync: standby_promote(backend); break;
        case state_potential: if (backend->attempt >= 2 * init_attempt) {
            Backend *sync = backend_state(state_sync);
            if (sync) standby_reprimary(sync);
            else if (kill(PostmasterPid, SIGKILL)) W("kill(%i, %i)", PostmasterPid, SIGKILL);
        } break;
        default: E("init_state = %s", init_state2char(init_state)); break;
    }
}

void standby_finished(Backend *backend) {
    if (backend->state <= state_primary) standby_primary = NULL;
}

void standby_fini(void) {
}

void standby_init(void) {
    init_set_system("synchronous_standby_names", NULL);
    switch (init_state) {
        case state_async: break;
        case state_initial: break;
        case state_potential: break;
        case state_primary: init_set_state(state_initial); break;
        case state_quorum: break;
        case state_single: init_set_state(state_initial); break;
        case state_sync: break;
        case state_unknown: init_set_state(state_initial); break;
        case state_wait_primary: init_set_state(state_initial); break;
        case state_wait_standby: break;
        default: E("init_state = %s", init_state2char(init_state)); break;
    }
    if (!standby_primary) standby_create(PrimaryConnInfo);
}

void standby_notify(Backend *backend, state_t state) {
    if (backend->state == state_sync && init_state == state_potential && (state == state_wait_primary || state == state_primary)) standby_reprimary(backend);
}

static void standby_result(Backend *backend, PGresult *result) {
    for (int row = 0; row < PQntuples(result); row++) {
        const char *host = PQgetvalue(result, row, PQfnumber(result, "application_name"));
        const char *state = PQgetvalue(result, row, PQfnumber(result, "sync_state"));
        backend_result(host, init_char2state(state));
    }
    if (!PQntuples(result)) switch (init_state) {
        case state_async: init_set_state(state_wait_standby); break;
        case state_potential: init_set_state(state_wait_standby); break;
        case state_quorum: init_set_state(state_wait_standby); break;
        case state_sync: init_set_state(state_wait_standby); break;
        case state_wait_standby: break;
        default: E("init_state = %s", init_state2char(init_state)); break;
    } else switch (init_state) {
        case state_async: break;
//        case state_initial: break;
        case state_potential: break;
        case state_quorum: break;
        case state_sync: break;
        case state_wait_standby: break;
        default: E("init_state = %s", init_state2char(init_state)); break;
    }
    backend->attempt = 0;
    init_reload();
}

static void standby_query_socket(Backend *backend) {
    bool ok = false;
    for (PGresult *result; (result = PQgetResult(backend->conn)); PQclear(result)) switch (PQresultStatus(result)) {
        case PGRES_TUPLES_OK: ok = true; standby_result(backend, result); break;
        default: W("%s:%s PQresultStatus = %s and %.*s", backend->host, init_state2char(backend->state), PQresStatus(PQresultStatus(result)), (int)strlen(PQresultErrorMessage(result)) - 1, PQresultErrorMessage(result)); break;
    }
    ok ? backend_idle(backend) : backend_finish(backend);
}

static void standby_query(Backend *backend) {
    static char *command = "SELECT * FROM pg_stat_replication WHERE state = 'streaming' AND NOT EXISTS (SELECT * FROM pg_stat_progress_basebackup)";
    if (PQisBusy(backend->conn)) backend->events = WL_SOCKET_READABLE; else if (!PQsendQuery(backend->conn, command)) {
        W("%s:%s !PQsendQuery and %.*s", backend->host, init_state2char(backend->state), (int)strlen(PQerrorMessage(backend->conn)) - 1, PQerrorMessage(backend->conn));
        backend_finish(backend);
    } else {
        backend->events = WL_SOCKET_WRITEABLE;
        backend->socket = standby_query_socket;
    }
    backend_fail(standby_primary);
}

void standby_timeout(void) {
    if (!standby_primary) standby_create(PrimaryConnInfo);
    if (PQstatus(standby_primary->conn) == CONNECTION_OK) standby_query(standby_primary);
}

void standby_updated(Backend *backend) {
}

void standby_update(state_t state) {
    if (init_state == state) return;
    init_set_state(state);
    switch (init_state) {
        case state_async: init_set_host(standby_primary->host, state_primary); break;
        case state_potential: init_set_host(standby_primary->host, state_primary); break;
        case state_quorum: init_set_host(standby_primary->host, state_primary); break;
        case state_sync: init_set_host(standby_primary->host, state_primary); break;
        default: E("init_state = %s", init_state2char(init_state)); break;
    }
    init_reload();
}
