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
    if (!(opts = PQconninfoParse(conninfo, &err))) ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("invalid connection string syntax"), errdetail("%s", err)));
    for (PQconninfoOption *opt = opts; opt->keyword; opt++) {
        if (!opt->val) continue;
        elog(DEBUG1, "%s = %s", opt->keyword, opt->val);
        if (strcmp(opt->keyword, "host")) continue;
        backend_create(opt->val, state_wait_primary);
    }
    if (err) PQfreemem(err);
    PQconninfoFree(opts);
}

static void standby_promote(Backend *backend) {
    elog(DEBUG1, "state = %s", init_state2char(init_state));
    init_set_host(backend->host, state_wait_standby);
    init_set_state(state_wait_primary);
    backend_finish(backend);
#if PG_VERSION_NUM >= 120000
    if (!DatumGetBool(DirectFunctionCall2(pg_promote, BoolGetDatum(true), Int32GetDatum(30)))) elog(WARNING, "!pg_promote");
    else primary_init();
#endif
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
    if (!backend_nevents()) { init_set_host(backend->host, state_wait_primary); if (kill(PostmasterPid, SIGKILL)) elog(WARNING, "kill(%i, %i)", PostmasterPid, SIGKILL); return; }
    switch (init_state) {
        case state_sync: standby_promote(backend); break;
        case state_potential: if (backend->attempt >= 2 * init_attempt) {
            Backend *sync = backend_state(state_sync);
            if (sync) standby_reprimary(sync);
            else if (kill(PostmasterPid, SIGKILL)) elog(WARNING, "kill(%i, %i)", PostmasterPid, SIGKILL);
        } break;
        default: ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("unknown init_state = %s", init_state2char(init_state)))); break;
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
        default: ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("unknown init_state = %s", init_state2char(init_state)))); break;
    }
#if PG_VERSION_NUM >= 120000
    if (!standby_primary) standby_create(PrimaryConnInfo);
#endif
}

static void standby_result(Backend *backend, PGresult *result) {
    for (int row = 0; row < PQntuples(result); row++) {
        const char *host = PQgetvalue(result, row, PQfnumber(result, "application_name"));
        const char *state = PQgetvalue(result, row, PQfnumber(result, "sync_state"));
        backend_result(host, init_char2state(state));
    }
    backend_update(backend, PQntuples(result) ? state_primary : state_wait_primary);
    if (!PQntuples(result)) switch (init_state) {
        case state_async: init_set_state(state_wait_standby); break;
        case state_potential: init_set_state(state_wait_standby); break;
        case state_quorum: init_set_state(state_wait_standby); break;
        case state_sync: init_set_state(state_wait_standby); break;
        case state_wait_standby: break;
        default: ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("unknown init_state = %s", init_state2char(init_state)))); break;
    } else switch (init_state) {
        case state_async: break;
        case state_potential: break;
        case state_quorum: break;
        case state_sync: break;
        case state_wait_standby: break;
        default: ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("unknown init_state = %s", init_state2char(init_state)))); break;
    }
    backend->attempt = 0;
    init_reload();
}

static void standby_select_result(Backend *backend) {
    bool ok = false;
    for (PGresult *result; PQstatus(backend->conn) == CONNECTION_OK && (result = PQgetResult(backend->conn)); ) {
        switch (PQresultStatus(result)) {
            case PGRES_TUPLES_OK: ok = true; standby_result(backend, result); break;
            default: elog(WARNING, "%s:%s PQresultStatus = %s and %s", backend->host, init_state2char(backend->state), PQresStatus(PQresultStatus(result)), PQresultErrorMessageMy(result)); break;
        }
        PQclear(result);
    }
    if (ok) backend_idle(backend);
    else if (PQstatus(backend->conn) == CONNECTION_OK) backend_finish(backend);
}

static void standby_select(Backend *backend) {
    backend->socket = standby_select;
    if (!PQsendQuery(backend->conn, SQL(SELECT * FROM pg_stat_replication WHERE state = 'streaming' AND NOT EXISTS (SELECT * FROM pg_stat_progress_basebackup)))) { elog(WARNING, "%s:%s !PQsendQuery and %s", backend->host, init_state2char(backend->state), PQerrorMessageMy(backend->conn)); backend_finish(backend); return; }
    backend->socket = standby_select_result;
    backend->event = WL_SOCKET_READABLE;
}

void standby_timeout(void) {
#if PG_VERSION_NUM >= 120000
    if (!standby_primary) standby_create(PrimaryConnInfo);
#endif
    if (PQstatus(standby_primary->conn) == CONNECTION_OK) standby_select(standby_primary);
}

void standby_updated(Backend *backend) {
}

void standby_update(state_t state) {
    if (init_state == state) return;
    elog(DEBUG1, "%s:%s->%s", hostname, init_state2char(init_state), init_state2char(state));
    init_set_state(state);
    switch (init_state) {
        case state_async: init_set_host(standby_primary->host, state_primary); break;
        case state_potential: init_set_host(standby_primary->host, state_primary); break;
        case state_quorum: init_set_host(standby_primary->host, state_primary); break;
        case state_sync: init_set_host(standby_primary->host, state_primary); break;
        default: ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("unknown init_state = %s", init_state2char(init_state)))); break;
    }
    init_reload();
}
