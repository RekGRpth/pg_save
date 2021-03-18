#include "include.h"

extern char *backend_save;
extern char *save_hostname;
extern char *schema_type;
extern int init_attempt;
extern queue_t save_queue;
extern STATE init_state;
static bool standby_prepared = false;

static void standby_create_slot_socket(Backend *backend) {
    for (PGresult *result; (result = PQgetResult(backend->conn)); PQclear(result)) switch (PQresultStatus(result)) {
        case PGRES_TUPLES_OK: break;
        default: W("%s:%s PQresultStatus = %s and %.*s", PQhost(backend->conn), init_state2char(backend->state), PQresStatus(PQresultStatus(result)), (int)strlen(PQresultErrorMessage(result)) - 1, PQresultErrorMessage(result)); break;
    }
    backend_idle(backend);
}

static void standby_create_slot(Backend *backend) {
    static Oid paramTypes[] = {NAMEOID};
    const char *paramValues[] = {PrimarySlotName};
    static const char *command = "SELECT pg_create_physical_replication_slot($1)";
    if (PQisBusy(backend->conn)) backend->events = WL_SOCKET_READABLE; else if (!PQsendQueryParams(backend->conn, command, countof(paramTypes), paramTypes, paramValues, NULL, NULL, false)) {
        W("%s:%s !PQsendQueryParams and %.*s", PQhost(backend->conn), init_state2char(backend->state), (int)strlen(PQerrorMessage(backend->conn)) - 1, PQerrorMessage(backend->conn));
        backend_finish(backend);
    } else {
        backend->socket = standby_create_slot_socket;
        backend->events = WL_SOCKET_WRITEABLE;
    }
}

static void standby_select_slot_socket(Backend *backend) {
    for (PGresult *result; (result = PQgetResult(backend->conn)); PQclear(result)) switch (PQresultStatus(result)) {
        case PGRES_TUPLES_OK: PQntuples(result) ? backend_idle(backend) : standby_create_slot(backend); break;
        default: W("%s:%s PQresultStatus = %s and %.*s", PQhost(backend->conn), init_state2char(backend->state), PQresStatus(PQresultStatus(result)), (int)strlen(PQresultErrorMessage(result)) - 1, PQresultErrorMessage(result)); backend_idle(backend); break;
    }
}

static void standby_select_slot(Backend *backend) {
    static Oid paramTypes[] = {NAMEOID};
    const char *paramValues[] = {PrimarySlotName};
    static const char *command = "select * FROM pg_replication_slots WHERE slot_name = $1";
    if (PQisBusy(backend->conn)) backend->events = WL_SOCKET_READABLE; else if (!PQsendQueryParams(backend->conn, command, countof(paramTypes), paramTypes, paramValues, NULL, NULL, false)) {
        W("%s:%s !PQsendQueryParams and %.*s", PQhost(backend->conn), init_state2char(backend->state), (int)strlen(PQerrorMessage(backend->conn)) - 1, PQerrorMessage(backend->conn));
        backend_finish(backend);
    } else {
        backend->socket = standby_select_slot_socket;
        backend->events = WL_SOCKET_WRITEABLE;
    }
}

void standby_connected(Backend *backend) {
    backend->attempt = 0;
    if (backend->state == PRIMARY) standby_prepared = false;
    init_set_remote_state(backend->state, PQhost(backend->conn));
    if (backend->state == PRIMARY) standby_select_slot(backend);
    else backend_idle(backend);
}

static void standby_promote(Backend *backend) {
    D1("state = %s", init_state2char(init_state));
    backend_finish(backend);
    if (!DatumGetBool(DirectFunctionCall2(pg_promote, BoolGetDatum(true), Int32GetDatum(30)))) W("!pg_promote");
    else primary_init();
}

static void standby_reprimary(Backend *backend) {
    D1("state = %s", init_state2char(init_state));
    backend_finish(backend);
    queue_each(&save_queue, queue) {
        Backend *backend = queue_data(queue, Backend, queue);
        StringInfoData buf;
        if (backend->state != SYNC) continue;
        initStringInfoMy(TopMemoryContext, &buf);
        appendStringInfo(&buf, "host=%s application_name=%s", PQhost(backend->conn), save_hostname);
        init_alter_system_set("primary_conninfo", buf.data);
        pfree(buf.data);
    }
}

void standby_failed(Backend *backend) {
    if (backend->state != PRIMARY) backend_finish(backend);
    else if (!queue_size(&save_queue)) { if (kill(PostmasterPid, SIGTERM)) W("kill and %m"); }
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
    if (init_state != UNKNOWN) init_set_remote_state(init_state, save_hostname);
}

static void standby_state(STATE state) {
    init_reset_remote_state(init_state);
    init_set_local_state(state);
    init_set_remote_state(state, save_hostname);
    init_reload();
    backend_array();
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
    const char *paramValues[] = {backend_save};
    if (PQisBusy(backend->conn)) backend->events = WL_SOCKET_READABLE; else if (!PQsendQueryPrepared(backend->conn, "standby_prepare", countof(paramValues), paramValues, NULL, NULL, false)) {
        W("%s:%s !PQsendQueryPrepared and %.*s", PQhost(backend->conn), init_state2char(backend->state), (int)strlen(PQerrorMessage(backend->conn)) - 1, PQerrorMessage(backend->conn));
        backend_finish(backend);
    } else {
        standby_prepared = true;
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
    if (PQisBusy(backend->conn)) backend->events = WL_SOCKET_READABLE; else if (!PQsendPrepare(backend->conn, "standby_prepare", command, countof(paramTypes), paramTypes)) {
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
    queue_each(&save_queue, queue) {
        Backend *backend = queue_data(queue, Backend, queue);
        if (backend->state == PRIMARY) primary = backend;
        if (PQstatus(backend->conn) == CONNECTION_BAD) backend_reset(backend);
    }
    if (!primary) standby_primary_connect();
    else if (PQstatus(primary->conn) != CONNECTION_OK);
    else if (standby_prepared) standby_query(primary);
    else standby_prepare(primary);
}

void standby_updated(Backend *backend) {
}
