#include "include.h"

Backend *primary = NULL;
extern char *default_primary;
extern char *hostname;
extern int default_reset;
extern queue_t backend_queue;

STATE backend_state(const char *state) {
    if (!pg_strcasecmp(state, "async")) return ASYNC;
    if (!pg_strcasecmp(state, "potential")) return POTENTIAL;
    if (!pg_strcasecmp(state, "sync")) return SYNC;
    if (!pg_strcasecmp(state, "quorum")) return QUORUM;
    E("unknown state = %s", state);
}

const char *backend_state_str(STATE state) {
    switch (state) {
        case PRIMARY: return "primary";
        case ASYNC: return "async";
        case POTENTIAL: return "potential";
        case SYNC: return "sync";
        case QUORUM: return "quorum";
    }
    return "unknown";
}

static char *backend_int2char(int number) {
    StringInfoData buf;
    initStringInfo(&buf);
    appendStringInfo(&buf, "%i", number);
    return buf.data;
}

static void backend_idle_socket(Backend *backend) {
    for (PGresult *result; (result = PQgetResult(backend->conn)); PQclear(result)) switch (PQresultStatus(result)) {
        default: D1("%s:%s/%s PQresultStatus = %s and %s", PQhost(backend->conn), PQport(backend->conn), backend_state_str(backend->state), PQresStatus(PQresultStatus(result)), PQresultErrorMessage(result)); break;
    }
}

void backend_idle(Backend *backend) {
    backend->socket = backend_idle_socket;
}

void backend_finish(Backend *backend) {
    D1("%s:%s/%s", PQhost(backend->conn), PQport(backend->conn), backend_state_str(backend->state));
    if (backend->state == PRIMARY) primary = NULL;
    queue_remove(&backend->queue);
    PQfinish(backend->conn);
    pfree(backend);
}

static void backend_reset_socket(Backend *backend) {
    bool connected = false;
    switch (PQstatus(backend->conn)) {
        case CONNECTION_AUTH_OK: D1("%s:%s/%s PQstatus == CONNECTION_AUTH_OK", PQhost(backend->conn), PQport(backend->conn), backend_state_str(backend->state)); break;
        case CONNECTION_AWAITING_RESPONSE: D1("%s:%s/%s PQstatus == CONNECTION_AWAITING_RESPONSE", PQhost(backend->conn), PQport(backend->conn), backend_state_str(backend->state)); break;
        case CONNECTION_BAD: E("%s:%s/%s PQstatus == CONNECTION_BAD and %s", PQhost(backend->conn), PQport(backend->conn), backend_state_str(backend->state), PQerrorMessage(backend->conn)); break;
#if (PG_VERSION_NUM >= 130000)
        case CONNECTION_CHECK_TARGET: D1("%s:%s/%s PQstatus == CONNECTION_CHECK_TARGET", PQhost(backend->conn), PQport(backend->conn), backend_state_str(backend->state)); break;
#endif
        case CONNECTION_CHECK_WRITABLE: D1("%s:%s/%s PQstatus == CONNECTION_CHECK_WRITABLE", PQhost(backend->conn), PQport(backend->conn), backend_state_str(backend->state)); break;
        case CONNECTION_CONSUME: D1("%s:%s/%s PQstatus == CONNECTION_CONSUME", PQhost(backend->conn), PQport(backend->conn), backend_state_str(backend->state)); break;
        case CONNECTION_GSS_STARTUP: D1("%s:%s/%s PQstatus == CONNECTION_GSS_STARTUP", PQhost(backend->conn), PQport(backend->conn), backend_state_str(backend->state)); break;
        case CONNECTION_MADE: D1("%s:%s/%s PQstatus == CONNECTION_MADE", PQhost(backend->conn), PQport(backend->conn), backend_state_str(backend->state)); break;
        case CONNECTION_NEEDED: D1("%s:%s/%s PQstatus == CONNECTION_NEEDED", PQhost(backend->conn), PQport(backend->conn), backend_state_str(backend->state)); break;
        case CONNECTION_OK: D1("%s:%s/%s PQstatus == CONNECTION_OK", PQhost(backend->conn), PQport(backend->conn), backend_state_str(backend->state)); connected = true; break;
        case CONNECTION_SETENV: D1("%s:%s/%s PQstatus == CONNECTION_SETENV", PQhost(backend->conn), PQport(backend->conn), backend_state_str(backend->state)); break;
        case CONNECTION_SSL_STARTUP: D1("%s:%s/%s PQstatus == CONNECTION_SSL_STARTUP", PQhost(backend->conn), PQport(backend->conn), backend_state_str(backend->state)); break;
        case CONNECTION_STARTED: D1("%s:%s/%s PQstatus == CONNECTION_STARTED", PQhost(backend->conn), PQport(backend->conn), backend_state_str(backend->state)); break;
    }
    switch (PQresetPoll(backend->conn)) {
        case PGRES_POLLING_ACTIVE: D1("%s:%s/%s PQresetPoll == PGRES_POLLING_ACTIVE", PQhost(backend->conn), PQport(backend->conn), backend_state_str(backend->state)); break;
        case PGRES_POLLING_FAILED: E("%s:%s/%s PQresetPoll == PGRES_POLLING_FAILED and %s", PQhost(backend->conn), PQport(backend->conn), backend_state_str(backend->state), PQerrorMessage(backend->conn)); break;
        case PGRES_POLLING_OK: D1("%s:%s/%s PQresetPoll == PGRES_POLLING_OK", PQhost(backend->conn), PQport(backend->conn), backend_state_str(backend->state)); connected = true; break;
        case PGRES_POLLING_READING: D1("%s:%s/%s PQresetPoll == PGRES_POLLING_READING", PQhost(backend->conn), PQport(backend->conn), backend_state_str(backend->state)); backend->events = WL_SOCKET_READABLE; break;
        case PGRES_POLLING_WRITING: D1("%s:%s/%s PQresetPoll == PGRES_POLLING_WRITING", PQhost(backend->conn), PQport(backend->conn), backend_state_str(backend->state)); backend->events = WL_SOCKET_WRITEABLE; break;
    }
    if (connected) { backend->reset = 0; backend->connect(backend); }
}

void backend_reset(Backend *backend, void (*connect) (Backend *backend), void (*after) (Backend *backend)) {
    const char *keywords[] = {"host", "port", "user", "dbname", "application_name", NULL};
    const char *values[] = {PQhost(backend->conn), PQport(backend->conn), PQuser(backend->conn), PQdb(backend->conn), PQparameterStatus(backend->conn, "application_name"), NULL};
    backend->reset++;
    W("%s:%s/%s %i < %i", PQhost(backend->conn), PQport(backend->conn), backend_state_str(backend->state), backend->reset, default_reset);
    if (backend->reset >= default_reset) { after(backend); return; }
    StaticAssertStmt(countof(keywords) == countof(values), "countof(keywords) == countof(values)");
    switch (PQpingParams(keywords, values, false)) {
        case PQPING_NO_ATTEMPT: E("%s:%s/%s PQpingParams == PQPING_NO_ATTEMPT", PQhost(backend->conn), PQport(backend->conn), backend_state_str(backend->state)); break;
        case PQPING_NO_RESPONSE: W("%s:%s/%s PQpingParams == PQPING_NO_RESPONSE", PQhost(backend->conn), PQport(backend->conn), backend_state_str(backend->state)); return;
        case PQPING_OK: D1("%s:%s/%s PQpingParams == PQPING_OK", PQhost(backend->conn), PQport(backend->conn), backend_state_str(backend->state)); break;
        case PQPING_REJECT: W("%s:%s/%s PQpingParams == PQPING_REJECT", PQhost(backend->conn), PQport(backend->conn), backend_state_str(backend->state)); return;
    }
    if (!(PQresetStart(backend->conn))) E("%s:%s/%s !PQresetStart and %s", PQhost(backend->conn), PQport(backend->conn), backend_state_str(backend->state), PQerrorMessage(backend->conn));
    if (PQstatus(backend->conn) == CONNECTION_BAD) E("%s:%s/%s PQstatus == CONNECTION_BAD and %s", PQhost(backend->conn), PQport(backend->conn), backend_state_str(backend->state), PQerrorMessage(backend->conn));
    if (!PQisnonblocking(backend->conn) && PQsetnonblocking(backend->conn, true) == -1) E("%s:%s/%s PQsetnonblocking == -1 and %s", PQhost(backend->conn), PQport(backend->conn), backend_state_str(backend->state), PQerrorMessage(backend->conn));
    if (PQclientEncoding(backend->conn) != GetDatabaseEncoding()) PQsetClientEncoding(backend->conn, GetDatabaseEncodingName());
    backend->connect = connect;
    backend->socket = backend_reset_socket;
    backend->events = WL_SOCKET_WRITEABLE;
}

static void backend_connect_socket(Backend *backend) {
    bool connected = false;
    switch (PQstatus(backend->conn)) {
        case CONNECTION_AUTH_OK: D1("%s:%s/%s PQstatus == CONNECTION_AUTH_OK", PQhost(backend->conn), PQport(backend->conn), backend_state_str(backend->state)); break;
        case CONNECTION_AWAITING_RESPONSE: D1("%s:%s/%s PQstatus == CONNECTION_AWAITING_RESPONSE", PQhost(backend->conn), PQport(backend->conn), backend_state_str(backend->state)); break;
        case CONNECTION_BAD: E("%s:%s/%s PQstatus == CONNECTION_BAD and %s", PQhost(backend->conn), PQport(backend->conn), backend_state_str(backend->state), PQerrorMessage(backend->conn)); break;
#if (PG_VERSION_NUM >= 130000)
        case CONNECTION_CHECK_TARGET: D1("%s:%s/%s PQstatus == CONNECTION_CHECK_TARGET", PQhost(backend->conn), PQport(backend->conn), backend_state_str(backend->state)); break;
#endif
        case CONNECTION_CHECK_WRITABLE: D1("%s:%s/%s PQstatus == CONNECTION_CHECK_WRITABLE", PQhost(backend->conn), PQport(backend->conn), backend_state_str(backend->state)); break;
        case CONNECTION_CONSUME: D1("%s:%s/%s PQstatus == CONNECTION_CONSUME", PQhost(backend->conn), PQport(backend->conn), backend_state_str(backend->state)); break;
        case CONNECTION_GSS_STARTUP: D1("%s:%s/%s PQstatus == CONNECTION_GSS_STARTUP", PQhost(backend->conn), PQport(backend->conn), backend_state_str(backend->state)); break;
        case CONNECTION_MADE: D1("%s:%s/%s PQstatus == CONNECTION_MADE", PQhost(backend->conn), PQport(backend->conn), backend_state_str(backend->state)); break;
        case CONNECTION_NEEDED: D1("%s:%s/%s PQstatus == CONNECTION_NEEDED", PQhost(backend->conn), PQport(backend->conn), backend_state_str(backend->state)); break;
        case CONNECTION_OK: D1("%s:%s/%s PQstatus == CONNECTION_OK", PQhost(backend->conn), PQport(backend->conn), backend_state_str(backend->state)); connected = true; break;
        case CONNECTION_SETENV: D1("%s:%s/%s PQstatus == CONNECTION_SETENV", PQhost(backend->conn), PQport(backend->conn), backend_state_str(backend->state)); break;
        case CONNECTION_SSL_STARTUP: D1("%s:%s/%s PQstatus == CONNECTION_SSL_STARTUP", PQhost(backend->conn), PQport(backend->conn), backend_state_str(backend->state)); break;
        case CONNECTION_STARTED: D1("%s:%s/%s PQstatus == CONNECTION_STARTED", PQhost(backend->conn), PQport(backend->conn), backend_state_str(backend->state)); break;
    }
    switch (PQconnectPoll(backend->conn)) {
        case PGRES_POLLING_ACTIVE: D1("%s:%s/%s PQconnectPoll == PGRES_POLLING_ACTIVE", PQhost(backend->conn), PQport(backend->conn), backend_state_str(backend->state)); break;
        case PGRES_POLLING_FAILED: E("%s:%s/%s PQconnectPoll == PGRES_POLLING_FAILED and %s", PQhost(backend->conn), PQport(backend->conn), backend_state_str(backend->state), PQerrorMessage(backend->conn)); break;
        case PGRES_POLLING_OK: D1("%s:%s/%s PQconnectPoll == PGRES_POLLING_OK", PQhost(backend->conn), PQport(backend->conn), backend_state_str(backend->state)); connected = true; break;
        case PGRES_POLLING_READING: D1("%s:%s/%s PQconnectPoll == PGRES_POLLING_READING", PQhost(backend->conn), PQport(backend->conn), backend_state_str(backend->state)); backend->events = WL_SOCKET_READABLE; break;
        case PGRES_POLLING_WRITING: D1("%s:%s/%s PQconnectPoll == PGRES_POLLING_WRITING", PQhost(backend->conn), PQport(backend->conn), backend_state_str(backend->state)); backend->events = WL_SOCKET_WRITEABLE; break;
    }
    if (connected) backend->connect(backend);
}

void backend_connect(Backend *backend, const char *host, int port, const char *user, const char *dbname, void (*connect) (Backend *backend)) {
    char *cport = backend_int2char(port);
    const char *keywords[] = {"host", "port", "user", "dbname", "application_name", NULL};
    const char *values[] = {host, cport, user, dbname, hostname, NULL};
    StaticAssertStmt(countof(keywords) == countof(values), "countof(keywords) == countof(values)");
    D1("host = %s, port = %i, user = %s, dbname = %s", host, port, user, dbname);
    switch (PQpingParams(keywords, values, false)) {
        case PQPING_NO_ATTEMPT: E("%s:%s/%s PQpingParams == PQPING_NO_ATTEMPT", host, cport, backend_state_str(backend->state)); break;
        case PQPING_NO_RESPONSE: E("%s:%s/%s PQpingParams == PQPING_NO_RESPONSE", host, cport, backend_state_str(backend->state)); break;
        case PQPING_OK: D1("%s:%s/%s PQpingParams == PQPING_OK", host, cport, backend_state_str(backend->state)); break;
        case PQPING_REJECT: E("%s:%s/%s PQpingParams == PQPING_REJECT", host, cport, backend_state_str(backend->state)); break;
    }
    if (!(backend->conn = PQconnectStartParams(keywords, values, false))) E("%s:%s/%s !PQconnectStartParams and %s", PQhost(backend->conn), PQport(backend->conn), backend_state_str(backend->state), PQerrorMessage(backend->conn));
    pfree(cport);
    if (PQstatus(backend->conn) == CONNECTION_BAD) E("%s:%s/%s PQstatus == CONNECTION_BAD and %s", PQhost(backend->conn), PQport(backend->conn), backend_state_str(backend->state), PQerrorMessage(backend->conn));
    if (!PQisnonblocking(backend->conn) && PQsetnonblocking(backend->conn, true) == -1) E("%s:%s/%s PQsetnonblocking == -1 and %s", PQhost(backend->conn), PQport(backend->conn), backend_state_str(backend->state), PQerrorMessage(backend->conn));
    if (PQclientEncoding(backend->conn) != GetDatabaseEncoding()) PQsetClientEncoding(backend->conn, GetDatabaseEncodingName());
    backend->connect = connect;
    backend->socket = backend_connect_socket;
    backend->events = WL_SOCKET_WRITEABLE;
    if (backend->state == PRIMARY) {
        primary = backend;
        backend_alter_system_set("pg_save.primary", default_primary, PQhost(backend->conn));
    }
    queue_insert_tail(&backend_queue, &backend->queue);
}

void backend_fini(void) {
    queue_each(&backend_queue, queue) {
        Backend *backend = queue_data(queue, Backend, queue);
        backend_finish(backend);
    }
}

static Node *makeStringConst(char *str, int location) {
    A_Const *n = makeNode(A_Const);
    n->val.type = T_String;
    n->val.val.str = str;
    n->location = location;
    return (Node *)n;
}

#define DirectFunctionCall0(func) DirectFunctionCall0Coll(func, InvalidOid)
static Datum DirectFunctionCall0Coll(PGFunction func, Oid collation) {
    LOCAL_FCINFO(fcinfo, 0);
    Datum result;
    InitFunctionCallInfoData(*fcinfo, NULL, 0, collation, NULL, NULL);
    result = (*func)(fcinfo);
    if (fcinfo->isnull) E("function %p returned NULL", (void *)func);
    return result;
}

void backend_alter_system_set(const char *name, const char *old, const char *new) {
    AlterSystemStmt *stmt;
    if (!pg_strcasecmp(old, new)) return;
    stmt = makeNode(AlterSystemStmt);
    stmt->setstmt = makeNode(VariableSetStmt);
    stmt->setstmt->name = (char *)name;
    stmt->setstmt->kind = VAR_SET_VALUE;
    stmt->setstmt->args = list_make1(makeStringConst((char *)new, -1));
    AlterSystemSetConfigFile(stmt);
    list_free_deep(stmt->setstmt->args);
    pfree(stmt->setstmt);
    pfree(stmt);
    if (!DatumGetBool(DirectFunctionCall0(pg_reload_conf))) E("!pg_reload_conf");
}
