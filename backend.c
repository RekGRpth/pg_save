#include "include.h"

extern char *hostname;
extern char *init_async;
extern char *init_potential;
extern char *init_primary;
extern char *init_quorum;
extern char *init_sync;
extern int init_probe;
extern queue_t backend_queue;

const char *backend_state(Backend *backend) {
    return backend->state ? backend->state : "primary";
}

static void backend_idle_socket(Backend *backend) {
    for (PGresult *result; (result = PQgetResult(backend->conn)); PQclear(result)) switch (PQresultStatus(result)) {
        default: D1("%s:%s/%s PQresultStatus = %s and %s", PQhost(backend->conn), PQport(backend->conn), backend_state(backend), PQresStatus(PQresultStatus(result)), PQresultErrorMessage(result)); break;
    }
}

void backend_idle(Backend *backend) {
    backend->socket = backend_idle_socket;
}

void backend_finish(Backend *backend) {
    D1("%s:%s/%s", PQhost(backend->conn), PQport(backend->conn), backend_state(backend));
    queue_remove(&backend->queue);
    backend->finish(backend);
    PQfinish(backend->conn);
    if (backend->name) pfree(backend->name);
    if (backend->state) pfree(backend->state);
    pfree(backend);
}

static void backend_reset_socket(Backend *backend) {
    bool connected = false;
    switch (PQstatus(backend->conn)) {
        case CONNECTION_AUTH_OK: D1("%s:%s/%s PQstatus == CONNECTION_AUTH_OK", PQhost(backend->conn), PQport(backend->conn), backend_state(backend)); break;
        case CONNECTION_AWAITING_RESPONSE: D1("%s:%s/%s PQstatus == CONNECTION_AWAITING_RESPONSE", PQhost(backend->conn), PQport(backend->conn), backend_state(backend)); break;
        case CONNECTION_BAD: E("%s:%s/%s PQstatus == CONNECTION_BAD and %s", PQhost(backend->conn), PQport(backend->conn), backend_state(backend), PQerrorMessage(backend->conn)); break;
#if (PG_VERSION_NUM >= 130000)
        case CONNECTION_CHECK_TARGET: D1("%s:%s/%s PQstatus == CONNECTION_CHECK_TARGET", PQhost(backend->conn), PQport(backend->conn), backend_state(backend)); break;
#endif
        case CONNECTION_CHECK_WRITABLE: D1("%s:%s/%s PQstatus == CONNECTION_CHECK_WRITABLE", PQhost(backend->conn), PQport(backend->conn), backend_state(backend)); break;
        case CONNECTION_CONSUME: D1("%s:%s/%s PQstatus == CONNECTION_CONSUME", PQhost(backend->conn), PQport(backend->conn), backend_state(backend)); break;
        case CONNECTION_GSS_STARTUP: D1("%s:%s/%s PQstatus == CONNECTION_GSS_STARTUP", PQhost(backend->conn), PQport(backend->conn), backend_state(backend)); break;
        case CONNECTION_MADE: D1("%s:%s/%s PQstatus == CONNECTION_MADE", PQhost(backend->conn), PQport(backend->conn), backend_state(backend)); break;
        case CONNECTION_NEEDED: D1("%s:%s/%s PQstatus == CONNECTION_NEEDED", PQhost(backend->conn), PQport(backend->conn), backend_state(backend)); break;
        case CONNECTION_OK: D1("%s:%s/%s PQstatus == CONNECTION_OK", PQhost(backend->conn), PQport(backend->conn), backend_state(backend)); connected = true; break;
        case CONNECTION_SETENV: D1("%s:%s/%s PQstatus == CONNECTION_SETENV", PQhost(backend->conn), PQport(backend->conn), backend_state(backend)); break;
        case CONNECTION_SSL_STARTUP: D1("%s:%s/%s PQstatus == CONNECTION_SSL_STARTUP", PQhost(backend->conn), PQport(backend->conn), backend_state(backend)); break;
        case CONNECTION_STARTED: D1("%s:%s/%s PQstatus == CONNECTION_STARTED", PQhost(backend->conn), PQport(backend->conn), backend_state(backend)); break;
    }
    if (!connected) switch (PQresetPoll(backend->conn)) {
        case PGRES_POLLING_ACTIVE: D1("%s:%s/%s PQresetPoll == PGRES_POLLING_ACTIVE", PQhost(backend->conn), PQport(backend->conn), backend_state(backend)); break;
        case PGRES_POLLING_FAILED: E("%s:%s/%s PQresetPoll == PGRES_POLLING_FAILED and %s", PQhost(backend->conn), PQport(backend->conn), backend_state(backend), PQerrorMessage(backend->conn)); break;
        case PGRES_POLLING_OK: D1("%s:%s/%s PQresetPoll == PGRES_POLLING_OK", PQhost(backend->conn), PQport(backend->conn), backend_state(backend)); connected = true; break;
        case PGRES_POLLING_READING: D1("%s:%s/%s PQresetPoll == PGRES_POLLING_READING", PQhost(backend->conn), PQport(backend->conn), backend_state(backend)); backend->events = WL_SOCKET_READABLE; break;
        case PGRES_POLLING_WRITING: D1("%s:%s/%s PQresetPoll == PGRES_POLLING_WRITING", PQhost(backend->conn), PQport(backend->conn), backend_state(backend)); backend->events = WL_SOCKET_WRITEABLE; break;
    }
    if (connected) backend->connect(backend);
}

void backend_reset(Backend *backend) {
    const char *keywords[] = {"host", "port", "user", "dbname", "application_name", NULL};
    const char *values[] = {PQhost(backend->conn), PQport(backend->conn), PQuser(backend->conn), PQdb(backend->conn), PQparameterStatus(backend->conn, "application_name"), NULL};
    StaticAssertStmt(countof(keywords) == countof(values), "countof(keywords) == countof(values)");
    switch (PQpingParams(keywords, values, false)) {
        case PQPING_NO_ATTEMPT: E("%s:%s/%s PQpingParams == PQPING_NO_ATTEMPT", PQhost(backend->conn), PQport(backend->conn), backend_state(backend)); break;
        case PQPING_NO_RESPONSE: W("%s:%s/%s PQpingParams == PQPING_NO_RESPONSE and %i < %i", PQhost(backend->conn), PQport(backend->conn), backend_state(backend), backend->probe, init_probe); backend->reset(backend); return;
        case PQPING_OK: D1("%s:%s/%s PQpingParams == PQPING_OK", PQhost(backend->conn), PQport(backend->conn), backend_state(backend)); break;
        case PQPING_REJECT: W("%s:%s/%s PQpingParams == PQPING_REJECT and %i < %i", PQhost(backend->conn), PQport(backend->conn), backend_state(backend), backend->probe, init_probe); backend->reset(backend); return;
    }
    if (!(PQresetStart(backend->conn))) E("%s:%s/%s !PQresetStart and %s", PQhost(backend->conn), PQport(backend->conn), backend_state(backend), PQerrorMessage(backend->conn));
    if (PQstatus(backend->conn) == CONNECTION_BAD) E("%s:%s/%s PQstatus == CONNECTION_BAD and %s", PQhost(backend->conn), PQport(backend->conn), backend_state(backend), PQerrorMessage(backend->conn));
    if (!PQisnonblocking(backend->conn) && PQsetnonblocking(backend->conn, true) == -1) E("%s:%s/%s PQsetnonblocking == -1 and %s", PQhost(backend->conn), PQport(backend->conn), backend_state(backend), PQerrorMessage(backend->conn));
    if (PQclientEncoding(backend->conn) != GetDatabaseEncoding()) PQsetClientEncoding(backend->conn, GetDatabaseEncodingName());
    backend->socket = backend_reset_socket;
    backend->events = WL_SOCKET_WRITEABLE;
}

static void backend_connect_socket(Backend *backend) {
    bool connected = false;
    switch (PQstatus(backend->conn)) {
        case CONNECTION_AUTH_OK: D1("%s:%s/%s PQstatus == CONNECTION_AUTH_OK", PQhost(backend->conn), PQport(backend->conn), backend_state(backend)); break;
        case CONNECTION_AWAITING_RESPONSE: D1("%s:%s/%s PQstatus == CONNECTION_AWAITING_RESPONSE", PQhost(backend->conn), PQport(backend->conn), backend_state(backend)); break;
        case CONNECTION_BAD: E("%s:%s/%s PQstatus == CONNECTION_BAD and %s", PQhost(backend->conn), PQport(backend->conn), backend_state(backend), PQerrorMessage(backend->conn)); break;
#if (PG_VERSION_NUM >= 130000)
        case CONNECTION_CHECK_TARGET: D1("%s:%s/%s PQstatus == CONNECTION_CHECK_TARGET", PQhost(backend->conn), PQport(backend->conn), backend_state(backend)); break;
#endif
        case CONNECTION_CHECK_WRITABLE: D1("%s:%s/%s PQstatus == CONNECTION_CHECK_WRITABLE", PQhost(backend->conn), PQport(backend->conn), backend_state(backend)); break;
        case CONNECTION_CONSUME: D1("%s:%s/%s PQstatus == CONNECTION_CONSUME", PQhost(backend->conn), PQport(backend->conn), backend_state(backend)); break;
        case CONNECTION_GSS_STARTUP: D1("%s:%s/%s PQstatus == CONNECTION_GSS_STARTUP", PQhost(backend->conn), PQport(backend->conn), backend_state(backend)); break;
        case CONNECTION_MADE: D1("%s:%s/%s PQstatus == CONNECTION_MADE", PQhost(backend->conn), PQport(backend->conn), backend_state(backend)); break;
        case CONNECTION_NEEDED: D1("%s:%s/%s PQstatus == CONNECTION_NEEDED", PQhost(backend->conn), PQport(backend->conn), backend_state(backend)); break;
        case CONNECTION_OK: D1("%s:%s/%s PQstatus == CONNECTION_OK", PQhost(backend->conn), PQport(backend->conn), backend_state(backend)); connected = true; break;
        case CONNECTION_SETENV: D1("%s:%s/%s PQstatus == CONNECTION_SETENV", PQhost(backend->conn), PQport(backend->conn), backend_state(backend)); break;
        case CONNECTION_SSL_STARTUP: D1("%s:%s/%s PQstatus == CONNECTION_SSL_STARTUP", PQhost(backend->conn), PQport(backend->conn), backend_state(backend)); break;
        case CONNECTION_STARTED: D1("%s:%s/%s PQstatus == CONNECTION_STARTED", PQhost(backend->conn), PQport(backend->conn), backend_state(backend)); break;
    }
    if (!connected) switch (PQconnectPoll(backend->conn)) {
        case PGRES_POLLING_ACTIVE: D1("%s:%s/%s PQconnectPoll == PGRES_POLLING_ACTIVE", PQhost(backend->conn), PQport(backend->conn), backend_state(backend)); break;
        case PGRES_POLLING_FAILED: E("%s:%s/%s PQconnectPoll == PGRES_POLLING_FAILED and %s", PQhost(backend->conn), PQport(backend->conn), backend_state(backend), PQerrorMessage(backend->conn)); break;
        case PGRES_POLLING_OK: D1("%s:%s/%s PQconnectPoll == PGRES_POLLING_OK", PQhost(backend->conn), PQport(backend->conn), backend_state(backend)); connected = true; break;
        case PGRES_POLLING_READING: D1("%s:%s/%s PQconnectPoll == PGRES_POLLING_READING", PQhost(backend->conn), PQport(backend->conn), backend_state(backend)); backend->events = WL_SOCKET_READABLE; break;
        case PGRES_POLLING_WRITING: D1("%s:%s/%s PQconnectPoll == PGRES_POLLING_WRITING", PQhost(backend->conn), PQport(backend->conn), backend_state(backend)); backend->events = WL_SOCKET_WRITEABLE; break;
    }
    if (connected) backend->connect(backend);
}

void backend_connect(Backend *backend, const char *host, const char *port, const char *user, const char *dbname) {
    const char *keywords[] = {"host", "port", "user", "dbname", "application_name", NULL};
    const char *values[] = {host, port, user, dbname, hostname, NULL};
    StaticAssertStmt(countof(keywords) == countof(values), "countof(keywords) == countof(values)");
    D1("host = %s, port = %s, user = %s, dbname = %s", host, port, user, dbname);
    switch (PQpingParams(keywords, values, false)) {
        case PQPING_NO_ATTEMPT: E("%s:%s/%s PQpingParams == PQPING_NO_ATTEMPT", host, port, backend_state(backend)); break;
        case PQPING_NO_RESPONSE: W("%s:%s/%s PQpingParams == PQPING_NO_RESPONSE and %i < %i", host, port, backend_state(backend), backend->probe, init_probe); backend->reset(backend); return;
        case PQPING_OK: D1("%s:%s/%s PQpingParams == PQPING_OK", host, port, backend_state(backend)); break;
        case PQPING_REJECT: W("%s:%s/%s PQpingParams == PQPING_REJECT and %i < %i", host, port, backend_state(backend), backend->probe, init_probe); backend->reset(backend); return;
    }
    if (!(backend->conn = PQconnectStartParams(keywords, values, false))) E("%s:%s/%s !PQconnectStartParams and %s", PQhost(backend->conn), PQport(backend->conn), backend_state(backend), PQerrorMessage(backend->conn));
    if (PQstatus(backend->conn) == CONNECTION_BAD) E("%s:%s/%s PQstatus == CONNECTION_BAD and %s", PQhost(backend->conn), PQport(backend->conn), backend_state(backend), PQerrorMessage(backend->conn));
    if (!PQisnonblocking(backend->conn) && PQsetnonblocking(backend->conn, true) == -1) E("%s:%s/%s PQsetnonblocking == -1 and %s", PQhost(backend->conn), PQport(backend->conn), backend_state(backend), PQerrorMessage(backend->conn));
    if (PQclientEncoding(backend->conn) != GetDatabaseEncoding()) PQsetClientEncoding(backend->conn, GetDatabaseEncodingName());
    backend->socket = backend_connect_socket;
    backend->events = WL_SOCKET_WRITEABLE;
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
    if (old && old[0] != '\0' && !strcmp(old, new)) return;
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

void backend_alter_system_reset(const char *name) {
    AlterSystemStmt *stmt;
    stmt = makeNode(AlterSystemStmt);
    stmt->setstmt = makeNode(VariableSetStmt);
    stmt->setstmt->name = (char *)name;
    stmt->setstmt->kind = VAR_RESET;
    AlterSystemSetConfigFile(stmt);
    pfree(stmt->setstmt);
    pfree(stmt);
    if (!DatumGetBool(DirectFunctionCall0(pg_reload_conf))) E("!pg_reload_conf");
}

void backend_set_state(const char *state, const char *host) {
    char *old;
    StringInfoData buf;
    initStringInfo(&buf);
    appendStringInfo(&buf, "pg_save.%s", state);
    old = GetConfigOptionByName(buf.data, NULL, false);
    backend_alter_system_set(buf.data, old, host);
    pfree(old);
    pfree(buf.data);
}

void backend_reset_state(Backend *backend) {
    if (!strcmp(init_async, PQhost(backend->conn))) backend_alter_system_reset("pg_save.async");
    else if (!strcmp(init_potential, PQhost(backend->conn))) backend_alter_system_reset("pg_save.potential");
    else if (!strcmp(init_primary, PQhost(backend->conn))) backend_alter_system_reset("pg_save.primary");
    else if (!strcmp(init_quorum, PQhost(backend->conn))) backend_alter_system_reset("pg_save.quorum");
    else if (!strcmp(init_sync, PQhost(backend->conn))) backend_alter_system_reset("pg_save.sync");
}

