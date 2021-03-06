#include "include.h"

Backend *primary = NULL;
extern int reset;
extern queue_t backend_queue;

static char *backend_int2char(int number) {
    StringInfoData buf;
    initStringInfo(&buf);
    appendStringInfo(&buf, "%i", number);
    return buf.data;
}

static void backend_idle_callback(Backend *backend) {
    if (!PQconsumeInput(backend->conn)) { W("%s:%s !PQconsumeInput and %s", PQhost(backend->conn), PQport(backend->conn), PQerrorMessage(backend->conn)); return; }
    if (PQisBusy(backend->conn)) { backend->events = WL_SOCKET_READABLE; return; }
    for (PGresult *result; (result = PQgetResult(backend->conn)); PQclear(result)) switch (PQresultStatus(result)) {
        default: D1("%s:%s PQresultStatus = %s and %s", PQhost(backend->conn), PQport(backend->conn), PQresStatus(PQresultStatus(result)), PQresultErrorMessage(result)); break;
    }
}

void backend_idle(Backend *backend) {
    backend->callback = backend_idle_callback;
}

void backend_finish(Backend *backend) {
    D1("%s:%s", PQhost(backend->conn), PQport(backend->conn));
    if (backend->state == PRIMARY) primary = NULL;
    queue_remove(&backend->queue);
    PQfinish(backend->conn);
    pfree(backend);
}

static void backend_reset_callback(Backend *backend) {
    bool connected = false;
    switch (PQstatus(backend->conn)) {
        case CONNECTION_AUTH_OK: D1("%s:%s PQstatus == CONNECTION_AUTH_OK", PQhost(backend->conn), PQport(backend->conn)); break;
        case CONNECTION_AWAITING_RESPONSE: D1("%s:%s PQstatus == CONNECTION_AWAITING_RESPONSE", PQhost(backend->conn), PQport(backend->conn)); break;
        case CONNECTION_BAD: E("%s:%s PQstatus == CONNECTION_BAD and %s", PQhost(backend->conn), PQport(backend->conn), PQerrorMessage(backend->conn)); break;
#if (PG_VERSION_NUM >= 130000)
        case CONNECTION_CHECK_TARGET: D1("%s:%s PQstatus == CONNECTION_CHECK_TARGET", PQhost(backend->conn), PQport(backend->conn)); break;
#endif
        case CONNECTION_CHECK_WRITABLE: D1("%s:%s PQstatus == CONNECTION_CHECK_WRITABLE", PQhost(backend->conn), PQport(backend->conn)); break;
        case CONNECTION_CONSUME: D1("%s:%s PQstatus == CONNECTION_CONSUME", PQhost(backend->conn), PQport(backend->conn)); break;
        case CONNECTION_GSS_STARTUP: D1("%s:%s PQstatus == CONNECTION_GSS_STARTUP", PQhost(backend->conn), PQport(backend->conn)); break;
        case CONNECTION_MADE: D1("%s:%s PQstatus == CONNECTION_MADE", PQhost(backend->conn), PQport(backend->conn)); break;
        case CONNECTION_NEEDED: D1("%s:%s PQstatus == CONNECTION_NEEDED", PQhost(backend->conn), PQport(backend->conn)); break;
        case CONNECTION_OK: D1("%s:%s PQstatus == CONNECTION_OK", PQhost(backend->conn), PQport(backend->conn)); connected = true; break;
        case CONNECTION_SETENV: D1("%s:%s PQstatus == CONNECTION_SETENV", PQhost(backend->conn), PQport(backend->conn)); break;
        case CONNECTION_SSL_STARTUP: D1("%s:%s PQstatus == CONNECTION_SSL_STARTUP", PQhost(backend->conn), PQport(backend->conn)); break;
        case CONNECTION_STARTED: D1("%s:%s PQstatus == CONNECTION_STARTED", PQhost(backend->conn), PQport(backend->conn)); break;
    }
    switch (PQresetPoll(backend->conn)) {
        case PGRES_POLLING_ACTIVE: D1("%s:%s PQresetPoll == PGRES_POLLING_ACTIVE", PQhost(backend->conn), PQport(backend->conn)); break;
        case PGRES_POLLING_FAILED: E("%s:%s PQresetPoll == PGRES_POLLING_FAILED and %s", PQhost(backend->conn), PQport(backend->conn), PQerrorMessage(backend->conn)); break;
        case PGRES_POLLING_OK: D1("%s:%s PQresetPoll == PGRES_POLLING_OK", PQhost(backend->conn), PQport(backend->conn)); connected = true; break;
        case PGRES_POLLING_READING: D1("%s:%s PQresetPoll == PGRES_POLLING_READING", PQhost(backend->conn), PQport(backend->conn)); backend->events = WL_SOCKET_READABLE; break;
        case PGRES_POLLING_WRITING: D1("%s:%s PQresetPoll == PGRES_POLLING_WRITING", PQhost(backend->conn), PQport(backend->conn)); backend->events = WL_SOCKET_WRITEABLE; break;
    }
    if (connected) {
        backend->reset = 0;
        backend_idle(backend);
    }
}

void backend_reset(Backend *backend, callback_t after) {
    const char *keywords[] = {"host", "port", "user", "dbname", "application_name", NULL};
    const char *values[] = {PQhost(backend->conn), PQport(backend->conn), PQuser(backend->conn), PQdb(backend->conn), PQparameterStatus(backend->conn, "application_name"), NULL};
    backend->reset++;
    W("%s:%s %i < %i", PQhost(backend->conn), PQport(backend->conn), backend->reset, reset);
    if (backend->reset >= reset) { after(backend); return; }
    StaticAssertStmt(countof(keywords) == countof(values), "countof(keywords) == countof(values)");
    switch (PQpingParams(keywords, values, false)) {
        case PQPING_NO_ATTEMPT: E("%s:%s PQpingParams == PQPING_NO_ATTEMPT", PQhost(backend->conn), PQport(backend->conn)); break;
        case PQPING_NO_RESPONSE: W("%s:%s PQpingParams == PQPING_NO_RESPONSE", PQhost(backend->conn), PQport(backend->conn)); return;
        case PQPING_OK: D1("%s:%s PQpingParams == PQPING_OK", PQhost(backend->conn), PQport(backend->conn)); break;
        case PQPING_REJECT: W("%s:%s PQpingParams == PQPING_REJECT", PQhost(backend->conn), PQport(backend->conn)); return;
    }
    if (!(PQresetStart(backend->conn))) E("%s:%s !PQresetStart and %s", PQhost(backend->conn), PQport(backend->conn), PQerrorMessage(backend->conn));
    if (PQstatus(backend->conn) == CONNECTION_BAD) E("%s:%s PQstatus == CONNECTION_BAD and %s", PQhost(backend->conn), PQport(backend->conn), PQerrorMessage(backend->conn));
    if (!PQisnonblocking(backend->conn) && PQsetnonblocking(backend->conn, true) == -1) E("%s:%s PQsetnonblocking == -1 and %s", PQhost(backend->conn), PQport(backend->conn), PQerrorMessage(backend->conn));
    if (PQclientEncoding(backend->conn) != GetDatabaseEncoding()) PQsetClientEncoding(backend->conn, GetDatabaseEncodingName());
    backend->callback = backend_reset_callback;
    backend->events = WL_SOCKET_WRITEABLE;
}

static void backend_connect_callback(Backend *backend) {
    bool connected = false;
    switch (PQstatus(backend->conn)) {
        case CONNECTION_AUTH_OK: D1("%s:%s PQstatus == CONNECTION_AUTH_OK", PQhost(backend->conn), PQport(backend->conn)); break;
        case CONNECTION_AWAITING_RESPONSE: D1("%s:%s PQstatus == CONNECTION_AWAITING_RESPONSE", PQhost(backend->conn), PQport(backend->conn)); break;
        case CONNECTION_BAD: E("%s:%s PQstatus == CONNECTION_BAD and %s", PQhost(backend->conn), PQport(backend->conn), PQerrorMessage(backend->conn)); break;
#if (PG_VERSION_NUM >= 130000)
        case CONNECTION_CHECK_TARGET: D1("%s:%s PQstatus == CONNECTION_CHECK_TARGET", PQhost(backend->conn), PQport(backend->conn)); break;
#endif
        case CONNECTION_CHECK_WRITABLE: D1("%s:%s PQstatus == CONNECTION_CHECK_WRITABLE", PQhost(backend->conn), PQport(backend->conn)); break;
        case CONNECTION_CONSUME: D1("%s:%s PQstatus == CONNECTION_CONSUME", PQhost(backend->conn), PQport(backend->conn)); break;
        case CONNECTION_GSS_STARTUP: D1("%s:%s PQstatus == CONNECTION_GSS_STARTUP", PQhost(backend->conn), PQport(backend->conn)); break;
        case CONNECTION_MADE: D1("%s:%s PQstatus == CONNECTION_MADE", PQhost(backend->conn), PQport(backend->conn)); break;
        case CONNECTION_NEEDED: D1("%s:%s PQstatus == CONNECTION_NEEDED", PQhost(backend->conn), PQport(backend->conn)); break;
        case CONNECTION_OK: D1("%s:%s PQstatus == CONNECTION_OK", PQhost(backend->conn), PQport(backend->conn)); connected = true; break;
        case CONNECTION_SETENV: D1("%s:%s PQstatus == CONNECTION_SETENV", PQhost(backend->conn), PQport(backend->conn)); break;
        case CONNECTION_SSL_STARTUP: D1("%s:%s PQstatus == CONNECTION_SSL_STARTUP", PQhost(backend->conn), PQport(backend->conn)); break;
        case CONNECTION_STARTED: D1("%s:%s PQstatus == CONNECTION_STARTED", PQhost(backend->conn), PQport(backend->conn)); break;
    }
    switch (PQconnectPoll(backend->conn)) {
        case PGRES_POLLING_ACTIVE: D1("%s:%s PQconnectPoll == PGRES_POLLING_ACTIVE", PQhost(backend->conn), PQport(backend->conn)); break;
        case PGRES_POLLING_FAILED: E("%s:%s PQconnectPoll == PGRES_POLLING_FAILED and %s", PQhost(backend->conn), PQport(backend->conn), PQerrorMessage(backend->conn)); break;
        case PGRES_POLLING_OK: D1("%s:%s PQconnectPoll == PGRES_POLLING_OK", PQhost(backend->conn), PQport(backend->conn)); connected = true; break;
        case PGRES_POLLING_READING: D1("%s:%s PQconnectPoll == PGRES_POLLING_READING", PQhost(backend->conn), PQport(backend->conn)); backend->events = WL_SOCKET_READABLE; break;
        case PGRES_POLLING_WRITING: D1("%s:%s PQconnectPoll == PGRES_POLLING_WRITING", PQhost(backend->conn), PQport(backend->conn)); backend->events = WL_SOCKET_WRITEABLE; break;
    }
    if (connected && backend->after) backend->after(backend);
}

void backend_connect(Backend *backend, const char *host, int port, const char *user, const char *dbname, callback_t after) {
    char *cport = backend_int2char(port);
    const char *keywords[] = {"host", "port", "user", "dbname", "application_name", NULL};
    const char *values[] = {host, cport, user, dbname, "pg_save", NULL};
    StaticAssertStmt(countof(keywords) == countof(values), "countof(keywords) == countof(values)");
    D1("host = %s, port = %i, user = %s, dbname = %s", host, port, user, dbname);
    switch (PQpingParams(keywords, values, false)) {
        case PQPING_NO_ATTEMPT: E("%s:%s PQpingParams == PQPING_NO_ATTEMPT", host, cport); break;
        case PQPING_NO_RESPONSE: E("%s:%s PQpingParams == PQPING_NO_RESPONSE", host, cport); break;
        case PQPING_OK: D1("%s:%s PQpingParams == PQPING_OK", host, cport); break;
        case PQPING_REJECT: E("%s:%s PQpingParams == PQPING_REJECT", host, cport); break;
    }
    if (!(backend->conn = PQconnectStartParams(keywords, values, false))) E("%s:%s !PQconnectStartParams and %s", PQhost(backend->conn), PQport(backend->conn), PQerrorMessage(backend->conn));
    pfree(cport);
    if (PQstatus(backend->conn) == CONNECTION_BAD) E("%s:%s PQstatus == CONNECTION_BAD and %s", PQhost(backend->conn), PQport(backend->conn), PQerrorMessage(backend->conn));
    if (!PQisnonblocking(backend->conn) && PQsetnonblocking(backend->conn, true) == -1) E("%s:%s PQsetnonblocking == -1 and %s", PQhost(backend->conn), PQport(backend->conn), PQerrorMessage(backend->conn));
    if (PQclientEncoding(backend->conn) != GetDatabaseEncoding()) PQsetClientEncoding(backend->conn, GetDatabaseEncodingName());
    backend->after = after;
    backend->callback = backend_connect_callback;
    backend->events = WL_SOCKET_WRITEABLE;
    queue_insert_tail(&backend_queue, &backend->queue);
}

void backend_fini(void) {
    queue_each(&backend_queue, queue) {
        Backend *backend = queue_data(queue, Backend, queue);
        backend_finish(backend);
    }
}
