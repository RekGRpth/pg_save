#include "include.h"

extern char *hostname;
extern int init_attempt;
extern queue_t backend_queue;

static void backend_connected(Backend *backend) {
    D1("%s:%s", backend->host, backend->state);
    return RecoveryInProgress() ? standby_connected(backend) : primary_connected(backend);
}

static void backend_connect_or_reset_socket(Backend *backend, PostgresPollingStatusType (*poll) (PGconn *conn)) {
    switch (PQstatus(backend->conn)) {
        case CONNECTION_AUTH_OK: D1("%s:%s CONNECTION_AUTH_OK", backend->host, backend->state); break;
        case CONNECTION_AWAITING_RESPONSE: D1("%s:%s CONNECTION_AWAITING_RESPONSE", backend->host, backend->state); break;
        case CONNECTION_BAD: W("%s:%s CONNECTION_BAD and %s", backend->host, backend->state, PQerrorMessage(backend->conn)); backend_finish(backend); break;
#if (PG_VERSION_NUM >= 130000)
        case CONNECTION_CHECK_TARGET: D1("%s:%s CONNECTION_CHECK_TARGET", backend->host, backend->state); break;
#endif
        case CONNECTION_CHECK_WRITABLE: D1("%s:%s CONNECTION_CHECK_WRITABLE", backend->host, backend->state); break;
        case CONNECTION_CONSUME: D1("%s:%s CONNECTION_CONSUME", backend->host, backend->state); break;
        case CONNECTION_GSS_STARTUP: D1("%s:%s CONNECTION_GSS_STARTUP", backend->host, backend->state); break;
        case CONNECTION_MADE: D1("%s:%s CONNECTION_MADE", backend->host, backend->state); break;
        case CONNECTION_NEEDED: D1("%s:%s CONNECTION_NEEDED", backend->host, backend->state); break;
        case CONNECTION_OK: D1("%s:%s CONNECTION_OK", backend->host, backend->state); backend_connected(backend); return;
        case CONNECTION_SETENV: D1("%s:%s CONNECTION_SETENV", backend->host, backend->state); break;
        case CONNECTION_SSL_STARTUP: D1("%s:%s CONNECTION_SSL_STARTUP", backend->host, backend->state); break;
        case CONNECTION_STARTED: D1("%s:%s CONNECTION_STARTED", backend->host, backend->state); break;
    }
    switch (poll(backend->conn)) {
        case PGRES_POLLING_ACTIVE: D1("%s:%s PGRES_POLLING_ACTIVE", backend->host, backend->state); break;
        case PGRES_POLLING_FAILED: W("%s:%s PGRES_POLLING_FAILED and %s", backend->host, backend->state, PQerrorMessage(backend->conn)); backend_finish(backend); break;
        case PGRES_POLLING_OK: D1("%s:%s PGRES_POLLING_OK", backend->host, backend->state); backend_connected(backend); return;
        case PGRES_POLLING_READING: D1("%s:%s PGRES_POLLING_READING", backend->host, backend->state); backend->events = WL_SOCKET_READABLE; break;
        case PGRES_POLLING_WRITING: D1("%s:%s PGRES_POLLING_WRITING", backend->host, backend->state); backend->events = WL_SOCKET_WRITEABLE; break;
    }
}

static void backend_connect_socket(Backend *backend) {
    backend_connect_or_reset_socket(backend, PQconnectPoll);
}

static void backend_reset_socket(Backend *backend) {
    backend_connect_or_reset_socket(backend, PQresetPoll);
}

static void backend_reseted(Backend *backend) {
    if (backend->attempt++ < init_attempt) return;
    D1("%s:%s", backend->host, backend->state);
    return RecoveryInProgress() ? standby_reseted(backend) : primary_reseted(backend);
}

static void backend_connect_or_reset(Backend *backend) {
    const char *keywords[] = {"host", "port", "user", "dbname", "application_name", NULL};
    const char *values[] = {backend->host, getenv("PGPORT") ? getenv("PGPORT") : DEF_PGPORT_STR, MyProcPort->user_name, MyProcPort->database_name, hostname, NULL};
    StaticAssertStmt(countof(keywords) == countof(values), "countof(keywords) == countof(values)");
    switch (PQpingParams(keywords, values, false)) {
        case PQPING_NO_ATTEMPT: W("%s:%s PQPING_NO_ATTEMPT", backend->host, backend->state); backend_finish(backend); return;
        case PQPING_NO_RESPONSE: W("%s:%s PQPING_NO_RESPONSE and %i < %i", backend->host, backend->state, backend->attempt, init_attempt); backend_reseted(backend); return;
        case PQPING_OK: D1("%s:%s PQPING_OK", backend->host, backend->state); break;
        case PQPING_REJECT: W("%s:%s PQPING_REJECT and %i < %i", backend->host, backend->state, backend->attempt, init_attempt); backend_reseted(backend); return;
    }
    if (!backend->conn) {
        if (!(backend->conn = PQconnectStartParams(keywords, values, false))) { W("%s:%s !PQconnectStartParams and %s", backend->host, backend->state, PQerrorMessage(backend->conn)); backend_finish(backend); return; }
        backend->socket = backend_connect_socket;
    } else {
        if (!(PQresetStart(backend->conn))) { W("%s:%s !PQresetStart and %s", backend->host, backend->state, PQerrorMessage(backend->conn)); backend_finish(backend); return; }
        backend->socket = backend_reset_socket;
    }
    if (PQstatus(backend->conn) == CONNECTION_BAD) { W("%s:%s PQstatus == CONNECTION_BAD and %s", backend->host, backend->state, PQerrorMessage(backend->conn)); backend_finish(backend); return; }
    if (!PQisnonblocking(backend->conn) && PQsetnonblocking(backend->conn, true) == -1) { W("%s:%s PQsetnonblocking == -1 and %s", backend->host, backend->state, PQerrorMessage(backend->conn)); backend_finish(backend); return; }
    if (PQclientEncoding(backend->conn) != GetDatabaseEncoding()) PQsetClientEncoding(backend->conn, GetDatabaseEncodingName());
    backend->events = WL_SOCKET_WRITEABLE;
}

void backend_connect(const char *host, const char *state) {
    Backend *backend = MemoryContextAllocZero(TopMemoryContext, sizeof(*backend));
    backend->host = MemoryContextStrdup(TopMemoryContext, host);
    backend->state = MemoryContextStrdup(TopMemoryContext, state);
    backend_connect_or_reset(backend);
    queue_insert_tail(&backend_queue, &backend->queue);
}

static void backend_finished(Backend *backend) {
    D1("%s:%s", backend->host, backend->state);
    return RecoveryInProgress() ? standby_finished(backend) : primary_finished(backend);
}

void backend_finish(Backend *backend) {
    queue_remove(&backend->queue);
    init_reset_state(backend->host);
    backend_finished(backend);
    PQfinish(backend->conn);
    pfree(backend->host);
    pfree(backend->state);
    pfree(backend);
}

void backend_fini(void) {
    queue_each(&backend_queue, queue) {
        Backend *backend = queue_data(queue, Backend, queue);
        backend_finish(backend);
    }
}

static void backend_idle_socket(Backend *backend) {
    for (PGresult *result; (result = PQgetResult(backend->conn)); PQclear(result)) switch (PQresultStatus(result)) {
        default: D1("%s:%s PQresultStatus = %s and %s", backend->host, backend->state, PQresStatus(PQresultStatus(result)), PQresultErrorMessage(result)); break;
    }
}

void backend_idle(Backend *backend) {
    backend->socket = backend_idle_socket;
}

void backend_reset(Backend *backend) {
    backend_connect_or_reset(backend);
}

static void backend_updated(Backend *backend) {
    D1("%s:%s", backend->host, backend->state);
    init_set_state(backend->host, backend->state);
    return RecoveryInProgress() ? standby_updated(backend) : primary_updated(backend);
}

void backend_update(Backend *backend, const char *state) {
    init_reset_state(backend->host);
    pfree(backend->state);
    backend->state = MemoryContextStrdup(TopMemoryContext, state);
    backend_updated(backend);
}
