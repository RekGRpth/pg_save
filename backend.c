#include "include.h"

extern char *hostname;
extern int init_attempt;
extern queue_t backend_queue;

static void backend_connected(Backend *backend) {
    return RecoveryInProgress() ? standby_connected(backend) : primary_connected(backend);
}

static void backend_connect_or_reset_socket(Backend *backend, PostgresPollingStatusType (*poll) (PGconn *conn)) {
    switch (PQstatus(backend->conn)) {
        case CONNECTION_AUTH_OK: D1("%s:%s/%s CONNECTION_AUTH_OK", backend->host, backend->port, backend->state); break;
        case CONNECTION_AWAITING_RESPONSE: D1("%s:%s/%s CONNECTION_AWAITING_RESPONSE", backend->host, backend->port, backend->state); break;
        case CONNECTION_BAD: E("%s:%s/%s CONNECTION_BAD and %s", backend->host, backend->port, backend->state, PQerrorMessage(backend->conn)); break;
#if (PG_VERSION_NUM >= 130000)
        case CONNECTION_CHECK_TARGET: D1("%s:%s/%s CONNECTION_CHECK_TARGET", backend->host, backend->port, backend->state); break;
#endif
        case CONNECTION_CHECK_WRITABLE: D1("%s:%s/%s CONNECTION_CHECK_WRITABLE", backend->host, backend->port, backend->state); break;
        case CONNECTION_CONSUME: D1("%s:%s/%s CONNECTION_CONSUME", backend->host, backend->port, backend->state); break;
        case CONNECTION_GSS_STARTUP: D1("%s:%s/%s CONNECTION_GSS_STARTUP", backend->host, backend->port, backend->state); break;
        case CONNECTION_MADE: D1("%s:%s/%s CONNECTION_MADE", backend->host, backend->port, backend->state); break;
        case CONNECTION_NEEDED: D1("%s:%s/%s CONNECTION_NEEDED", backend->host, backend->port, backend->state); break;
        case CONNECTION_OK: D1("%s:%s/%s CONNECTION_OK", backend->host, backend->port, backend->state); backend_connected(backend); return;
        case CONNECTION_SETENV: D1("%s:%s/%s CONNECTION_SETENV", backend->host, backend->port, backend->state); break;
        case CONNECTION_SSL_STARTUP: D1("%s:%s/%s CONNECTION_SSL_STARTUP", backend->host, backend->port, backend->state); break;
        case CONNECTION_STARTED: D1("%s:%s/%s CONNECTION_STARTED", backend->host, backend->port, backend->state); break;
    }
    switch (poll(backend->conn)) {
        case PGRES_POLLING_ACTIVE: D1("%s:%s/%s PGRES_POLLING_ACTIVE", backend->host, backend->port, backend->state); break;
        case PGRES_POLLING_FAILED: E("%s:%s/%s PGRES_POLLING_FAILED and %s", backend->host, backend->port, backend->state, PQerrorMessage(backend->conn)); break;
        case PGRES_POLLING_OK: D1("%s:%s/%s PGRES_POLLING_OK", backend->host, backend->port, backend->state); backend_connected(backend); return;
        case PGRES_POLLING_READING: D1("%s:%s/%s PGRES_POLLING_READING", backend->host, backend->port, backend->state); backend->events = WL_SOCKET_READABLE; break;
        case PGRES_POLLING_WRITING: D1("%s:%s/%s PGRES_POLLING_WRITING", backend->host, backend->port, backend->state); backend->events = WL_SOCKET_WRITEABLE; break;
    }
}

static void backend_connect_socket(Backend *backend) {
    backend_connect_or_reset_socket(backend, PQconnectPoll);
}

static void backend_reset_socket(Backend *backend) {
    backend_connect_or_reset_socket(backend, PQresetPoll);
}

static void backend_reseted(Backend *backend) {
    return RecoveryInProgress() ? standby_reseted(backend) : primary_reseted(backend);
}

static void backend_connect_or_reset(Backend *backend) {
    const char *keywords[] = {"host", "port", "user", "dbname", "application_name", NULL};
    const char *values[] = {backend->host, backend->port, backend->user, backend->data, hostname, NULL};
    StaticAssertStmt(countof(keywords) == countof(values), "countof(keywords) == countof(values)");
    switch (PQpingParams(keywords, values, false)) {
        case PQPING_NO_ATTEMPT: E("%s:%s/%s PQPING_NO_ATTEMPT", backend->host, backend->port, backend->state); break;
        case PQPING_NO_RESPONSE: W("%s:%s/%s PQPING_NO_RESPONSE and %i < %i", backend->host, backend->port, backend->state, backend->attempt, init_attempt); backend_reseted(backend); return;
        case PQPING_OK: D1("%s:%s/%s PQPING_OK", backend->host, backend->port, backend->state); break;
        case PQPING_REJECT: W("%s:%s/%s PQPING_REJECT and %i < %i", backend->host, backend->port, backend->state, backend->attempt, init_attempt); backend_reseted(backend); return;
    }
    if (!backend->conn) {
        if (!(backend->conn = PQconnectStartParams(keywords, values, false))) E("%s:%s/%s !PQconnectStartParams and %s", backend->host, backend->port, backend->state, PQerrorMessage(backend->conn));
        backend->socket = backend_connect_socket;
    } else {
        if (!(PQresetStart(backend->conn))) E("%s:%s/%s !PQresetStart and %s", backend->host, backend->port, backend->state, PQerrorMessage(backend->conn));
        backend->socket = backend_reset_socket;
    }
    if (PQstatus(backend->conn) == CONNECTION_BAD) E("%s:%s/%s PQstatus == CONNECTION_BAD and %s", backend->host, backend->port, backend->state, PQerrorMessage(backend->conn));
    if (!PQisnonblocking(backend->conn) && PQsetnonblocking(backend->conn, true) == -1) E("%s:%s/%s PQsetnonblocking == -1 and %s", backend->host, backend->port, backend->state, PQerrorMessage(backend->conn));
    if (PQclientEncoding(backend->conn) != GetDatabaseEncoding()) PQsetClientEncoding(backend->conn, GetDatabaseEncodingName());
    backend->events = WL_SOCKET_WRITEABLE;
}

void backend_connect(const char *host, const char *port, const char *user, const char *data, const char *state, const char *name) {
    Backend *backend = MemoryContextAllocZero(TopMemoryContext, sizeof(*backend));
    backend->data = MemoryContextStrdup(TopMemoryContext, data);
    backend->host = MemoryContextStrdup(TopMemoryContext, host);
    backend->name = name ? MemoryContextStrdup(TopMemoryContext, name) : NULL;
    backend->port = MemoryContextStrdup(TopMemoryContext, port);
    backend->state = MemoryContextStrdup(TopMemoryContext, state);
    backend->user = MemoryContextStrdup(TopMemoryContext, user);
    backend_connect_or_reset(backend);
    queue_insert_tail(&backend_queue, &backend->queue);
}

static void backend_finished(Backend *backend) {
    return RecoveryInProgress() ? standby_finished(backend) : primary_finished(backend);
}

void backend_finish(Backend *backend) {
    D1("%s:%s/%s", backend->host, backend->port, backend->state);
    queue_remove(&backend->queue);
    backend_finished(backend);
    PQfinish(backend->conn);
    pfree(backend->data);
    pfree(backend->host);
    if (backend->name) pfree(backend->name);
    pfree(backend->port);
    pfree(backend->state);
    pfree(backend->user);
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
        default: D1("%s:%s/%s PQresultStatus = %s and %s", backend->host, backend->port, backend->state, PQresStatus(PQresultStatus(result)), PQresultErrorMessage(result)); break;
    }
}

void backend_idle(Backend *backend) {
    backend->socket = backend_idle_socket;
}

void backend_reset(Backend *backend) {
    backend_connect_or_reset(backend);
}

static void backend_updated(Backend *backend) {
    return RecoveryInProgress() ? standby_updated(backend) : primary_updated(backend);
}

void backend_update(Backend *backend, const char *state, const char *name) {
    init_reset_state(backend->host);
    if (backend->name) pfree(backend->name);
    backend->name = name ? MemoryContextStrdup(TopMemoryContext, name) : NULL;
    pfree(backend->state);
    backend->state = MemoryContextStrdup(TopMemoryContext, state);
    backend_updated(backend);
}
