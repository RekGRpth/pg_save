#include "include.h"

extern char *hostname;
extern int init_probe;
extern queue_t backend_queue;

static void backend_connected(Backend *backend) {
    return RecoveryInProgress() ? standby_connected(backend) : primary_connected(backend);
}

static void backend_reseted(Backend *backend) {
    return RecoveryInProgress() ? standby_reseted(backend) : primary_reseted(backend);
}

static void backend_finished(Backend *backend) {
    return RecoveryInProgress() ? standby_finished(backend) : primary_finished(backend);
}

const char *backend_state(Backend *backend) {
    return backend->state ? backend->state : "primary";
}

const char *backend_host(Backend *backend) {
    return PQhost(backend->conn);
}

const char *backend_hostaddr(Backend *backend) {
    return PQhostaddr(backend->conn);
}

const char *backend_port(Backend *backend) {
    return PQport(backend->conn);
}

static void backend_idle_socket(Backend *backend) {
    for (PGresult *result; (result = PQgetResult(backend->conn)); PQclear(result)) switch (PQresultStatus(result)) {
        default: D1("%s:%s/%s PQresultStatus = %s and %s", backend_host(backend), backend_port(backend), backend_state(backend), PQresStatus(PQresultStatus(result)), PQresultErrorMessage(result)); break;
    }
}

void backend_idle(Backend *backend) {
    backend->socket = backend_idle_socket;
}

void backend_finish(Backend *backend) {
    D1("%s:%s/%s", backend_host(backend), backend_port(backend), backend_state(backend));
    queue_remove(&backend->queue);
    backend_finished(backend);
    PQfinish(backend->conn);
    if (backend->name) pfree(backend->name);
    if (backend->state) pfree(backend->state);
    pfree(backend);
}

static void backend_connect_or_reset_socket(Backend *backend) {
    bool connected = false;
    switch (PQstatus(backend->conn)) {
        case CONNECTION_AUTH_OK: D1("%s:%s/%s CONNECTION_AUTH_OK", backend_host(backend), backend_port(backend), backend_state(backend)); break;
        case CONNECTION_AWAITING_RESPONSE: D1("%s:%s/%s CONNECTION_AWAITING_RESPONSE", backend_host(backend), backend_port(backend), backend_state(backend)); break;
        case CONNECTION_BAD: E("%s:%s/%s CONNECTION_BAD and %s", backend_host(backend), backend_port(backend), backend_state(backend), PQerrorMessage(backend->conn)); break;
#if (PG_VERSION_NUM >= 130000)
        case CONNECTION_CHECK_TARGET: D1("%s:%s/%s CONNECTION_CHECK_TARGET", backend_host(backend), backend_port(backend), backend_state(backend)); break;
#endif
        case CONNECTION_CHECK_WRITABLE: D1("%s:%s/%s CONNECTION_CHECK_WRITABLE", backend_host(backend), backend_port(backend), backend_state(backend)); break;
        case CONNECTION_CONSUME: D1("%s:%s/%s CONNECTION_CONSUME", backend_host(backend), backend_port(backend), backend_state(backend)); break;
        case CONNECTION_GSS_STARTUP: D1("%s:%s/%s CONNECTION_GSS_STARTUP", backend_host(backend), backend_port(backend), backend_state(backend)); break;
        case CONNECTION_MADE: D1("%s:%s/%s CONNECTION_MADE", backend_host(backend), backend_port(backend), backend_state(backend)); break;
        case CONNECTION_NEEDED: D1("%s:%s/%s CONNECTION_NEEDED", backend_host(backend), backend_port(backend), backend_state(backend)); break;
        case CONNECTION_OK: D1("%s:%s/%s CONNECTION_OK", backend_host(backend), backend_port(backend), backend_state(backend)); connected = true; break;
        case CONNECTION_SETENV: D1("%s:%s/%s CONNECTION_SETENV", backend_host(backend), backend_port(backend), backend_state(backend)); break;
        case CONNECTION_SSL_STARTUP: D1("%s:%s/%s CONNECTION_SSL_STARTUP", backend_host(backend), backend_port(backend), backend_state(backend)); break;
        case CONNECTION_STARTED: D1("%s:%s/%s CONNECTION_STARTED", backend_host(backend), backend_port(backend), backend_state(backend)); break;
    }
    if (!connected) switch (backend->poll(backend->conn)) {
        case PGRES_POLLING_ACTIVE: D1("%s:%s/%s PGRES_POLLING_ACTIVE", backend_host(backend), backend_port(backend), backend_state(backend)); break;
        case PGRES_POLLING_FAILED: E("%s:%s/%s PGRES_POLLING_FAILED and %s", backend_host(backend), backend_port(backend), backend_state(backend), PQerrorMessage(backend->conn)); break;
        case PGRES_POLLING_OK: D1("%s:%s/%s PGRES_POLLING_OK", backend_host(backend), backend_port(backend), backend_state(backend)); connected = true; break;
        case PGRES_POLLING_READING: D1("%s:%s/%s PGRES_POLLING_READING", backend_host(backend), backend_port(backend), backend_state(backend)); backend->events = WL_SOCKET_READABLE; break;
        case PGRES_POLLING_WRITING: D1("%s:%s/%s PGRES_POLLING_WRITING", backend_host(backend), backend_port(backend), backend_state(backend)); backend->events = WL_SOCKET_WRITEABLE; break;
    }
    if (connected) backend_connected(backend);
}

static bool backend_connect_or_reset(Backend *backend, const char *host, const char *port, const char *user, const char *dbname) {
    const char *keywords[] = {"host", "port", "user", "dbname", "application_name", NULL};
    const char *values[] = {host, port, user, dbname, hostname, NULL};
    StaticAssertStmt(countof(keywords) == countof(values), "countof(keywords) == countof(values)");
    switch (PQpingParams(keywords, values, false)) {
        case PQPING_NO_ATTEMPT: E("%s:%s/%s PQPING_NO_ATTEMPT", host, port, backend_state(backend)); break;
        case PQPING_NO_RESPONSE: W("%s:%s/%s PQPING_NO_RESPONSE and %i < %i", host, port, backend_state(backend), backend->probe, init_probe); backend_reseted(backend); return false;
        case PQPING_OK: D1("%s:%s/%s PQPING_OK", host, port, backend_state(backend)); break;
        case PQPING_REJECT: W("%s:%s/%s PQPING_REJECT and %i < %i", host, port, backend_state(backend), backend->probe, init_probe); backend_reseted(backend); return false;
    }
    if (!backend->conn) {
        if (!(backend->conn = PQconnectStartParams(keywords, values, false))) E("%s:%s/%s !PQconnectStartParams and %s", backend_host(backend), backend_port(backend), backend_state(backend), PQerrorMessage(backend->conn));
    } else {
        if (!(PQresetStart(backend->conn))) E("%s:%s/%s !PQresetStart and %s", backend_host(backend), backend_port(backend), backend_state(backend), PQerrorMessage(backend->conn));
    }
    if (PQstatus(backend->conn) == CONNECTION_BAD) E("%s:%s/%s PQstatus == CONNECTION_BAD and %s", backend_host(backend), backend_port(backend), backend_state(backend), PQerrorMessage(backend->conn));
    if (!PQisnonblocking(backend->conn) && PQsetnonblocking(backend->conn, true) == -1) E("%s:%s/%s PQsetnonblocking == -1 and %s", backend_host(backend), backend_port(backend), backend_state(backend), PQerrorMessage(backend->conn));
    if (PQclientEncoding(backend->conn) != GetDatabaseEncoding()) PQsetClientEncoding(backend->conn, GetDatabaseEncodingName());
    return true;
}

void backend_reset(Backend *backend) {
    if (!backend_connect_or_reset(backend, backend_host(backend), backend_port(backend), PQuser(backend->conn), PQdb(backend->conn))) return;
    backend->poll = PQresetPoll;
    backend->socket = backend_connect_or_reset_socket;
    backend->events = WL_SOCKET_WRITEABLE;
}

void backend_connect(const char *host, const char *port, const char *user, const char *dbname, const char *state, const char *name) {
    Backend *backend = MemoryContextAllocZero(TopMemoryContext, sizeof(*backend));
    if (!backend_connect_or_reset(backend, host, port, user, dbname)) return;
    backend->events = WL_SOCKET_WRITEABLE;
    backend->name = name ? MemoryContextStrdup(TopMemoryContext, name) : NULL;
    backend->poll = PQconnectPoll;
    backend->socket = backend_connect_or_reset_socket;
    backend->state = state ? MemoryContextStrdup(TopMemoryContext, state) : NULL;
    queue_insert_tail(&backend_queue, &backend->queue);
}

void backend_fini(void) {
    queue_each(&backend_queue, queue) {
        Backend *backend = queue_data(queue, Backend, queue);
        backend_finish(backend);
    }
}
