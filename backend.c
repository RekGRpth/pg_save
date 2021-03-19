#include "include.h"

char *backend_save = NULL;
extern char *save_hostname;
extern int init_attempt;
extern queue_t save_queue;
extern STATE init_state;

void backend_array(void) {
    StringInfoData buf;
    int nelems = queue_size(&save_queue);
    if (backend_save) pfree(backend_save);
    backend_save = NULL;
    if (!nelems) return;
    initStringInfoMy(TopMemoryContext, &buf);
    appendStringInfoString(&buf, "{");
    nelems = 0;
    queue_each(&save_queue, queue) {
        Backend *backend = queue_data(queue, Backend, queue);
        if (backend->state == PRIMARY) continue;
        if (nelems) appendStringInfoString(&buf, ",");
        appendStringInfo(&buf, "\"(%s,%s)\"", PQhost(backend->conn), init_state2char(backend->state));
        nelems++;
    }
    if (init_state != UNKNOWN && init_state != PRIMARY) {
        if (nelems) appendStringInfoString(&buf, ",");
        appendStringInfo(&buf, "\"(%s,%s)\"", save_hostname, init_state2char(init_state));
    }
    appendStringInfoString(&buf, "}");
    backend_save = buf.data;
    D1("save = %s", backend_save);
}

static void backend_connected(Backend *backend) {
    D1("%s:%s", PQhost(backend->conn), init_state2char(backend->state));
    backend->attempt = 0;
    init_set_host_state(PQhost(backend->conn), backend->state);
    RecoveryInProgress() ? standby_connected(backend) : primary_connected(backend);
    init_reload();
    if (backend->state != PRIMARY) backend_array();
}

static void backend_connect_or_reset_socket(Backend *backend, PostgresPollingStatusType (*poll) (PGconn *conn)) {
    switch (PQstatus(backend->conn)) {
        case CONNECTION_AUTH_OK: D1("%s:%s CONNECTION_AUTH_OK", PQhost(backend->conn), init_state2char(backend->state)); break;
        case CONNECTION_AWAITING_RESPONSE: D1("%s:%s CONNECTION_AWAITING_RESPONSE", PQhost(backend->conn), init_state2char(backend->state)); break;
        case CONNECTION_BAD: W("%s:%s CONNECTION_BAD and %i < %i and %.*s", PQhost(backend->conn), init_state2char(backend->state), backend->attempt, init_attempt, (int)strlen(PQerrorMessage(backend->conn)) - 1, PQerrorMessage(backend->conn)); backend_fail(backend); return;
#if (PG_VERSION_NUM >= 130000)
        case CONNECTION_CHECK_TARGET: D1("%s:%s CONNECTION_CHECK_TARGET", PQhost(backend->conn), init_state2char(backend->state)); break;
#endif
        case CONNECTION_CHECK_WRITABLE: D1("%s:%s CONNECTION_CHECK_WRITABLE", PQhost(backend->conn), init_state2char(backend->state)); break;
        case CONNECTION_CONSUME: D1("%s:%s CONNECTION_CONSUME", PQhost(backend->conn), init_state2char(backend->state)); break;
        case CONNECTION_GSS_STARTUP: D1("%s:%s CONNECTION_GSS_STARTUP", PQhost(backend->conn), init_state2char(backend->state)); break;
        case CONNECTION_MADE: D1("%s:%s CONNECTION_MADE", PQhost(backend->conn), init_state2char(backend->state)); break;
        case CONNECTION_NEEDED: D1("%s:%s CONNECTION_NEEDED", PQhost(backend->conn), init_state2char(backend->state)); break;
        case CONNECTION_OK: D1("%s:%s CONNECTION_OK", PQhost(backend->conn), init_state2char(backend->state)); backend_connected(backend); return;
        case CONNECTION_SETENV: D1("%s:%s CONNECTION_SETENV", PQhost(backend->conn), init_state2char(backend->state)); break;
        case CONNECTION_SSL_STARTUP: D1("%s:%s CONNECTION_SSL_STARTUP", PQhost(backend->conn), init_state2char(backend->state)); break;
        case CONNECTION_STARTED: D1("%s:%s CONNECTION_STARTED", PQhost(backend->conn), init_state2char(backend->state)); break;
    }
    switch (poll(backend->conn)) {
        case PGRES_POLLING_ACTIVE: D1("%s:%s PGRES_POLLING_ACTIVE", PQhost(backend->conn), init_state2char(backend->state)); break;
        case PGRES_POLLING_FAILED: W("%s:%s PGRES_POLLING_FAILED and %i < %i and %.*s", PQhost(backend->conn), init_state2char(backend->state), backend->attempt, init_attempt, (int)strlen(PQerrorMessage(backend->conn)) - 1, PQerrorMessage(backend->conn)); backend_fail(backend); return;
        case PGRES_POLLING_OK: D1("%s:%s PGRES_POLLING_OK", PQhost(backend->conn), init_state2char(backend->state)); backend_connected(backend); return;
        case PGRES_POLLING_READING: D1("%s:%s PGRES_POLLING_READING", PQhost(backend->conn), init_state2char(backend->state)); backend->events = WL_SOCKET_READABLE; break;
        case PGRES_POLLING_WRITING: D1("%s:%s PGRES_POLLING_WRITING", PQhost(backend->conn), init_state2char(backend->state)); backend->events = WL_SOCKET_WRITEABLE; break;
    }
}

static void backend_connect_socket(Backend *backend) {
    backend_connect_or_reset_socket(backend, PQconnectPoll);
}

static void backend_reset_socket(Backend *backend) {
    backend_connect_or_reset_socket(backend, PQresetPoll);
}

static void backend_connect_or_reset(Backend *backend, const char *host) {
    if (!backend->conn) {
        const char *keywords[] = {"host", "port", "user", "dbname", "application_name", NULL};
        const char *values[] = {host, getenv("PGPORT") ? getenv("PGPORT") : DEF_PGPORT_STR, MyProcPort->user_name, MyProcPort->database_name, save_hostname, NULL};
        StaticAssertStmt(countof(keywords) == countof(values), "countof(keywords) == countof(values)");
        if (!(backend->conn = PQconnectStartParams(keywords, values, false))) { W("%s:%s !PQconnectStartParams and %i < %i and %.*s", PQhost(backend->conn), init_state2char(backend->state), backend->attempt, init_attempt, (int)strlen(PQerrorMessage(backend->conn)) - 1, PQerrorMessage(backend->conn)); backend_fail(backend); return; }
        backend->socket = backend_connect_socket;
    } else {
        if (!(PQresetStart(backend->conn))) { W("%s:%s !PQresetStart and %i < %i and %.*s", PQhost(backend->conn), init_state2char(backend->state), backend->attempt, init_attempt, (int)strlen(PQerrorMessage(backend->conn)) - 1, PQerrorMessage(backend->conn)); backend_fail(backend); return; }
        backend->socket = backend_reset_socket;
    }
    if (PQstatus(backend->conn) == CONNECTION_BAD) { W("%s:%s PQstatus == CONNECTION_BAD and %.*s", PQhost(backend->conn), init_state2char(backend->state), (int)strlen(PQerrorMessage(backend->conn)) - 1, PQerrorMessage(backend->conn)); backend_finish(backend); return; }
    if (!PQisnonblocking(backend->conn) && PQsetnonblocking(backend->conn, true) == -1) { W("%s:%s PQsetnonblocking == -1 and %.*s", PQhost(backend->conn), init_state2char(backend->state), (int)strlen(PQerrorMessage(backend->conn)) - 1, PQerrorMessage(backend->conn)); backend_finish(backend); return; }
    if (PQclientEncoding(backend->conn) != GetDatabaseEncoding()) PQsetClientEncoding(backend->conn, GetDatabaseEncodingName());
    backend->events = WL_SOCKET_WRITEABLE;
}

void backend_connect(const char *host, STATE state) {
    Backend *backend = MemoryContextAllocZero(TopMemoryContext, sizeof(*backend));
    backend->state = state;
    backend_connect_or_reset(backend, host);
    queue_insert_tail(&save_queue, &backend->queue);
}

static void backend_finished(Backend *backend) {
    D1("%s:%s", PQhost(backend->conn), init_state2char(backend->state));
    init_reset_host_state(PQhost(backend->conn), backend->state);
    RecoveryInProgress() ? standby_finished(backend) : primary_finished(backend);
    init_reload();
}

void backend_fail(Backend *backend) {
    if (backend->attempt++ < init_attempt) return;
    D1("%s:%s", PQhost(backend->conn), init_state2char(backend->state));
    RecoveryInProgress() ? standby_failed(backend) : primary_failed(backend);
    init_reload();
}

void backend_finish(Backend *backend) {
    queue_remove(&backend->queue);
    backend_finished(backend);
    PQfinish(backend->conn);
    if (backend->state != PRIMARY) backend_array();
    pfree(backend);
}

void backend_fini(void) {
    queue_each(&save_queue, queue) {
        Backend *backend = queue_data(queue, Backend, queue);
        backend_finish(backend);
    }
    RecoveryInProgress() ? standby_fini() : primary_fini();
}

void backend_init(void) {
    RecoveryInProgress() ? standby_init() : primary_init();
    etcd_init();
    init_reload();
}

static void backend_idle_socket(Backend *backend) {
    for (PGresult *result; (result = PQgetResult(backend->conn)); PQclear(result)) switch (PQresultStatus(result)) {
        default: D1("%s:%s PQresultStatus = %s and %.*s", PQhost(backend->conn), init_state2char(backend->state), PQresStatus(PQresultStatus(result)), (int)strlen(PQresultErrorMessage(result)) - 1, PQresultErrorMessage(result)); break;
    }
}

void backend_idle(Backend *backend) {
    backend->socket = backend_idle_socket;
}

void backend_reset(Backend *backend) {
    backend_connect_or_reset(backend, NULL);
}

void backend_result(const char *host, STATE state) {
    Backend *backend = NULL;
    D1("state = %s, host = %s", init_state2char(state), host);
    queue_each(&save_queue, queue) {
        Backend *backend_ = queue_data(queue, Backend, queue);
        if (!strcmp(host, PQhost(backend_->conn))) { backend = backend_; break; }
    }
    if (backend) {
        if (state != UNKNOWN) backend_update(backend, state);
        else backend_fail(backend);
    } else if (state != UNKNOWN) {
        backend_connect(host, state);
    }
}

void backend_timeout(void) {
    etcd_timeout();
    queue_each(&save_queue, queue) {
        Backend *backend = queue_data(queue, Backend, queue);
        if (PQstatus(backend->conn) == CONNECTION_BAD) backend_reset(backend);
    }
    RecoveryInProgress() ? standby_timeout() : primary_timeout();
}

static void backend_updated(Backend *backend) {
    D1("%s:%s", PQhost(backend->conn), init_state2char(backend->state));
    RecoveryInProgress() ? standby_updated(backend) : primary_updated(backend);
    init_reload();
}

void backend_update(Backend *backend, STATE state) {
    init_reset_host_state(PQhost(backend->conn), backend->state);
    backend->state = state;
    init_set_host_state(PQhost(backend->conn), backend->state);
    backend_updated(backend);
    if (backend->state != PRIMARY) backend_array();
}
