#include "include.h"

extern int init_attempt;
extern state_t init_state;
static queue_t backend_queue;

Backend *backend_host(const char *host) {
    if (host) queue_each(&backend_queue, queue) {
        Backend *backend = queue_data(queue, Backend, queue);
        if (!strcmp(host, PQhost(backend->conn))) return backend;
    }
    return NULL;
}

Backend *backend_state(state_t state) {
    if (state != state_unknown) queue_each(&backend_queue, queue) {
        Backend *backend = queue_data(queue, Backend, queue);
        if (backend->state == state) return backend;
    }
    return NULL;
}

int backend_nevents(void) {
    int nevents = 0;
    queue_each(&backend_queue, queue) {
        Backend *backend = queue_data(queue, Backend, queue);
        if (PQstatus(backend->conn) == CONNECTION_BAD) continue;
        if (PQsocket(backend->conn) < 0) continue;
        nevents++;
    }
    return nevents;
}

size_t backend_size(void) {
    return queue_size(&backend_queue);
}

static void backend_query_socket(Backend *backend) {
    bool ok = false;
    for (PGresult *result; (result = PQgetResult(backend->conn)); PQclear(result)) switch (PQresultStatus(result)) {
        case PGRES_COMMAND_OK: ok = true; break;
        default: W("%s:%s PQresultStatus = %s and %.*s", PQhost(backend->conn), init_state2char(backend->state), PQresStatus(PQresultStatus(result)), (int)strlen(PQresultErrorMessage(result)) - 1, PQresultErrorMessage(result)); break;
    }
    ok ? backend_idle(backend) : backend_finish(backend);
}

static void backend_query(Backend *backend) {
    const char *channel = PQhost(backend->conn);
    const char *channel_quote = quote_identifier(channel);
    StringInfoData buf;
    initStringInfoMy(TopMemoryContext, &buf);
    appendStringInfo(&buf, "LISTEN %s", channel_quote);
    if (channel_quote != channel) pfree((void *)channel_quote);
    if (PQisBusy(backend->conn)) backend->events = WL_SOCKET_READABLE; else if (!PQsendQuery(backend->conn, buf.data)) {
        W("%s:%s !PQsendQuery and %.*s", PQhost(backend->conn), init_state2char(backend->state), (int)strlen(PQerrorMessage(backend->conn)) - 1, PQerrorMessage(backend->conn));
        backend_finish(backend);
    } else {
        backend->events = WL_SOCKET_WRITEABLE;
        backend->socket = backend_query_socket;
    }
    pfree(buf.data);
}

static void backend_connected(Backend *backend) {
    D1("%s:%s", PQhost(backend->conn), init_state2char(backend->state));
    backend->attempt = 0;
    init_set_host(PQhost(backend->conn), backend->state);
    backend_query(backend);
    RecoveryInProgress() ? standby_connected(backend) : primary_connected(backend);
    init_reload();
}

static void backend_fail(Backend *backend) {
    if (backend->attempt++ < init_attempt) return;
    D1("%s:%s", PQhost(backend->conn), init_state2char(backend->state));
    RecoveryInProgress() ? standby_failed(backend) : primary_failed(backend);
    init_reload();
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

static void backend_create_socket(Backend *backend) {
    backend_connect_or_reset_socket(backend, PQconnectPoll);
}

static void backend_reset_socket(Backend *backend) {
    backend_connect_or_reset_socket(backend, PQresetPoll);
}

static void backend_connect_or_reset(Backend *backend, const char *host) {
    if (!backend->conn) {
        const char *keywords[] = {"host", "port", "user", "dbname", "application_name", "target_session_attrs", NULL};
        const char *values[] = {host, getenv("PGPORT") ? getenv("PGPORT") : DEF_PGPORT_STR, MyProcPort->user_name, MyProcPort->database_name, MyBgworkerEntry->bgw_type, backend->state <= state_primary ? "read-write" : "any", NULL};
        StaticAssertStmt(countof(keywords) == countof(values), "countof(keywords) == countof(values)");
        if (!(backend->conn = PQconnectStartParams(keywords, values, false))) { W("%s:%s !PQconnectStartParams and %i < %i and %.*s", PQhost(backend->conn), init_state2char(backend->state), backend->attempt, init_attempt, (int)strlen(PQerrorMessage(backend->conn)) - 1, PQerrorMessage(backend->conn)); backend_fail(backend); return; }
        backend->socket = backend_create_socket;
    } else {
        if (!(PQresetStart(backend->conn))) { W("%s:%s !PQresetStart and %i < %i and %.*s", PQhost(backend->conn), init_state2char(backend->state), backend->attempt, init_attempt, (int)strlen(PQerrorMessage(backend->conn)) - 1, PQerrorMessage(backend->conn)); backend_fail(backend); return; }
        backend->socket = backend_reset_socket;
    }
    if (PQstatus(backend->conn) == CONNECTION_BAD) { W("%s:%s PQstatus == CONNECTION_BAD and %.*s", PQhost(backend->conn), init_state2char(backend->state), (int)strlen(PQerrorMessage(backend->conn)) - 1, PQerrorMessage(backend->conn)); backend_finish(backend); return; }
    if (!PQisnonblocking(backend->conn) && PQsetnonblocking(backend->conn, true) == -1) { W("%s:%s PQsetnonblocking == -1 and %.*s", PQhost(backend->conn), init_state2char(backend->state), (int)strlen(PQerrorMessage(backend->conn)) - 1, PQerrorMessage(backend->conn)); backend_finish(backend); return; }
    if (PQclientEncoding(backend->conn) != GetDatabaseEncoding()) PQsetClientEncoding(backend->conn, GetDatabaseEncodingName());
    backend->events = WL_SOCKET_WRITEABLE;
}

static void backend_created(Backend *backend) {
    RecoveryInProgress() ? standby_created(backend) : primary_created(backend);
}

void backend_create(const char *host, state_t state) {
    Backend *backend;
    if (!strcmp(host, MyBgworkerEntry->bgw_type)) { W("backend with host \"%s\" is local!", host); return; }
    if ((backend = backend_host(host))) { W("backend with host \"%s\" already exists!", host); return; }
    backend = MemoryContextAllocZero(TopMemoryContext, sizeof(*backend));
    backend->state = state;
    queue_insert_tail(&backend_queue, &backend->queue);
    backend_connect_or_reset(backend, host);
    backend_created(backend);
}

void backend_event(WaitEventSet *set) {
    queue_each(&backend_queue, queue) {
        int fd;
        Backend *backend = queue_data(queue, Backend, queue);
        if (PQstatus(backend->conn) == CONNECTION_BAD) continue;
        if ((fd = PQsocket(backend->conn)) < 0) continue;
        if (backend->events & WL_SOCKET_WRITEABLE) switch (PQflush(backend->conn)) {
            case 0: /*D1("PQflush = 0");*/ break;
            case 1: D1("PQflush = 1"); break;
            default: D1("PQflush = default"); break;
        }
        AddWaitEventToSet(set, backend->events & WL_SOCKET_MASK, fd, NULL, backend);
    }
}

static void backend_finished(Backend *backend) {
    D1("%s:%s", PQhost(backend->conn), init_state2char(backend->state));
    RecoveryInProgress() ? standby_finished(backend) : primary_finished(backend);
    init_reload();
}

void backend_finish(Backend *backend) {
    queue_remove(&backend->queue);
    backend_finished(backend);
    PQfinish(backend->conn);
    pfree(backend);
}

void backend_fini(void) {
    queue_each(&backend_queue, queue) {
        Backend *backend = queue_data(queue, Backend, queue);
        backend_finish(backend);
    }
    RecoveryInProgress() ? standby_fini() : primary_fini();
}

static void backend_notify(Backend *backend, state_t state) {
    D1("%s:%s state = %s", PQhost(backend->conn), init_state2char(backend->state), init_state2char(state));
    RecoveryInProgress() ? standby_notify(backend, state) : primary_notify(backend, state);
}

static void backend_idle_socket(Backend *backend) {
    for (PGresult *result; (result = PQgetResult(backend->conn)); PQclear(result)) switch (PQresultStatus(result)) {
        default: D1("%s:%s PQresultStatus = %s and %.*s", PQhost(backend->conn), init_state2char(backend->state), PQresStatus(PQresultStatus(result)), (int)strlen(PQresultErrorMessage(result)) - 1, PQresultErrorMessage(result)); break;
    }
    for (PGnotify *notify; (notify = PQnotifies(backend->conn)); PQfreemem(notify)) if (MyProcPid != notify->be_pid) backend_notify(backend, init_char2state(notify->extra));
}

void backend_idle(Backend *backend) {
    backend->events = WL_SOCKET_READABLE;
    backend->socket = backend_idle_socket;
}

void backend_init(void) {
    queue_init(&backend_queue);
    RecoveryInProgress() ? standby_init() : primary_init();
    init_reload();
}

void backend_reset(Backend *backend) {
    backend_connect_or_reset(backend, NULL);
}

void backend_result(const char *host, state_t state) {
    Backend *backend = backend_host(host);
    if (RecoveryInProgress() && !strcmp(host, MyBgworkerEntry->bgw_type)) return standby_update(state);
    backend ? backend_update(backend, state) : backend_create(host, state);
}

void backend_socket(Backend *backend) {
    if (PQstatus(backend->conn) == CONNECTION_OK) {
        if (!PQconsumeInput(backend->conn)) { W("%s:%s !PQconsumeInput and %.*s", PQhost(backend->conn), init_state2char(backend->state), (int)strlen(PQerrorMessage(backend->conn)) - 1, PQerrorMessage(backend->conn)); return; }
        if (PQisBusy(backend->conn)) { backend->events = WL_SOCKET_READABLE; return; }
    }
    backend->socket(backend);
}

void backend_timeout(void) {
    queue_each(&backend_queue, queue) {
        Backend *backend = queue_data(queue, Backend, queue);
        if (PQstatus(backend->conn) == CONNECTION_BAD) backend_reset(backend);
    }
    RecoveryInProgress() ? standby_timeout() : primary_timeout();
    init_reload();
}

static void backend_updated(Backend *backend) {
    D1("%s:%s", PQhost(backend->conn), init_state2char(backend->state));
    RecoveryInProgress() ? standby_updated(backend) : primary_updated(backend);
    init_reload();
}

void backend_update(Backend *backend, state_t state) {
    if (backend->state == state) return;
    backend->state = state;
    init_set_host(PQhost(backend->conn), state);
    backend_updated(backend);
}
