#include "lib.h"

extern char *hostname;
extern int init_attempt;
extern state_t init_state;
static char *pgport;
static dlist_head backends = DLIST_STATIC_INIT(backends);

Backend *backend_host(const char *host) {
    dlist_mutable_iter iter;
    if (host) dlist_foreach_modify(iter, &backends) {
        Backend *backend = dlist_container(Backend, node, iter.cur);
        if (!strcmp(host, backend->host)) return backend;
    }
    return NULL;
}

Backend *backend_state(state_t state) {
    dlist_mutable_iter iter;
    if (state != state_unknown) dlist_foreach_modify(iter, &backends) {
        Backend *backend = dlist_container(Backend, node, iter.cur);
        if (backend->state == state) return backend;
    }
    return NULL;
}

bool backend_busy(Backend *backend, int event) {
    if (PQisBusy(backend->conn)) { W("%s:%s PQisBusy", backend->host, init_state2char(backend->state)); backend->event = event; return false; }
    return true;
}

bool backend_consume(Backend *backend) {
    if (!PQconsumeInput(backend->conn)) { W("%s:%s !PQconsumeInput and %s and %s", backend->host, init_state2char(backend->state), backend_status(backend), PQerrorMessageMy(backend->conn)); return false; }
    return true;
}

bool backend_consume_flush_busy(Backend *backend) {
    if (!backend_consume(backend)) return false;
    if (!backend_flush(backend)) return false;
    if (!backend_busy(backend, WL_SOCKET_READABLE)) return false;
    return true;
}

bool backend_flush(Backend *backend) {
    switch (PQflush(backend->conn)) {
        case 0: break;
        case 1: D1("PQflush == 1"); backend->event = WL_SOCKET_MASK; return false;
        case -1: W("%s:%s PQflush == -1 and %s and %s", backend->host, init_state2char(backend->state), backend_status(backend), PQerrorMessageMy(backend->conn)); return false;
    }
    return true;
}

const char *backend_status(Backend *backend) {
    switch (PQstatus(backend->conn)) {
        case CONNECTION_AUTH_OK: return "CONNECTION_AUTH_OK";
        case CONNECTION_AWAITING_RESPONSE: return "CONNECTION_AWAITING_RESPONSE";
        case CONNECTION_BAD: return "CONNECTION_BAD";
#if (PG_VERSION_NUM >= 130000)
        case CONNECTION_CHECK_TARGET: return "CONNECTION_CHECK_TARGET";
#endif
        case CONNECTION_CHECK_WRITABLE: return "CONNECTION_CHECK_WRITABLE";
        case CONNECTION_CONSUME: return "CONNECTION_CONSUME";
        case CONNECTION_GSS_STARTUP: return "CONNECTION_GSS_STARTUP";
        case CONNECTION_MADE: return "CONNECTION_MADE";
        case CONNECTION_NEEDED: return "CONNECTION_NEEDED";
        case CONNECTION_OK: return "CONNECTION_OK";
        case CONNECTION_SETENV: return "CONNECTION_SETENV";
        case CONNECTION_SSL_STARTUP: return "CONNECTION_SSL_STARTUP";
        case CONNECTION_STARTED: return "CONNECTION_STARTED";
    }
    return "";
}

int backend_nevents(void) {
    int nevents = 0;
    dlist_mutable_iter iter;
    dlist_foreach_modify(iter, &backends) {
        Backend *backend = dlist_container(Backend, node, iter.cur);
        if (PQstatus(backend->conn) == CONNECTION_BAD) continue;
        if (PQsocket(backend->conn) == PGINVALID_SOCKET) continue;
        nevents++;
    }
    return nevents;
}

static void backend_listen_result(Backend *backend) {
    bool ok = false;
    PGresult *result;
    while (PQstatus(backend->conn) == CONNECTION_OK) {
        if (!(result = PQgetResult(backend->conn))) break;
        switch (PQresultStatus(result)) {
            case PGRES_COMMAND_OK: ok = true; break;
            default: W("%s:%s PQresultStatus = %s and %s", backend->host, init_state2char(backend->state), PQresStatus(PQresultStatus(result)), PQresultErrorMessageMy(result)); break;
        }
        PQclear(result);
        if (!backend_consume_flush_busy(backend)) return;
    }
    if (ok) backend_idle(backend);
    else if (PQstatus(backend->conn) == CONNECTION_OK) backend_finish(backend);
}

static void backend_listen(Backend *backend) {
    const char *channel = backend->host;
    const char *channel_quote;
    StringInfoData buf;
    backend->socket = backend_listen;
    if (!backend_busy(backend, WL_SOCKET_WRITEABLE)) return;
    channel_quote = quote_identifier(channel);
    initStringInfoMy(TopMemoryContext, &buf);
    appendStringInfo(&buf, SQL(LISTEN %s), channel_quote);
    if (channel_quote != channel) pfree((void *)channel_quote);
    if (!PQsendQuery(backend->conn, buf.data)) { W("%s:%s !PQsendQuery and %s", backend->host, init_state2char(backend->state), PQerrorMessageMy(backend->conn)); backend_finish(backend); pfree(buf.data); return; }
    pfree(buf.data);
    backend->socket = backend_listen_result;
    if (!backend_flush(backend)) return;
    backend->event = WL_SOCKET_READABLE;
}

static void backend_connected(Backend *backend) {
    D1("%s:%s", backend->host, init_state2char(backend->state));
    backend->attempt = 0;
    init_set_host(backend->host, backend->state);
    backend_listen(backend);
    RecoveryInProgress() ? standby_connected(backend) : primary_connected(backend);
    init_reload();
}

static void backend_fail(Backend *backend) {
    if (backend->attempt++ < init_attempt) return;
    D1("%s:%s", backend->host, init_state2char(backend->state));
    init_set_host(backend->host, state_unknown);
    RecoveryInProgress() ? standby_failed(backend) : primary_failed(backend);
    init_reload();
}

static void backend_connect_or_reset_socket(Backend *backend, PostgresPollingStatusType (*poll) (PGconn *conn)) {
    switch (PQstatus(backend->conn)) {
        case CONNECTION_BAD: W("%s:%s CONNECTION_BAD and %i < %i and %s", backend->host, init_state2char(backend->state), backend->attempt, init_attempt, PQerrorMessageMy(backend->conn)); backend_fail(backend); return;
        case CONNECTION_OK: D1("%s:%s CONNECTION_OK", backend->host, init_state2char(backend->state)); backend_connected(backend); return;
        default: break;
    }
    switch (poll(backend->conn)) {
        case PGRES_POLLING_ACTIVE: D1("%s:%s PGRES_POLLING_ACTIVE and %s", backend->host, init_state2char(backend->state), backend_status(backend)); break;
        case PGRES_POLLING_FAILED: W("%s:%s PGRES_POLLING_FAILED and %s and %i < %i and %s", backend->host, init_state2char(backend->state), backend_status(backend), backend->attempt, init_attempt, PQerrorMessageMy(backend->conn)); backend_fail(backend); return;
        case PGRES_POLLING_OK: D1("%s:%s PGRES_POLLING_OK and %s", backend->host, init_state2char(backend->state), backend_status(backend)); backend_connected(backend); return;
        case PGRES_POLLING_READING: D1("%s:%s PGRES_POLLING_READING and %s", backend->host, init_state2char(backend->state), backend_status(backend)); backend->event = WL_SOCKET_READABLE; break;
        case PGRES_POLLING_WRITING: D1("%s:%s PGRES_POLLING_WRITING and %s", backend->host, init_state2char(backend->state), backend_status(backend)); backend->event = WL_SOCKET_WRITEABLE; break;
    }
}

static void backend_create_socket(Backend *backend) {
    backend_connect_or_reset_socket(backend, PQconnectPoll);
}

static void backend_reset_socket(Backend *backend) {
    backend_connect_or_reset_socket(backend, PQresetPoll);
}

static void backend_connect_or_reset(Backend *backend) {
    const char *keywords[] = {"host", "port", "user", "dbname", "application_name", "target_session_attrs", NULL};
    const char *values[] = {backend->host, pgport ? pgport : DEF_PGPORT_STR, MyProcPort->user_name, MyProcPort->database_name, hostname, backend->state <= state_primary ? "read-write" : "any", NULL};
    StaticAssertStmt(countof(keywords) == countof(values), "countof(keywords) == countof(values)");
    switch (PQpingParams(keywords, values, false)) {
        case PQPING_NO_ATTEMPT: W("%s:%s PQPING_NO_ATTEMPT and %i < %i", backend->host, init_state2char(backend->state), backend->attempt, init_attempt); backend_fail(backend); return;
        case PQPING_NO_RESPONSE: W("%s:%s PQPING_NO_RESPONSE and %i < %i", backend->host, init_state2char(backend->state), backend->attempt, init_attempt); backend_fail(backend); return;
        case PQPING_OK: D1("%s:%s PQPING_OK", backend->host, init_state2char(backend->state)); break;
        case PQPING_REJECT: W("%s:%s PQPING_REJECT and %i < %i", backend->host, init_state2char(backend->state), backend->attempt, init_attempt); backend_fail(backend); return;
    }
    if (!backend->conn) {
        if (!(backend->conn = PQconnectStartParams(keywords, values, false))) { W("%s:%s !PQconnectStartParams and %i < %i and %s", backend->host, init_state2char(backend->state), backend->attempt, init_attempt, PQerrorMessageMy(backend->conn)); backend_fail(backend); return; }
        backend->socket = backend_create_socket;
    } else {
        if (!(PQresetStart(backend->conn))) { W("%s:%s !PQresetStart and %i < %i and %s", backend->host, init_state2char(backend->state), backend->attempt, init_attempt, PQerrorMessageMy(backend->conn)); backend_fail(backend); return; }
        backend->socket = backend_reset_socket;
    }
    if (PQstatus(backend->conn) == CONNECTION_BAD) { W("%s:%s PQstatus == CONNECTION_BAD and %s", backend->host, init_state2char(backend->state), PQerrorMessageMy(backend->conn)); backend_finish(backend); return; }
    if (!PQisnonblocking(backend->conn) && PQsetnonblocking(backend->conn, true) == -1) { W("%s:%s PQsetnonblocking == -1 and %s", backend->host, init_state2char(backend->state), PQerrorMessageMy(backend->conn)); backend_finish(backend); return; }
    if (PQclientEncoding(backend->conn) != GetDatabaseEncoding()) PQsetClientEncoding(backend->conn, GetDatabaseEncodingName());
    backend->event = WL_SOCKET_MASK;
}

static void backend_created(Backend *backend) {
    RecoveryInProgress() ? standby_created(backend) : primary_created(backend);
}

void backend_create(const char *host, state_t state) {
    Backend *backend;
    if (!strcmp(host, hostname)) { W("backend with host \"%s\" is local!", host); return; }
    if ((backend = backend_host(host))) { W("backend with host \"%s\" already exists!", host); return; }
    backend = MemoryContextAllocZero(TopMemoryContext, sizeof(*backend));
    backend->host = MemoryContextStrdup(TopMemoryContext, host);
    backend->state = state;
    dlist_push_head(&backends, &backend->node);
    backend_connect_or_reset(backend);
    backend_created(backend);
}

void backend_event(WaitEventSet *set) {
    dlist_mutable_iter iter;
    AddWaitEventToSet(set, WL_LATCH_SET, PGINVALID_SOCKET, MyLatch, NULL);
    AddWaitEventToSet(set, WL_POSTMASTER_DEATH, PGINVALID_SOCKET, NULL, NULL);
    dlist_foreach_modify(iter, &backends) {
        Backend *backend = dlist_container(Backend, node, iter.cur);
        int fd;
        if (PQstatus(backend->conn) == CONNECTION_BAD) continue;
        if ((fd = PQsocket(backend->conn)) == PGINVALID_SOCKET) continue;
        AddWaitEventToSet(set, backend->event, fd, NULL, backend);
    }
}

static void backend_finished(Backend *backend) {
    D1("%s:%s", backend->host, init_state2char(backend->state));
    RecoveryInProgress() ? standby_finished(backend) : primary_finished(backend);
    init_reload();
}

void backend_finish(Backend *backend) {
    dlist_delete(&backend->node);
    backend_finished(backend);
    PQfinish(backend->conn);
    pfree(backend->host);
    pfree(backend);
}

void backend_fini(void) {
    dlist_mutable_iter iter;
    dlist_foreach_modify(iter, &backends) {
        Backend *backend = dlist_container(Backend, node, iter.cur);
        backend_finish(backend);
    }
    RecoveryInProgress() ? standby_fini() : primary_fini();
}

static void backend_idle_result(Backend *backend) {
    PGresult *result;
    while (PQstatus(backend->conn) == CONNECTION_OK) {
        if (!(result = PQgetResult(backend->conn))) break;
        switch (PQresultStatus(result)) {
            default: D1("%s:%s PQresultStatus = %s and %s", backend->host, init_state2char(backend->state), PQresStatus(PQresultStatus(result)), PQresultErrorMessageMy(result)); break;
        }
        PQclear(result);
        if (!backend_consume_flush_busy(backend)) return;
    }
}

void backend_idle(Backend *backend) {
    backend->event = WL_SOCKET_READABLE;
    backend->socket = backend_idle_result;
}

void backend_init(void) {
    pgport = getenv("PGPORT");
    init_backend();
    RecoveryInProgress() ? standby_init() : primary_init();
    init_reload();
}

static void backend_notify(Backend *backend, state_t state) {
    D1("%s:%s state = %s", backend->host, init_state2char(backend->state), init_state2char(state));
    RecoveryInProgress() ? standby_notify(backend, state) : primary_notify(backend, state);
}

static void backend_updated(Backend *backend) {
    D1("%s:%s", backend->host, init_state2char(backend->state));
    RecoveryInProgress() ? standby_updated(backend) : primary_updated(backend);
    init_reload();
}

void backend_readable(Backend *backend) {
    PGnotify *notify;
    while (PQstatus(backend->conn) == CONNECTION_OK) {
        if (!backend_consume_flush_busy(backend)) return;
        if (!(notify = PQnotifies(backend->conn))) break;
        if (MyProcPid != notify->be_pid) backend_notify(backend, init_char2state(notify->extra));
        PQfreemem(notify);
    }
    backend->socket(backend);
}

void backend_result(const char *host, state_t state) {
    Backend *backend = backend_host(host);
    if (RecoveryInProgress() && !strcmp(host, hostname)) return standby_update(state);
    backend ? backend_update(backend, state) : backend_create(host, state);
}

void backend_timeout(void) {
    dlist_mutable_iter iter;
    dlist_foreach_modify(iter, &backends) {
        Backend *backend = dlist_container(Backend, node, iter.cur);
        if (PQstatus(backend->conn) == CONNECTION_BAD) backend_connect_or_reset(backend);
    }
    RecoveryInProgress() ? standby_timeout() : primary_timeout();
    init_reload();
}

void backend_update(Backend *backend, state_t state) {
    if (backend->state == state) return;
    D1("%s:%s->%s", backend->host, init_state2char(backend->state), init_state2char(state));
    backend->state = state;
    init_set_host(backend->host, state);
    backend_updated(backend);
}

void backend_writeable(Backend *backend) {
    if (PQstatus(backend->conn) == CONNECTION_OK) if (!backend_flush(backend)) return;
    backend->socket(backend);
}
