#include "include.h"

ArrayType *save = NULL;
extern char *hostname;
extern int init_attempt;
extern Oid type_array;
extern Oid type;
extern queue_t backend_queue;

static void backend_save(void) {
    Datum *elems;
    TupleDescData *tupdesc;
    int nelems = queue_size(&backend_queue);
    if (save) pfree(save);
    save = NULL;
    if (!nelems) return;
    SPI_connect_my("TypeGetTupleDesc");
    tupdesc = TypeGetTupleDesc(type, NULL);
    SPI_commit_my();
    SPI_finish_my();
    elems = MemoryContextAlloc(TopMemoryContext, nelems * sizeof(*elems));
    nelems = 0;
    queue_each(&backend_queue, queue) {
        Backend *backend = queue_data(queue, Backend, queue);
        Datum values[] = {CStringGetTextDatum(PQhost(backend->conn)), CStringGetTextDatum(init_state2char(backend->state))};
        bool isnull[] = {false, false};
        HeapTupleData *tuple = heap_form_tuple(tupdesc, values, isnull);
        D1("state = %s, host = %s", init_state2char(backend->state), PQhost(backend->conn));
        elems[nelems] = HeapTupleGetDatum(tuple);
        for (int i = 0; i < countof(values); i++) pfree((void *)values[i]);
        nelems++;
    }
    if (nelems) {
        MemoryContext oldMemoryContext = MemoryContextSwitchTo(TopMemoryContext);
        save = construct_array(elems, nelems, type_array, -1, false, TYPALIGN_INT);
        MemoryContextSwitchTo(oldMemoryContext);
//        for (int i = 0; i < nelems; i++) heap_freetuple(elems[i]);
    }
//    FreeTupleDesc(tupdesc);
    pfree(elems);
}

static void backend_connected(Backend *backend) {
    D1("%s:%s", PQhost(backend->conn), init_state2char(backend->state));
    RecoveryInProgress() ? standby_connected(backend) : primary_connected(backend);
    backend_save();
    init_reload();
}

static void backend_connect_or_reset_socket(Backend *backend, PostgresPollingStatusType (*poll) (PGconn *conn)) {
    switch (PQstatus(backend->conn)) {
        case CONNECTION_AUTH_OK: D1("%s:%s CONNECTION_AUTH_OK", PQhost(backend->conn), init_state2char(backend->state)); break;
        case CONNECTION_AWAITING_RESPONSE: D1("%s:%s CONNECTION_AWAITING_RESPONSE", PQhost(backend->conn), init_state2char(backend->state)); break;
        case CONNECTION_BAD: W("%s:%s CONNECTION_BAD and %.*s", PQhost(backend->conn), init_state2char(backend->state), (int)strlen(PQerrorMessage(backend->conn)) - 1, PQerrorMessage(backend->conn)); backend_finish(backend); return;
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
        const char *values[] = {host, getenv("PGPORT") ? getenv("PGPORT") : DEF_PGPORT_STR, MyProcPort->user_name, MyProcPort->database_name, hostname, NULL};
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
    queue_insert_tail(&backend_queue, &backend->queue);
}

static void backend_finished(Backend *backend) {
    D1("%s:%s", PQhost(backend->conn), init_state2char(backend->state));
    init_reset_remote_state(backend->state);
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
    pfree(backend);
    backend_save();
}

void backend_fini(void) {
    queue_each(&backend_queue, queue) {
        Backend *backend = queue_data(queue, Backend, queue);
        backend_finish(backend);
    }
}

static void backend_idle_socket(Backend *backend) {
    for (PGresult *result; (result = PQgetResult(backend->conn)); PQclear(result)) switch (PQresultStatus(result)) {
        default: D1("%s:%s PQresultStatus = %s and %.*s", PQhost(backend->conn), init_state2char(backend->state), PQresStatus(PQresultStatus(result)), (int)strlen(PQresultErrorMessage(result)), PQresultErrorMessage(result)); break;
    }
}

void backend_idle(Backend *backend) {
    backend->socket = backend_idle_socket;
}

void backend_reset(Backend *backend) {
    backend_connect_or_reset(backend, NULL);
}

static void backend_updated(Backend *backend) {
    D1("%s:%s", PQhost(backend->conn), init_state2char(backend->state));
    RecoveryInProgress() ? standby_updated(backend) : primary_updated(backend);
    backend_save();
    init_reload();
}

void backend_update(Backend *backend, STATE state) {
    init_reset_remote_state(backend->state);
    backend->state = state;
    init_set_remote_state(backend->state, PQhost(backend->conn));
    backend_updated(backend);
}
