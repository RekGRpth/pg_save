#include "include.h"

extern char *hostname;
extern int reset;
extern queue_t backend_queue;
extern TimestampTz start;
static STATE my_state;

static char *standby_int2char(int number) {
    StringInfoData buf;
    initStringInfo(&buf);
    appendStringInfo(&buf, "%i", number);
    return buf.data;
}

static void standby_idle_callback(Backend *backend) {
    if (!PQconsumeInput(backend->conn)) E("!PQconsumeInput and %s", PQerrorMessage(backend->conn));
    if (PQisBusy(backend->conn)) backend->events = WL_SOCKET_READABLE; else {
        for (PGresult *result; (result = PQgetResult(backend->conn)); PQclear(result)) switch (PQresultStatus(result)) {
            default: D1("PQresultStatus = %s and %s", PQresStatus(PQresultStatus(result)), PQresultErrorMessage(result)); break;
        }
    }
}

static void standby_idle(Backend *backend) {
    backend->callback = standby_idle_callback;
}

static void standby_reload_conf_callback(Backend *backend) {
    if (!PQconsumeInput(backend->conn)) E("!PQconsumeInput and %s", PQerrorMessage(backend->conn));
    if (PQisBusy(backend->conn)) backend->events = WL_SOCKET_READABLE; else {
        for (PGresult *result; (result = PQgetResult(backend->conn)); PQclear(result)) switch (PQresultStatus(result)) {
            case PGRES_TUPLES_OK: standby_idle(backend); break;
            default: E("PQresultStatus = %s and %s", PQresStatus(PQresultStatus(result)), PQresultErrorMessage(result)); break;
        }
    }
}

static void standby_reload_conf(Backend *backend) {
    if (!PQsendQuery(backend->conn, "SELECT pg_reload_conf()")) E("!PQsendQuery and %s", PQerrorMessage(backend->conn));
    backend->callback = standby_reload_conf_callback;
    backend->events = WL_SOCKET_WRITEABLE;
}

static void standby_set_synchronous_standby_names_callback(Backend *backend) {
    if (!PQconsumeInput(backend->conn)) E("!PQconsumeInput and %s", PQerrorMessage(backend->conn));
    if (PQisBusy(backend->conn)) backend->events = WL_SOCKET_READABLE; else {
        for (PGresult *result; (result = PQgetResult(backend->conn)); PQclear(result)) switch (PQresultStatus(result)) {
            case PGRES_COMMAND_OK: standby_reload_conf(backend); break;
            default: E("PQresultStatus = %s and %s", PQresStatus(PQresultStatus(result)), PQresultErrorMessage(result)); break;
        }
    }
}

static void standby_set_synchronous_standby_names(Backend *backend) {
    char *cluster_name_ = cluster_name ? cluster_name : "walreceiver";
    const char *cluster_name_quote = quote_identifier(cluster_name_);
    StringInfoData buf;
    initStringInfo(&buf);
    appendStringInfo(&buf, "ALTER SYSTEM SET synchronous_standby_names TO 'FIRST 1 (%s)'", cluster_name_quote);
    if (cluster_name_quote != cluster_name_) pfree((void *)cluster_name_quote);
    if (!PQsendQuery(backend->conn, buf.data)) E("!PQsendQuery and %s", PQerrorMessage(backend->conn));
    backend->callback = standby_set_synchronous_standby_names_callback;
    backend->events = WL_SOCKET_WRITEABLE;
    pfree(buf.data);
}

static void standby_connect_callback(Backend *backend) {
    bool connected = false;
    switch (PQstatus(backend->conn)) {
        case CONNECTION_AUTH_OK: D1("PQstatus == CONNECTION_AUTH_OK"); break;
        case CONNECTION_AWAITING_RESPONSE: D1("PQstatus == CONNECTION_AWAITING_RESPONSE"); break;
        case CONNECTION_BAD: E("PQstatus == CONNECTION_BAD and %s", PQerrorMessage(backend->conn)); break;
#if (PG_VERSION_NUM >= 130000)
        case CONNECTION_CHECK_TARGET: D1("PQstatus == CONNECTION_CHECK_TARGET"); break;
#endif
        case CONNECTION_CHECK_WRITABLE: D1("PQstatus == CONNECTION_CHECK_WRITABLE"); break;
        case CONNECTION_CONSUME: D1("PQstatus == CONNECTION_CONSUME"); break;
        case CONNECTION_GSS_STARTUP: D1("PQstatus == CONNECTION_GSS_STARTUP"); break;
        case CONNECTION_MADE: D1("PQstatus == CONNECTION_MADE"); break;
        case CONNECTION_NEEDED: D1("PQstatus == CONNECTION_NEEDED"); break;
        case CONNECTION_OK: D1("PQstatus == CONNECTION_OK"); connected = true; break;
        case CONNECTION_SETENV: D1("PQstatus == CONNECTION_SETENV"); break;
        case CONNECTION_SSL_STARTUP: D1("PQstatus == CONNECTION_SSL_STARTUP"); break;
        case CONNECTION_STARTED: D1("PQstatus == CONNECTION_STARTED"); break;
    }
    switch (PQconnectPoll(backend->conn)) {
        case PGRES_POLLING_ACTIVE: D1("PQconnectPoll == PGRES_POLLING_ACTIVE"); break;
        case PGRES_POLLING_FAILED: E("PQconnectPoll == PGRES_POLLING_FAILED and %s", PQerrorMessage(backend->conn)); break;
        case PGRES_POLLING_OK: D1("PQconnectPoll == PGRES_POLLING_OK"); connected = true; break;
        case PGRES_POLLING_READING: D1("PQconnectPoll == PGRES_POLLING_READING"); backend->events = WL_SOCKET_READABLE; break;
        case PGRES_POLLING_WRITING: D1("PQconnectPoll == PGRES_POLLING_WRITING"); backend->events = WL_SOCKET_WRITEABLE; break;
    }
    if ((backend->fd = PQsocket(backend->conn)) < 0) E("PQsocket < 0 and %s", PQerrorMessage(backend->conn));
    if (connected) {
        if (backend->state == PRIMARY) standby_set_synchronous_standby_names(backend);
    }
}

static void standby_connect(Backend *backend, const char *host, int port, const char *user, const char *dbname) {
    char *cport = standby_int2char(port);
    const char *keywords[] = {"host", "port", "user", "dbname", "application_name", NULL};
    const char *values[] = {host, cport, user, dbname, "pg_save", NULL};
    StaticAssertStmt(countof(keywords) == countof(values), "countof(keywords) == countof(values)");
    if (!(backend->conn = PQconnectStartParams(keywords, values, false))) E("!PQconnectStartParams and %s", PQerrorMessage(backend->conn));
    pfree(cport);
    if (PQstatus(backend->conn) == CONNECTION_BAD) E("PQstatus == CONNECTION_BAD and %s", PQerrorMessage(backend->conn));
    if (!PQisnonblocking(backend->conn) && PQsetnonblocking(backend->conn, true) == -1) E("PQsetnonblocking == -1 and %s", PQerrorMessage(backend->conn));
    if ((backend->fd = PQsocket(backend->conn)) < 0) E("PQsocket < 0");
    if (PQclientEncoding(backend->conn) != GetDatabaseEncoding()) PQsetClientEncoding(backend->conn, GetDatabaseEncodingName());
    backend->callback = standby_connect_callback;
    backend->events = WL_SOCKET_WRITEABLE;
    queue_insert_tail(&backend_queue, &backend->queue);
}

static void standby_primary_init(const char *host, int port, const char *user, const char *dbname) {
    Backend *backend = palloc0(sizeof(*backend));
    backend->reset = reset;
    backend->state = PRIMARY;
    standby_connect(backend, host, port, user, dbname);
}

static void standby_reprimary(void) {
    //WriteRecoveryConfig(pgconn, target_dir, GenerateRecoveryConfig(pgconn, replication_slot));
}

static void standby_promote(void) {
    if (!DatumGetBool(DirectFunctionCall2(pg_promote, BoolGetDatum(true), Int32GetDatum(30)))) E("!pg_promote");
    else standby_fini();
}

static void standby_reprimary_or_promote_or_kill(void) {
    if (my_state != SYNC) standby_reprimary();
    else if (queue_size(&backend_queue) > 1) standby_promote();
    else init_kill();
}

static void standby_standby_init(Backend *backend, PGresult *result) {
    for (int row = 0; row < PQntuples(result); row++) {
        Backend *backend;
        const char *addr = PQgetvalue(result, row, PQfnumber(result, "addr"));
        const char *host = PQgetvalue(result, row, PQfnumber(result, "host"));
        const char *cstate = PQgetvalue(result, row, PQfnumber(result, "state"));
        const char *cme = PQgetvalue(result, row, PQfnumber(result, "me"));
        bool me = cme[0] == 't' || cme[0] == 'T';
        STATE state;
        if (!me) D1("addr = %s, host = %s, state = %s", addr, host, cstate);
        if (pg_strcasecmp(cstate, "async")) state = ASYNC;
        else if (pg_strcasecmp(cstate, "potential")) state = POTENTIAL;
        else if (pg_strcasecmp(cstate, "sync")) state = SYNC;
        else if (pg_strcasecmp(cstate, "quorum")) state = QUORUM;
        else E("unknown state = %s", cstate);
        if (me) { my_state = state; continue; }
        backend = palloc0(sizeof(*backend));
        backend->reset = reset;
        backend->state = state;
        standby_connect(backend, host, 5432, MyProcPort->user_name, MyProcPort->database_name);
    }
    standby_idle(backend);
}

static void standby_primary_callback(Backend *backend) {
    if (!PQconsumeInput(backend->conn)) E("!PQconsumeInput and %s", PQerrorMessage(backend->conn));
    if (PQisBusy(backend->conn)) backend->events = WL_SOCKET_READABLE; else {
        for (PGresult *result; (result = PQgetResult(backend->conn)); PQclear(result)) switch (PQresultStatus(result)) {
            case PGRES_TUPLES_OK: standby_standby_init(backend, result); break;
            default: E("PQresultStatus = %s and %s", PQresStatus(PQresultStatus(result)), PQresultErrorMessage(result)); break;
        }
    }
}

static void standby_primary(void) {
    Backend *primary = NULL;
    int nParams = queue_size(&backend_queue) - 1;
    Oid *paramTypes = nParams ? palloc(nParams * sizeof(*paramTypes)) : NULL;
    char **paramValues = nParams ? palloc(nParams * sizeof(**paramValues)) : NULL;
    StringInfoData buf;
    initStringInfo(&buf);
    appendStringInfoString(&buf, "SELECT client_addr AS addr, coalesce(client_hostname, client_addr::text) AS host, sync_state AS state, client_addr IS NOT DISTINCT FROM (SELECT client_addr FROM pg_stat_activity WHERE pid = pg_backend_pid()) AS me FROM pg_stat_replication");
    nParams = 0;
    queue_each(&backend_queue, queue) {
        Backend *backend = queue_data(queue, Backend, queue);
        if (backend->state == PRIMARY) { primary = backend; continue; }
        if (nParams) appendStringInfoString(&buf, ", ");
        else appendStringInfoString(&buf, " WHERE client_addr NOT IN (");
        paramTypes[nParams] = INETOID;
        paramValues[nParams] = PQhostaddr(backend->conn);
        nParams++;
        appendStringInfo(&buf, "$%i", nParams);
    }
    if (nParams) appendStringInfoString(&buf, ")");
    if (primary && primary->callback == standby_idle_callback) {
        if (!PQsendQueryParams(primary->conn, buf.data, nParams, paramTypes, (const char * const*)paramValues, NULL, NULL, false)) E("!PQsendQueryParams and %s", PQerrorMessage(primary->conn));
        primary->callback = standby_primary_callback;
        primary->events = WL_SOCKET_WRITEABLE;
    }
    if (paramTypes) pfree(paramTypes);
    if (paramValues) pfree(paramValues);
    pfree(buf.data);
}

void standby_init(void) {
    bool ready_to_display;
    char sender_host[NI_MAXHOST];
    int pid;
    int sender_port = 0;
    char slotname[NAMEDATALEN];
    SpinLockAcquire(&WalRcv->mutex);
    pid = (int)WalRcv->pid;
    ready_to_display = WalRcv->ready_to_display;
    sender_port = WalRcv->sender_port;
    strlcpy(sender_host, (char *)WalRcv->sender_host, sizeof(sender_host));
    strlcpy(slotname, (char *) WalRcv->slotname, sizeof(slotname));
    SpinLockRelease(&WalRcv->mutex);
    if (!pid) E("!pid");
    if (!ready_to_display) E("!ready_to_display");
    D1("sender_host = %s, sender_port = %i, slotname = %s", sender_host, sender_port, slotname);
    standby_primary_init(sender_host, sender_port, MyProcPort->user_name, MyProcPort->database_name);
}

static void standby_finish(Backend *backend) {
    queue_remove(&backend->queue);
    PQfinish(backend->conn);
    pfree(backend);
}

static void standby_standby_callback(Backend *backend) {
    if (!PQconsumeInput(backend->conn)) E("!PQconsumeInput and %s", PQerrorMessage(backend->conn));
    if (PQisBusy(backend->conn)) backend->events = WL_SOCKET_READABLE; else {
        for (PGresult *result; (result = PQgetResult(backend->conn)); PQclear(result)) switch (PQresultStatus(result)) {
            case PGRES_TUPLES_OK: standby_idle(backend); break;
            default: E("PQresultStatus = %s and %s", PQresStatus(PQresultStatus(result)), PQresultErrorMessage(result)); break;
        }
    }
}

static void standby_standby(void) {
    queue_each(&backend_queue, queue) {
        Backend *backend = queue_data(queue, Backend, queue);
        if (backend->callback != standby_idle_callback) continue;
        if (!PQsendQuery(backend->conn, "SELECT * FROM pg_stat_get_wal_receiver()")) E("!PQsendQuery and %s", PQerrorMessage(backend->conn));
        backend->callback = standby_standby_callback;
        backend->events = WL_SOCKET_WRITEABLE;
    }
}

void standby_timeout(void) {
    if (!save_etcd_kv_put(hostname, timestamptz_to_str(start), 0)) {
        W("!save_etcd_kv_put");
        init_kill();
    }
    standby_primary();
    standby_standby();
}

void standby_fini(void) {
    queue_each(&backend_queue, queue) {
        Backend *backend = queue_data(queue, Backend, queue);
        standby_finish(backend);
    }
}

// pg_rewind --target-pgdata /home/pg_data --source-server application_name=pgautofailover_standby_12 host=postgres-docker-bill-02 port=5432 user=pgautofailover_replicator dbname=postgres sslmode=prefer --progress
