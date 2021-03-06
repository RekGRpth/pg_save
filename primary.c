#include "include.h"

extern char *hostname;
extern queue_t backend_queue;
extern TimestampTz start;

/*static void primary_info(void) {
    SyncRepStandbyData *sync_standbys;
    int num_standbys = SyncRepGetCandidateStandbys(&sync_standbys);
    int num_backends = pgstat_fetch_stat_numbackends();
    for (int i = 0; i < max_wal_senders; i++) {
        char *client_addr = NULL;
        char *client_hostname = NULL;
        WalSnd *walsnd = &WalSndCtl->walsnds[i];
        int priority;
        int pid;
        WalSndState state;
        bool is_sync_standby = false;
        char *sync_state;
        SpinLockAcquire(&walsnd->mutex);
        if (!walsnd->pid) { SpinLockRelease(&walsnd->mutex); continue; }
        pid = walsnd->pid;
        state = walsnd->state;
        priority = walsnd->sync_standby_priority;
        SpinLockRelease(&walsnd->mutex);
        for (int j = 0; j < num_standbys; j++) if (sync_standbys[j].walsnd_index == i && sync_standbys[j].pid == pid) { is_sync_standby = true; break; }
        if (priority == 0) sync_state = "async";
        else if (is_sync_standby) sync_state = SyncRepConfig->syncrep_method == SYNC_REP_PRIORITY ? "sync" : "quorum";
        else sync_state = "potential";
        for (int curr_backend = 1; curr_backend <= num_backends; curr_backend++) {
            SockAddr zero_clientaddr;
            PgBackendStatus *beentry;
            LocalPgBackendStatus *local_beentry = pgstat_fetch_stat_local_beentry(curr_backend);
            if (!local_beentry) continue;
            beentry = &local_beentry->backendStatus;
            if (pid != -1 && beentry->st_procpid != pid) continue;
            memset(&zero_clientaddr, 0, sizeof(zero_clientaddr));
            if (memcmp(&(beentry->st_clientaddr), &zero_clientaddr, sizeof(zero_clientaddr))) {
                if (beentry->st_clientaddr.addr.ss_family == AF_INET
#ifdef HAVE_IPV6
                    || beentry->st_clientaddr.addr.ss_family == AF_INET6
#endif
                ) {
                    char remote_host[NI_MAXHOST];
                    char remote_port[NI_MAXSERV];
                    remote_host[0] = '\0';
                    remote_port[0] = '\0';
                    if (!pg_getnameinfo_all(&beentry->st_clientaddr.addr, beentry->st_clientaddr.salen, remote_host, sizeof(remote_host), remote_port, sizeof(remote_port), NI_NUMERICHOST | NI_NUMERICSERV)) {
                        clean_ipv6_addr(beentry->st_clientaddr.addr.ss_family, remote_host);
                        client_addr = remote_host;
                        if (beentry->st_clienthostname && beentry->st_clienthostname[0]) client_hostname = beentry->st_clienthostname;
                    }
                }
            }
        }
        D1("pid = %i, state = %i, sync_priority = %i, sync_state = %s, client_addr = %s, client_hostname = %s", pid, state, priority, sync_state, client_addr ? client_addr : "(null)", client_hostname ? client_hostname : "(null)");
    }
    pfree(sync_standbys);
}*/

static void primary_standby(void) {
    int nargs = queue_size(&backend_queue);
    Oid *argtypes = nargs ? palloc(nargs * sizeof(*argtypes)) : NULL;
    Datum *values = nargs ? palloc(nargs * sizeof(*values)) : NULL;
    StringInfoData buf;
    initStringInfo(&buf);
    appendStringInfoString(&buf, "SELECT client_addr AS addr, coalesce(client_hostname, client_addr::text) AS host, sync_state AS state FROM pg_stat_replication");
    nargs = 0;
    queue_each(&backend_queue, queue) {
        Backend *backend = queue_data(queue, Backend, queue);
        if (nargs) appendStringInfoString(&buf, ", ");
        else appendStringInfoString(&buf, " WHERE client_addr NOT IN (");
        argtypes[nargs] = INETOID;
        values[nargs] = CStringGetTextDatum(PQhostaddr(backend->conn));
        nargs++;
        appendStringInfo(&buf, "$%i", nargs);
    }
    if (nargs) appendStringInfoString(&buf, ")");
    SPI_connect_my(buf.data);
    SPI_execute_with_args_my(buf.data, nargs, argtypes, values, NULL, SPI_OK_SELECT, true);
    for (uint64 row = 0; row < SPI_processed; row++) {
        STATE state;
        Backend *backend;
        MemoryContext oldMemoryContext = MemoryContextSwitchTo(TopMemoryContext);
        const char *addr = TextDatumGetCStringMy(SPI_getbinval_my(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, "addr", false));
        const char *host = TextDatumGetCStringMy(SPI_getbinval_my(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, "host", false));
        const char *cstate = TextDatumGetCStringMy(SPI_getbinval_my(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, "state", false));
        D1("addr = %s, host = %s, state = %s", addr, host, cstate);
        if (pg_strcasecmp(cstate, "async")) state = ASYNC;
        else if (pg_strcasecmp(cstate, "potential")) state = POTENTIAL;
        else if (pg_strcasecmp(cstate, "sync")) state = SYNC;
        else if (pg_strcasecmp(cstate, "quorum")) state = QUORUM;
        else E("unknown state = %s", cstate);
        backend = palloc0(sizeof(*backend));
        MemoryContextSwitchTo(oldMemoryContext);
        backend->state = state;
        backend_connect(backend, host, 5432, MyProcPort->user_name, MyProcPort->database_name, backend_idle);
        pfree((void *)addr);
        pfree((void *)host);
        pfree((void *)cstate);
    }
    SPI_finish_my();
    for (int i = 0; i < nargs; i++) pfree((void *)values[i]);
}

void primary_timeout(void) {
    if (!save_etcd_kv_put("primary", hostname, 0)) {
        W("!save_etcd_kv_put");
        init_kill();
    }
    if (!save_etcd_kv_put(hostname, timestamptz_to_str(start), 0)) {
        W("!save_etcd_kv_put");
        init_kill();
    }
    //primary_info();
    queue_each(&backend_queue, queue) {
        Backend *backend = queue_data(queue, Backend, queue);
        if (PQstatus(backend->conn) == CONNECTION_BAD) { backend_reset(backend, backend_idle, backend_finish); continue; }
    }
    primary_standby();
}

static void primary_schema(const char *schema) {
    StringInfoData buf;
    List *names;
    const char *schema_quote = quote_identifier(schema);
    D1("schema = %s", schema);
    initStringInfo(&buf);
    appendStringInfo(&buf, "CREATE SCHEMA %s", schema_quote);
    names = stringToQualifiedNameList(schema_quote);
    SPI_connect_my(buf.data);
    if (!OidIsValid(get_namespace_oid(strVal(linitial(names)), true))) SPI_execute_with_args_my(buf.data, 0, NULL, NULL, NULL, SPI_OK_UTILITY, false);
    else D1("schema %s already exists", schema_quote);
    SPI_commit_my();
    SPI_finish_my();
    list_free_deep(names);
    if (schema_quote != schema) pfree((void *)schema_quote);
    pfree(buf.data);
}

static void primary_extension(const char *schema, const char *extension) {
    StringInfoData buf;
    List *names;
    const char *schema_quote = schema ? quote_identifier(schema) : NULL;
    const char *extension_quote = quote_identifier(extension);
    D1("schema = %s, extension = %s", schema ? schema : "(null)", extension);
    initStringInfo(&buf);
    appendStringInfo(&buf, "CREATE EXTENSION %s", extension_quote);
    if (schema) appendStringInfo(&buf, " SCHEMA %s", schema_quote);
    names = stringToQualifiedNameList(extension_quote);
    SPI_connect_my(buf.data);
    if (!OidIsValid(get_extension_oid(strVal(linitial(names)), true))) SPI_execute_with_args_my(buf.data, 0, NULL, NULL, NULL, SPI_OK_UTILITY, false);
    else D1("extension %s already exists", extension_quote);
    SPI_commit_my();
    SPI_finish_my();
    list_free_deep(names);
    if (schema && schema_quote != schema) pfree((void *)schema_quote);
    if (extension_quote != extension) pfree((void *)extension_quote);
    pfree(buf.data);
}

void primary_init(void) {
    primary_schema("curl");
    primary_extension("curl", "pg_curl");
    primary_schema("save");
    primary_extension("save", "pg_save");
}

void primary_fini(void) {
}
