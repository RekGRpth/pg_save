#include "include.h"

extern char *hostname;
extern char *init_policy;
extern char *init_primary;
extern char *init_state;
extern int init_probe;
extern queue_t backend_queue;
extern TimestampTz start;

static void primary_set_synchronous_standby_names(void) {
    StringInfoData buf;
    char **names;
    int i = 0;
    if (!queue_size(&backend_queue)) return;
    names = MemoryContextAlloc(TopMemoryContext, queue_size(&backend_queue) * sizeof(*names));
    queue_each(&backend_queue, queue) {
        Backend *backend = queue_data(queue, Backend, queue);
        names[i++] = backend->name ? backend->name : cluster_name ? cluster_name : "walreceiver";
    }
    pg_qsort(names, queue_size(&backend_queue), sizeof(*names), pg_qsort_strcmp);
    initStringInfo(&buf);
    appendStringInfo(&buf, "%s (", init_policy);
    for (int i = 0; i < queue_size(&backend_queue); i++) {
        const char *name_quote = quote_identifier(names[i]);
        if (i) appendStringInfoString(&buf, ", ");
        appendStringInfoString(&buf, name_quote);
        if (name_quote != names[i]) pfree((void *)name_quote);
    }
    pfree(names);
    appendStringInfoString(&buf, ")");
    init_alter_system_set("synchronous_standby_names", SyncRepStandbyNames, buf.data);
    pfree(buf.data);
}

void primary_connected(Backend *backend) {
    backend->probe = 0;
    primary_set_synchronous_standby_names();
    init_set_state(backend_host(backend), backend_state(backend));
    backend_idle(backend);
}

void primary_reseted(Backend *backend) {
    if (backend->probe++ < init_probe) return;
    backend_finish(backend);
}

void primary_finished(Backend *backend) {
}

static void primary_standby(void) {
    int nargs = queue_size(&backend_queue);
    Oid *argtypes = nargs ? palloc(2 * nargs * sizeof(*argtypes)) : NULL;
    Datum *values = nargs ? palloc(2 * nargs * sizeof(*values)) : NULL;
    StringInfoData buf;
    initStringInfo(&buf);
    appendStringInfoString(&buf, "SELECT application_name AS name, coalesce(client_hostname, client_addr::text) AS host, sync_state AS state FROM pg_stat_replication");
    nargs = 0;
    queue_each(&backend_queue, queue) {
        Backend *backend = queue_data(queue, Backend, queue);
        if (nargs) appendStringInfoString(&buf, ", ");
        else appendStringInfoString(&buf, " WHERE (client_addr, sync_state) NOT IN (");
        argtypes[nargs] = INETOID;
        values[nargs] = DirectFunctionCall1(inet_in, CStringGetDatum(PQhostaddr(backend->conn)));
        nargs++;
        appendStringInfo(&buf, "($%i", nargs);
        argtypes[nargs] = TEXTOID;
        values[nargs] = CStringGetTextDatum(backend->state);
        nargs++;
        appendStringInfo(&buf, ", $%i)", nargs);
    }
    if (nargs) appendStringInfoString(&buf, ")");
    SPI_connect_my(buf.data);
    SPI_execute_with_args_my(buf.data, nargs, argtypes, values, NULL, SPI_OK_SELECT, true);
    for (uint64 row = 0; row < SPI_processed; row++) {
        Backend *backend = NULL;
        MemoryContext oldMemoryContext = MemoryContextSwitchTo(TopMemoryContext);
        const char *name = TextDatumGetCStringMy(SPI_getbinval_my(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, "name", false));
        const char *host = TextDatumGetCStringMy(SPI_getbinval_my(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, "host", false));
        const char *state = TextDatumGetCStringMy(SPI_getbinval_my(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, "state", false));
        MemoryContextSwitchTo(oldMemoryContext);
        D1("name = %s, host = %s, state = %s", name, host, state);
        queue_each(&backend_queue, queue) {
            Backend *backend_ = queue_data(queue, Backend, queue);
            if (!strcmp(host, backend_host(backend_))) { backend = backend_; break; }
        }
        if (backend) {
            init_reset_state(backend_host(backend));
            if (backend->name) pfree(backend->name);
            pfree(backend->state);
            backend->name = MemoryContextStrdup(TopMemoryContext, name);
            backend->state = MemoryContextStrdup(TopMemoryContext, state);
            primary_set_synchronous_standby_names();
            init_set_state(backend_host(backend), backend_state(backend));
        } else {
            backend_connect(host, getenv("PGPORT") ? getenv("PGPORT") : DEF_PGPORT_STR, MyProcPort->user_name, MyProcPort->database_name, state, name);
        }
        pfree((void *)name);
        pfree((void *)host);
        pfree((void *)state);
    }
    SPI_finish_my();
    for (int i = 0; i < nargs; i++) pfree((void *)values[i]);
    pfree(buf.data);
}

void primary_timeout(void) {
    if (!save_etcd_kv_put(init_state, hostname, 0)) {
        W("!save_etcd_kv_put");
        init_kill();
    }
    if (!save_etcd_kv_put(hostname, timestamptz_to_str(start), 0)) {
        W("!save_etcd_kv_put");
        init_kill();
    }
    queue_each(&backend_queue, queue) {
        Backend *backend = queue_data(queue, Backend, queue);
        if (PQstatus(backend->conn) == CONNECTION_BAD) backend_reset(backend);
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
    init_alter_system_reset("primary_conninfo");
    init_alter_system_reset("primary_slot_name");
    init_alter_system_set("pg_save.primary", init_primary, hostname);
    init_alter_system_set("pg_save.state", init_state, "primary");
    primary_schema("curl");
    primary_extension("curl", "pg_curl");
    primary_schema("save");
    primary_extension("save", "pg_save");
}

void primary_fini(void) {
    backend_fini();
}
