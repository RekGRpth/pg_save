#include "include.h"

extern char *default_policy;
extern char *default_primary;
extern char *default_state;
extern char *hostname;
extern queue_t backend_queue;
extern TimestampTz start;

static void primary_set_synchronous_standby_names(Backend *backend) {
    int i = 0;
    StringInfoData buf;
    initStringInfo(&buf);
    appendStringInfo(&buf, "%s (", default_policy);
    queue_each(&backend_queue, queue) {
        if (i++) appendStringInfoString(&buf, ", ");
        appendStringInfoString(&buf, PQparameterStatus(backend->conn, "application_name") ? PQparameterStatus(backend->conn, "application_name") : cluster_name ? cluster_name : "walreceiver");
    }
    appendStringInfoString(&buf, ")");
    backend_alter_system_set("synchronous_standby_names", SyncRepStandbyNames, buf.data);
    pfree(buf.data);
    backend_idle(backend);
}

static void primary_standby(void) {
    int nargs = queue_size(&backend_queue);
    Oid *argtypes = nargs ? palloc(nargs * sizeof(*argtypes)) : NULL;
    Datum *values = nargs ? palloc(nargs * sizeof(*values)) : NULL;
    StringInfoData buf;
    initStringInfo(&buf);
    appendStringInfoString(&buf, "SELECT coalesce(client_hostname, client_addr::text) AS host, sync_state AS state FROM pg_stat_replication");
    nargs = 0;
    queue_each(&backend_queue, queue) {
        Backend *backend = queue_data(queue, Backend, queue);
        if (nargs) appendStringInfoString(&buf, ", ");
        else appendStringInfoString(&buf, " WHERE client_addr NOT IN (");
        argtypes[nargs] = INETOID;
        values[nargs] = DirectFunctionCall1(inet_in, CStringGetDatum(PQhostaddr(backend->conn)));
        nargs++;
        appendStringInfo(&buf, "$%i", nargs);
    }
    if (nargs) appendStringInfoString(&buf, ")");
    SPI_connect_my(buf.data);
    SPI_execute_with_args_my(buf.data, nargs, argtypes, values, NULL, SPI_OK_SELECT, true);
    for (uint64 row = 0; row < SPI_processed; row++) {
        Backend *backend;
        MemoryContext oldMemoryContext = MemoryContextSwitchTo(TopMemoryContext);
        const char *host = TextDatumGetCStringMy(SPI_getbinval_my(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, "host", false));
        const char *state = TextDatumGetCStringMy(SPI_getbinval_my(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, "state", false));
        D1("host = %s, state = %s", host, state);
        backend = palloc0(sizeof(*backend));
        MemoryContextSwitchTo(oldMemoryContext);
        backend->state = backend_state(state);
        backend_connect(backend, host, 5432, MyProcPort->user_name, MyProcPort->database_name, primary_set_synchronous_standby_names);
        pfree((void *)host);
        pfree((void *)state);
    }
    SPI_finish_my();
    for (int i = 0; i < nargs; i++) pfree((void *)values[i]);
    pfree(buf.data);
}

void primary_timeout(void) {
    if (!save_etcd_kv_put(default_state, hostname, 0)) {
        W("!save_etcd_kv_put");
        init_kill();
    }
    if (!save_etcd_kv_put(hostname, timestamptz_to_str(start), 0)) {
        W("!save_etcd_kv_put");
        init_kill();
    }
    queue_each(&backend_queue, queue) {
        Backend *backend = queue_data(queue, Backend, queue);
        if (PQstatus(backend->conn) == CONNECTION_BAD) backend_reset(backend, backend_idle, backend_finish);
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
    backend_alter_system_set("pg_save.primary", default_primary, hostname);
    backend_alter_system_set("pg_save.state", default_state, "primary");
    primary_schema("curl");
    primary_extension("curl", "pg_curl");
    primary_schema("save");
    primary_extension("save", "pg_save");
}

void primary_fini(void) {
    backend_fini();
}
