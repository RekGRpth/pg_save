#include "include.h"

extern char *hostname;
extern char *init_policy;
extern char *init_primary;
extern char *init_state;
extern int init_attempt;
extern queue_t backend_queue;

static void primary_set_synchronous_standby_names(void) {
    StringInfoData buf;
    char **names;
    int i = 0;
    if (!queue_size(&backend_queue)) return;
    names = MemoryContextAlloc(TopMemoryContext, queue_size(&backend_queue) * sizeof(*names));
    queue_each(&backend_queue, queue) {
        Backend *backend = queue_data(queue, Backend, queue);
        names[i++] = (char *)backend->host;
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
    backend->attempt = 0;
    primary_set_synchronous_standby_names();
    init_set_state(backend->host, backend->state);
    backend_idle(backend);
}

void primary_finished(Backend *backend) {
    primary_set_synchronous_standby_names();
}

void primary_fini(void) {
    backend_fini();
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

void primary_init(void) {
    init_alter_system_reset("primary_conninfo", PrimaryConnInfo);
    init_alter_system_reset("primary_slot_name", PrimarySlotName);
    init_alter_system_set("pg_save.primary", init_primary, hostname);
    init_alter_system_set("pg_save.state", init_state, "primary");
    primary_schema("curl");
    primary_extension("curl", "pg_curl");
    primary_schema("save");
    primary_extension("save", "pg_save");
}

void primary_reseted(Backend *backend) {
    backend_finish(backend);
}

static void primary_result(void) {
    for (uint64 row = 0; row < SPI_processed; row++) {
        Backend *backend = NULL;
        const char *host = TextDatumGetCStringMy(SPI_getbinval_my(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, "host", false));
        const char *state = TextDatumGetCStringMy(SPI_getbinval_my(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, "state", false));
        D1("host = %s, state = %s", host, state);
        queue_each(&backend_queue, queue) {
            Backend *backend_ = queue_data(queue, Backend, queue);
            if (!strcmp(host, backend_->host)) { backend = backend_; break; }
        }
        backend ? backend_update(backend, state) : backend_connect(host, getenv("PGPORT") ? getenv("PGPORT") : DEF_PGPORT_STR, MyProcPort->user_name, MyProcPort->database_name, state);
        pfree((void *)host);
        pfree((void *)state);
    }
}

static void primary_standby(void) {
    int nargs = queue_size(&backend_queue);
    Oid *argtypes = nargs ? MemoryContextAlloc(TopMemoryContext, 2 * nargs * sizeof(*argtypes)) : NULL;
    Datum *values = nargs ? MemoryContextAlloc(TopMemoryContext, 2 * nargs * sizeof(*values)) : NULL;
    StringInfoData buf;
    initStringInfo(&buf);
    appendStringInfoString(&buf, "SELECT coalesce(client_hostname, client_addr::text) AS host, sync_state AS state FROM pg_stat_get_wal_senders() AS w INNER JOIN pg_stat_get_activity(pid) AS a USING (pid) WHERE w.state = 'streaming'");
    nargs = 0;
    queue_each(&backend_queue, queue) {
        Backend *backend = queue_data(queue, Backend, queue);
        appendStringInfoString(&buf, nargs ? ", " : " AND (client_addr, sync_state) NOT IN (");
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
    primary_result();
    SPI_finish_my();
    for (int i = 0; i < nargs; i++) pfree((void *)values[i]);
    pfree(buf.data);
}

void primary_timeout(void) {
    queue_each(&backend_queue, queue) {
        Backend *backend = queue_data(queue, Backend, queue);
        if (PQstatus(backend->conn) == CONNECTION_BAD) backend_reset(backend);
    }
    primary_standby();
}

void primary_updated(Backend *backend) {
    primary_set_synchronous_standby_names();
    init_set_state(backend->host, backend->state);
}
