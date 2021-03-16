#include "include.h"

extern char *hostname;
extern char *init_policy;
extern int init_attempt;
extern queue_t backend_queue;
extern STATE init_state;

static void primary_set_synchronous_standby_names(void) {
    StringInfoData buf;
    char **names;
    int i = 0;
    if (!queue_size(&backend_queue)) return;
    names = MemoryContextAlloc(TopMemoryContext, queue_size(&backend_queue) * sizeof(*names));
    queue_each(&backend_queue, queue) {
        Backend *backend = queue_data(queue, Backend, queue);
        names[i++] = (char *)PQhost(backend->conn);
    }
    pg_qsort(names, queue_size(&backend_queue), sizeof(*names), pg_qsort_strcmp);
    initStringInfoMy(TopMemoryContext, &buf);
    appendStringInfo(&buf, "%s (", init_policy);
    for (int i = 0; i < queue_size(&backend_queue); i++) {
        const char *name_quote = quote_identifier(names[i]);
        if (i) appendStringInfoString(&buf, ", ");
        appendStringInfoString(&buf, name_quote);
        if (name_quote != names[i]) pfree((void *)name_quote);
    }
    pfree(names);
    appendStringInfoString(&buf, ")");
    init_alter_system_set("synchronous_standby_names", buf.data);
    pfree(buf.data);
}

void primary_connected(Backend *backend) {
    backend->attempt = 0;
    primary_set_synchronous_standby_names();
    init_set_remote_state(backend->state, PQhost(backend->conn));
    backend_idle(backend);
}

void primary_failed(Backend *backend) {
    backend_finish(backend);
}

void primary_finished(Backend *backend) {
    if (ShutdownRequestPending) return;
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
    initStringInfoMy(TopMemoryContext, &buf);
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
    initStringInfoMy(TopMemoryContext, &buf);
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
    init_alter_system_reset("primary_conninfo");
    init_alter_system_reset("primary_slot_name");
    init_set_remote_state(PRIMARY, hostname);
    init_set_local_state(PRIMARY);
    primary_schema("curl");
    primary_extension("curl", "pg_curl");
    primary_schema("save");
    primary_extension("save", "pg_save");
}

static void primary_result(void) {
    for (uint64 row = 0; row < SPI_processed; row++) {
        Backend *backend = NULL;
        const char *host = TextDatumGetCStringMy(TopMemoryContext, SPI_getbinval_my(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, "host", false));
        const char *state = TextDatumGetCStringMy(TopMemoryContext, SPI_getbinval_my(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, "state", false));
        D1("host = %s, state = %s", host, state);
        queue_each(&backend_queue, queue) {
            Backend *backend_ = queue_data(queue, Backend, queue);
            if (!strcmp(host, PQhost(backend_->conn))) { backend = backend_; break; }
        }
        backend ? backend_update(backend, init_char2state(state)) : backend_connect(host, init_char2state(state));
        pfree((void *)host);
        pfree((void *)state);
    }
}

static void primary_standby(void) {
    int nargs = 2 * queue_size(&backend_queue);
    Oid *argtypes = nargs ? MemoryContextAlloc(TopMemoryContext, nargs * sizeof(*argtypes)) : NULL;
    Datum *values = nargs ? MemoryContextAlloc(TopMemoryContext, nargs * sizeof(*values)) : NULL;
    StringInfoData buf;
    initStringInfoMy(TopMemoryContext, &buf);
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
        values[nargs] = CStringGetTextDatum(init_state2char(backend->state));
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
}
