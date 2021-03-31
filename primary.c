#include "include.h"

extern char *backend_save;
extern char *init_policy;
extern char *init_sync;
extern int init_attempt;
extern queue_t save_queue;
extern STATE init_state;
static int primary_attempt = 0;

static void primary_set_synchronous_standby_names(void) {
    StringInfoData buf;
    char **names;
    int i = 0;
    if (!queue_size(&save_queue)) return;
    names = MemoryContextAlloc(TopMemoryContext, queue_size(&save_queue) * sizeof(*names));
    queue_each(&save_queue, queue) {
        Backend *backend = queue_data(queue, Backend, queue);
        names[i++] = (char *)PQhost(backend->conn);
    }
    pg_qsort(names, queue_size(&save_queue), sizeof(*names), pg_qsort_strcmp);
    initStringInfoMy(TopMemoryContext, &buf);
    appendStringInfo(&buf, "%s (", init_policy);
    for (int i = 0; i < queue_size(&save_queue); i++) {
        const char *name_quote = quote_identifier(names[i]);
        if (i) appendStringInfoString(&buf, ", ");
        appendStringInfoString(&buf, name_quote);
        if (name_quote != names[i]) pfree((void *)name_quote);
    }
    pfree(names);
    appendStringInfoString(&buf, ")");
    init_set_system("synchronous_standby_names", buf.data);
    pfree(buf.data);
}

static void primary_demote(void) {
    Backend *backend;
    if (queue_size(&save_queue) < 2) return;
    if (!(backend = backend_host(init_sync))) return;
    if (strcmp(PQhost(backend->conn), MyBgworkerEntry->bgw_type) > 0) return;
    W("%i < %i", primary_attempt, init_attempt);
    if (primary_attempt++ < init_attempt) return;
    init_state = UNKNOWN;
    if (!etcd_kv_put(init_state2char(PRIMARY), PQhost(backend->conn), 0)) W("!etcd_kv_put");
    if (kill(-PostmasterPid, SIGTERM)) W("kill");
}

void primary_connected(Backend *backend) {
    primary_set_synchronous_standby_names();
    primary_attempt = 0;
}

void primary_created(Backend *backend) {
}

void primary_failed(Backend *backend) {
    backend_finish(backend);
}

void primary_finished(Backend *backend) {
}

void primary_fini(void) {
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
    init_set_system("primary_conninfo", NULL);
    init_set_state(PRIMARY);
    primary_schema("curl");
    primary_extension("curl", "pg_curl");
    primary_schema("save");
    primary_extension("save", "pg_save");
    primary_schema("queue");
    primary_extension("queue", "pg_queue");
}

void primary_notify(Backend *backend, const char *channel, const char *payload, int32 srcPid) {
}

static void primary_result(void) {
    for (uint64 row = 0; row < SPI_processed; row++) {
        char *host = TextDatumGetCStringMy(TopMemoryContext, SPI_getbinval_my(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, "application_name", false));
        char *state = TextDatumGetCStringMy(TopMemoryContext, SPI_getbinval_my(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, "sync_state", true));
        backend_result(host, init_char2state(state));
        pfree(host);
        if (state) pfree(state);
    }
}

void primary_timeout(void) {
    static Oid argtypes[] = {TEXTOID};
    Datum values[] = {backend_save ? CStringGetTextDatum(backend_save) : (Datum)NULL};
    char nulls[] = {backend_save ? ' ' : 'n'};
    static SPI_plan *plan = NULL;
    static char *command = "SELECT application_name, s.sync_state\n"
        "FROM pg_stat_replication AS s FULL OUTER JOIN json_populate_recordset(NULL::record, $1::json) AS v (application_name text, sync_state text) USING (application_name)\n"
        "WHERE state = 'streaming' AND s.sync_state IS DISTINCT FROM v.sync_state";
    StaticAssertStmt(countof(argtypes) == countof(values), "countof(argtypes) == countof(values)");
    StaticAssertStmt(countof(argtypes) == countof(nulls), "countof(argtypes) == countof(values)");
    SPI_connect_my(command);
    if (!plan) plan = SPI_prepare_my(command, countof(argtypes), argtypes);
    SPI_execute_plan_my(plan, values, nulls, SPI_OK_SELECT, true);
    primary_result();
    SPI_finish_my();
    if (backend_save) pfree((void *)values[0]);
    primary_demote();
}

void primary_updated(Backend *backend) {
    primary_set_synchronous_standby_names();
}
