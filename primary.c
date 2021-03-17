#include "include.h"

extern char *hostname;
extern char *init_policy;
extern char *schema_type;
extern int init_attempt;
extern Oid type;
extern Oid type_array;
extern queue_t backend_queue;
extern STATE init_state;
static ArrayType *save;

static void primary_save(void) {
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
    primary_save();
}

void primary_failed(Backend *backend) {
    backend_finish(backend);
}

void primary_finished(Backend *backend) {
    if (ShutdownRequestPending) return;
    primary_set_synchronous_standby_names();
    primary_save();
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
        char *host = TextDatumGetCStringMy(TopMemoryContext, SPI_getbinval_my(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, "application_name", false));
        char *state = TextDatumGetCStringMy(TopMemoryContext, SPI_getbinval_my(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, "sync_state", true));
        backend_result(state, host);
        pfree(host);
        if (state) pfree(state);
    }
}

static void primary_standby(void) {
    Oid argtypes[] = {type_array};
    Datum values[] = {save ? PointerGetDatum(save) : (Datum)NULL};
    char nulls[] = {save ? ' ' : 'n'};
    static SPI_plan *plan = NULL;
    static char *command = NULL;
    StaticAssertStmt(countof(argtypes) == countof(values), "countof(argtypes) == countof(values)");
    StaticAssertStmt(countof(argtypes) == countof(nulls), "countof(argtypes) == countof(values)");
    if (!command) {
        StringInfoData buf;
        initStringInfoMy(TopMemoryContext, &buf);
        appendStringInfo(&buf,
            "SELECT application_name, s.sync_state\n"
            "FROM pg_stat_replication AS s FULL OUTER JOIN unnest($1::%1$s[]) AS v USING (application_name)\n"
            "WHERE state = 'streaming' AND s.sync_state IS DISTINCT FROM v.sync_state", schema_type);
        command = buf.data;
    }
    SPI_connect_my(command);
    if (!plan) plan = SPI_prepare_my(command, countof(argtypes), argtypes);
    SPI_execute_plan_my(plan, values, nulls, SPI_OK_SELECT, true);
    primary_result();
    SPI_finish_my();
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
    primary_save();
}
