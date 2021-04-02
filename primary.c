#include "include.h"

extern char *backend_save;
extern char *init_policy;
extern int init_attempt;
extern queue_t save_queue;
extern state_t init_state;
static int primary_attempt = 0;

static void primary_set_synchronous_standby_names(void) {
    char **names = backend_names();
    int nelems = queue_size(&save_queue);
    StringInfoData buf;
    if (!names) return;
    initStringInfoMy(TopMemoryContext, &buf);
    appendStringInfo(&buf, "%s (", init_policy);
    for (int i = 0; i < nelems; i++) {
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
    primary_schema("async");
    primary_extension("async", "pg_async");
    primary_schema("curl");
    primary_extension("curl", "pg_curl");
    primary_schema("save");
    primary_extension("save", "pg_save");
    init_set_system("primary_conninfo", NULL);
    if (init_state <= state_unknown) init_set_state(state_initial);
}

static void primary_demote(Backend *backend) {
    init_state = state_unknown;
//    if (!etcd_kv_put(init_state2char(state_primary), "", 0)) W("!etcd_kv_put");
    if (kill(PostmasterPid, SIGKILL)) W("kill(%i ,%i)", PostmasterPid, SIGKILL);
}

void primary_notify(Backend *backend, const char *state) {
    if (backend->state == state_sync && !strcmp(state, "demote")) primary_demote(backend);
}

static void primary_result(void) {
    if (!SPI_processed) switch (init_state) {
        case state_initial: init_set_state(state_single); break;
        case state_primary: init_set_state(state_wait_primary); break;
        case state_single: break;
        default: E("init_state = %s", init_state2char(init_state)); break;
    } else switch (init_state) {
        case state_single: init_set_state(state_wait_primary); break;
        default: break;
    }
    for (uint64 row = 0; row < SPI_processed; row++) {
        char *host = TextDatumGetCStringMy(TopMemoryContext, SPI_getbinval_my(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, "application_name", false));
        char *state = TextDatumGetCStringMy(TopMemoryContext, SPI_getbinval_my(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, "sync_state", false));
        backend_result(host, init_char2state(state));
        pfree(host);
        pfree(state);
    }
}

void primary_timeout(void) {
    static Oid argtypes[] = {TEXTOID};
    Datum values[] = {backend_save ? CStringGetTextDatum(backend_save) : (Datum)NULL};
    char nulls[] = {backend_save ? ' ' : 'n'};
    static SPI_plan *plan = NULL;
    static char *command = "SELECT * FROM pg_stat_replication WHERE state = 'streaming' AND application_name NOT IN (SELECT unnest($1::text[]));";
    SPI_connect_my(command);
    if (!plan) plan = SPI_prepare_my(command, countof(argtypes), argtypes);
    SPI_execute_plan_my(plan, values, nulls, SPI_OK_SELECT, true);
    primary_result();
    SPI_finish_my();
    if (backend_save) pfree((void *)values[0]);
}

void primary_updated(Backend *backend) {
    primary_set_synchronous_standby_names();
}
