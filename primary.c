#include "include.h"

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
    if (init_state == state_wait_primary) init_set_state(state_primary);
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

void primary_init(void) {
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
    if (init_state == state_wait_primary) init_set_state(state_primary);
}

static void primary_result(void) {
    if (!SPI_tuptable->numvals) switch (init_state) {
        case state_initial: init_set_state(state_single); break;
        case state_primary: init_set_state(state_wait_primary); break;
        case state_single: break;
        case state_wait_primary: break;
        default: E("init_state = %s", init_state2char(init_state)); break;
    } else switch (init_state) {
        case state_single: init_set_state(state_wait_primary); break;
        case state_wait_primary: break;
        case state_primary: break;
        default: E("init_state = %s", init_state2char(init_state)); break;
    }
    for (uint64 row = 0; row < SPI_tuptable->numvals; row++) {
        char *host = TextDatumGetCStringMy(TopMemoryContext, SPI_getbinval_my(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, "application_name", false));
        char *state = TextDatumGetCStringMy(TopMemoryContext, SPI_getbinval_my(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, "sync_state", false));
        backend_result(host, init_char2state(state));
        pfree(host);
        pfree(state);
    }
}

void primary_timeout(void) {
    static SPI_plan *plan = NULL;
    static char *command = "SELECT * FROM pg_stat_replication WHERE state = 'streaming'";
    SPI_connect_my(command);
    if (!plan) plan = SPI_prepare_my(command, 0, NULL);
    SPI_execute_plan_my(plan, NULL, NULL, SPI_OK_SELECT, false);
    primary_result();
    SPI_commit_my();
    SPI_finish_my();
}

void primary_updated(Backend *backend) {
    primary_set_synchronous_standby_names();
    if (init_state == state_wait_primary) init_set_state(state_primary);
}
