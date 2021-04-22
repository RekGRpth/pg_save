#include "lib.h"

extern int init_attempt;
extern state_t init_state;
static int primary_attempt = 0;

void primary_connected(Backend *backend) {
    primary_attempt = 0;
    if (init_state == state_wait_primary) init_set_state(state_primary);
}

void primary_created(Backend *backend) {
}

void primary_failed(Backend *backend) {
    backend_finish(backend);
    if (init_state != state_primary) return;
    if (backend_nevents()) return;
    init_set_state(state_wait_standby);
    if (kill(PostmasterPid, SIGKILL)) W("kill(%i, %i)", PostmasterPid, SIGKILL);
}

void primary_finished(Backend *backend) {
}

void primary_fini(void) {
}

void primary_init(void) {
    init_set_system("primary_conninfo", NULL);
    switch (init_state) {
        case state_initial: break;
        case state_primary: break;
        case state_single: break;
        case state_unknown: init_set_state(state_initial); break;
        case state_wait_primary: break;
        default: E("init_state = %s", init_state2char(init_state)); break;
    }
}

void primary_notify(Backend *backend, state_t state) {
    switch (init_state) {
        case state_primary: break;
        case state_wait_primary: init_set_state(state_primary); break;
        default: E("init_state = %s", init_state2char(init_state)); break;
    }
}

static void primary_demote(void) {
    Backend *backend;
    if (backend_nevents() < 2) return;
    if (!(backend = backend_state(state_sync))) return;
    if (strcmp(backend->host, getenv("HOSTNAME")) > 0) return;
    W("%i < %i", primary_attempt, init_attempt);
    if (primary_attempt++ < init_attempt) return;
    init_set_state(state_wait_standby);
    if (kill(PostmasterPid, SIGKILL)) W("kill(%i, %i)", PostmasterPid, SIGKILL);
}

static void primary_result(void) {
    for (uint64 row = 0; row < SPI_tuptable->numvals; row++) {
        char *host = TextDatumGetCStringMy(TopMemoryContext, SPI_getbinval_my(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, "application_name", false));
        char *state = TextDatumGetCStringMy(TopMemoryContext, SPI_getbinval_my(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, "sync_state", false));
        backend_result(host, init_char2state(state));
        pfree(host);
        pfree(state);
    }
    if (!SPI_tuptable->numvals) switch (init_state) {
        case state_initial: init_set_state(state_single); break;
        case state_primary: init_set_state(state_wait_primary); break;
        case state_single: break;
        case state_wait_primary: break;
        default: E("init_state = %s", init_state2char(init_state)); break;
    } else switch (init_state) {
        case state_primary: break;
        case state_single: init_set_state(state_wait_primary); break;
        case state_wait_primary: init_set_state(state_primary); break;
        default: E("init_state = %s", init_state2char(init_state)); break;
    }
    init_reload();
}

void primary_timeout(void) {
    static SPI_plan *plan = NULL;
    static char *command = "SELECT * FROM pg_stat_replication WHERE state = 'streaming' AND NOT EXISTS (SELECT * FROM pg_stat_progress_basebackup)";
    SPI_connect_my(command);
    if (!plan) plan = SPI_prepare_my(command, 0, NULL);
    SPI_execute_plan_my(plan, NULL, NULL, SPI_OK_SELECT, false);
    primary_result();
    SPI_commit_my();
    SPI_finish_my();
    primary_demote();
}

void primary_updated(Backend *backend) {
    switch (init_state) {
        case state_primary: break;
        case state_single: init_set_state(state_wait_primary); break;
        case state_wait_primary: init_set_state(state_primary); break;
        default: E("init_state = %s", init_state2char(init_state)); break;
    }
}
