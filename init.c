#include "include.h"

PG_MODULE_MAGIC;

char *init_async;
char *init_policy;
char *init_potential;
char *init_primary;
char *init_quorum;
char *init_state;
char *init_sync;
int init_probe;
int init_timeout;
static int init_restart;

static void save_work(void) {
    StringInfoData buf;
    BackgroundWorker worker;
    MemSet(&worker, 0, sizeof(worker));
    worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
    worker.bgw_restart_time = init_restart;
    worker.bgw_start_time = BgWorkerStart_ConsistentState;
    initStringInfo(&buf);
    appendStringInfoString(&buf, "pg_save");
    if (buf.len + 1 > BGW_MAXLEN) E("%i > BGW_MAXLEN", buf.len + 1);
    memcpy(worker.bgw_library_name, buf.data, buf.len);
    resetStringInfo(&buf);
    appendStringInfoString(&buf, "save_worker");
    if (buf.len + 1 > BGW_MAXLEN) E("%i > BGW_MAXLEN", buf.len + 1);
    memcpy(worker.bgw_function_name, buf.data, buf.len);
    resetStringInfo(&buf);
    appendStringInfoString(&buf, "pg_save");
    if (buf.len + 1 > BGW_MAXLEN) E("%i > BGW_MAXLEN", buf.len + 1);
    memcpy(worker.bgw_type, buf.data, buf.len);
    resetStringInfo(&buf);
    appendStringInfoString(&buf, "postgres postgres pg_save");
    if (buf.len + 1 > BGW_MAXLEN) E("%i > BGW_MAXLEN", buf.len + 1);
    memcpy(worker.bgw_name, buf.data, buf.len);
    pfree(buf.data);
    RegisterBackgroundWorker(&worker);
}

void _PG_init(void); void _PG_init(void) {
    if (IsBinaryUpgrade) { W("IsBinaryUpgrade"); return; }
    if (!process_shared_preload_libraries_in_progress) F("!process_shared_preload_libraries_in_progress");
    DefineCustomIntVariable("pg_save.probe", "pg_save probe", NULL, &init_probe, 30, 1, INT_MAX, PGC_SIGHUP, 0, NULL, NULL, NULL);
    D1("probe = %i", init_probe);
    DefineCustomIntVariable("pg_save.restart", "pg_save restart", NULL, &init_restart, 10, 1, INT_MAX, PGC_POSTMASTER, 0, NULL, NULL, NULL);
    D1("restart = %i", init_restart);
    DefineCustomIntVariable("pg_save.timeout", "pg_save timeout", NULL, &init_timeout, 1000, 1, INT_MAX, PGC_SIGHUP, 0, NULL, NULL, NULL);
    D1("timeout = %i", init_timeout);
    DefineCustomStringVariable("pg_save.async", "pg_save async", NULL, &init_async, "unknown", PGC_SIGHUP, 0, NULL, NULL, NULL);
    D1("async = %s", init_async);
    DefineCustomStringVariable("pg_save.policy", "pg_save policy", NULL, &init_policy, "FIRST 1", PGC_POSTMASTER, 0, NULL, NULL, NULL);
    D1("policy = %s", init_policy);
    DefineCustomStringVariable("pg_save.potential", "pg_save potential", NULL, &init_potential, "unknown", PGC_SIGHUP, 0, NULL, NULL, NULL);
    D1("potential = %s", init_potential);
    DefineCustomStringVariable("pg_save.primary", "pg_save primary", NULL, &init_primary, "unknown", PGC_SIGHUP, 0, NULL, NULL, NULL);
    D1("primary = %s", init_primary);
    DefineCustomStringVariable("pg_save.quorum", "pg_save quorum", NULL, &init_quorum, "unknown", PGC_SIGHUP, 0, NULL, NULL, NULL);
    D1("quorum = %s", init_quorum);
    DefineCustomStringVariable("pg_save.state", "pg_save state", NULL, &init_state, "unknown", PGC_SIGHUP, 0, NULL, NULL, NULL);
    D1("state = %s", init_state);
    DefineCustomStringVariable("pg_save.sync", "pg_save sync", NULL, &init_sync, "unknown", PGC_SIGHUP, 0, NULL, NULL, NULL);
    D1("sync = %s", init_sync);
    save_work();
}

void init_kill(void) {
#ifdef HAVE_SETSID
    if (kill(-PostmasterPid, SIGQUIT))
#else
    if (kill(PostmasterPid, SIGQUIT))
#endif
    E("kill");
}
