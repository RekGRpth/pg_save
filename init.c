#include "include.h"

PG_MODULE_MAGIC;

static char *data;
static char *schema;
static char *table;
static char *user;
static int timeout;

static void save_work(void) {
    StringInfoData buf;
    BackgroundWorker worker;
    MemSet(&worker, 0, sizeof(worker));
    worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
    worker.bgw_restart_time = BGW_DEFAULT_RESTART_INTERVAL;
    worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
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
    appendStringInfo(&buf, "%s %s %s", user, data, worker.bgw_type);
    if (buf.len + 1 > BGW_MAXLEN) E("%i > BGW_MAXLEN", buf.len + 1);
    memcpy(worker.bgw_name, buf.data, buf.len);
    pfree(buf.data);
    RegisterBackgroundWorker(&worker);
}

void _PG_init(void); void _PG_init(void) {
    if (IsBinaryUpgrade) return;
    if (!process_shared_preload_libraries_in_progress) F("!process_shared_preload_libraries_in_progress");
    DefineCustomIntVariable("pg_save.timeout", "pg_save timeout", NULL, &timeout, 1000, 1, INT_MAX, PGC_SIGHUP, 0, NULL, NULL, NULL);
    DefineCustomStringVariable("pg_save.data", "pg_save data", NULL, &data, "postgres", PGC_POSTMASTER, 0, NULL, NULL, NULL);
    DefineCustomStringVariable("pg_save.schema", "pg_save schema", NULL, &schema, NULL, PGC_POSTMASTER, 0, NULL, NULL, NULL);
    DefineCustomStringVariable("pg_save.table", "pg_save table", NULL, &table, "save", PGC_POSTMASTER, 0, NULL, NULL, NULL);
    DefineCustomStringVariable("pg_save.user", "pg_save user", NULL, &user, "postgres", PGC_POSTMASTER, 0, NULL, NULL, NULL);
    save_work();
}
