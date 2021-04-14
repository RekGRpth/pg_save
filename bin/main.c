#include "bin.h"

static char *main_primary(void) {
    char *host;
    PGconn *conn;
    switch (PQping(getenv("PRIMARY_CONNINFO"))) {
        case PQPING_NO_ATTEMPT: W("PQPING_NO_ATTEMPT"); return NULL;
        case PQPING_NO_RESPONSE: W("PQPING_NO_RESPONSE"); return NULL;
        case PQPING_OK: I("PQPING_OK"); break;
        case PQPING_REJECT: W("PQPING_REJECT"); return NULL;
    }
    if (!(conn = PQconnectdb(getenv("PRIMARY_CONNINFO")))) { W("!PQconnectdb and %.*s", (int)strlen(PQerrorMessage(conn)) - 1, PQerrorMessage(conn)); return NULL; }
    host = strdup(PQhost(conn));
    PQfinish(conn);
    return host;
}

static void main_check(void) {
/*    char base[MAXPGPATH];
    struct stat buf;
    if (pg_mkdir_p(getenv("PGDATA"), pg_dir_create_mode) == -1) E("pg_mkdir_p(\"%s\") == -1 and %m", getenv("PGDATA"));
    snprintf(base, sizeof(base), "%s/base", getenv("PGDATA"));
    if (!stat(base, &buf) && S_ISDIR(buf.st_mode)) {
    } else {
    }
    switch (pg_check_dir(getenv("PGDATA"))) {
        case 0: I("directory \"%s\" does not exist", getenv("PGDATA")); break;
        case 1: I("directory \"%s\" exists and empty", getenv("PGDATA")); break;
        case 2: I("directory \"%s\" exists and contains _only_ dot files", getenv("PGDATA")); break;
        case 3: I("directory \"%s\" exists and contains a mount point", getenv("PGDATA")); break;
        case 4: I("directory \"%s\" exists and not empty", getenv("PGDATA")); break;
        case -1: I("pg_check_dir(\"%s\") == -1 and %m", getenv("PGDATA")); break;
    }*/
}

/*static char *main_dbname(const char *primary) {
    static char dbname[MAXPGPATH];
    snprintf(dbname, sizeof(dbname), "host=%s application_name=%s target_session_attrs=read-write", primary, getenv("HOSTNAME"));
    return dbname;
}*/

static void main_recovery(void) {
    FILE *file;
    char filename[MAXPGPATH];
    snprintf(filename, sizeof(filename), "%s/%s", getenv("PGDATA"), "postgresql.auto.conf");
    if (!(file = fopen(filename, "a"))) E("fopen(\"%s\") and %m", filename);
    fprintf(file, "primary_conninfo = '%s'\n", getenv("PRIMARY_CONNINFO"));
    fclose(file);
    snprintf(filename, sizeof(filename), "%s/%s", getenv("PGDATA"), "standby.signal");
    if (!(file = fopen(filename, "w"))) E("fopen(\"%s\") and %m", filename);
    fclose(file);
}

static void main_conf(void) {
    FILE *file;
    char filename[MAXPGPATH];
    snprintf(filename, sizeof(filename), "%s/%s", getenv("PGDATA"), "postgresql.auto.conf");
    if (!(file = fopen(filename, "a"))) E("fopen(\"%s\") and %m", filename);
    fprintf(file,
        "archive_command = 'test ! -f \"$ARCLOG/%%f\" && gzip -9 <\"%%p\" >\"$ARCLOG/%%f\" || echo \"$ARCLOG/%%f already exists!\"'"
        "archive_mode = 'on'"
        "auto_explain.log_analyze = 'on'"
        "auto_explain.log_buffers = 'on'"
        "auto_explain.log_min_duration = '100'"
        "auto_explain.log_nested_statements = 'on'"
        "auto_explain.log_triggers = 'on'"
        "auto_explain.log_verbose = 'on'"
        "cluster_name = '%s'"
        "datestyle = 'iso, dmy'"
        "hot_standby_feedback = 'on'"
        "listen_addresses = '*'"
        "log_connections = 'on'"
        "log_hostname = 'on'"
        "log_line_prefix = '%%m [%%p] %%r %%u@%%d/%%a '"
        "log_min_messages = 'debug1'"
        "max_logical_replication_workers = '0'"
        "max_sync_workers_per_subscription = '0'"
        "max_wal_senders = '3'"
        "restore_command = 'test -f \"$ARCLOG/%%f\" && gunzip <\"$ARCLOG/%%f\" >\"%%p\" || echo \"$ARCLOG/%%f does not exists!\"'"
        "shared_preload_libraries = 'auto_explain,pg_async,pg_save'"
        "trace_notify = 'on'"
        "wal_compression = 'on'"
        "wal_level = 'replica'"
        "wal_log_hints = 'on'"
        "wal_receiver_create_temp_slot = 'on'"
    , getenv("CLUSTER_NAME") ? getenv("CLUSTER_NAME") : "");
    fclose(file);
}

static void main_backup(const char *primary) {
    char pgdata[] = "XXXXXX";
    char str[MAXPGPATH];
    snprintf(str, sizeof(str), "pg_basebackup"
        "--dbname=\"host=%s application_name=%s target_session_attrs=read-write\""
        "--pgdata=\"%s\""
        "--progress"
        "--verbose"
        "--wal-method=stream", primary, getenv("HOSTNAME"), mktemp(pgdata));
    if (pg_mkdir_p(pgdata, pg_dir_create_mode) == -1) E("pg_mkdir_p(\"%s\") == -1 and %m", pgdata);
    if (system(str)) { rmtree(pgdata, true); E("system(\"%s\") and %m", str); }
    rmtree(getenv("PGDATA"), true);
    if (rename(pgdata, getenv("PGDATA"))) E("rename(\"%s\", \"%s\") and %m", pgdata, getenv("PGDATA"));
    main_recovery();
}

static void main_initdb(void) {
    char str[MAXPGPATH];
    snprintf(str, sizeof(str), "initdb --pgdata=\"%s\"", getenv("PGDATA"));
    if (system(str)) E("system(\"%s\") and %m", str);
}

static void main_init(void) {
    char *primary = main_primary();
    I("host = %s", primary ? primary : "(null)");
    if (primary) {
        main_backup(primary);
        free(primary);
    } else {
        size_t count = strlen(getenv("HOSTNAME"));
        char *err;
        PQconninfoOption *opts;
        if (!(opts = PQconninfoParse(getenv("PRIMARY_CONNINFO"), &err))) E("!PQconninfoParse and %s", err);
        for (PQconninfoOption *opt = opts; opt->keyword; opt++) {
            if (!opt->val) continue;
            I("%s = %s", opt->keyword, opt->val);
            if (strcmp(opt->keyword, "host")) continue;
            if (*(opt->val + count) == ',' && strncmp(opt->val, getenv("HOSTNAME"), count)) E("HOSTNAME = %s is not first in PRIMARY_CONNINFO = %s", getenv("HOSTNAME"), getenv("PRIMARY_CONNINFO"));
        }
        if (err) PQfreemem(err);
        PQconninfoFree(opts);
        main_initdb();
        main_conf();
//        main_hba();
    }
}

int main(int argc, char *argv[]) {
//    char *primary;
    pg_logging_init(argv[0]);
    I("PRIMARY_CONNINFO = %s", getenv("PRIMARY_CONNINFO"));
//    if (pg_mkdir_p(getenv("ARCLOG"), pg_dir_create_mode) == -1) E("pg_mkdir_p(\"%s\") == -1 and %m", getenv("ARCLOG"));
//    if (pg_mkdir_p(getenv("PGDATA"), pg_dir_create_mode) == -1) E("pg_mkdir_p(\"%s\") == -1 and %m", getenv("PGDATA"));
    switch (pg_check_dir(getenv("PGDATA"))) {
        case 0: E("directory \"%s\" does not exist", getenv("PGDATA")); break;
        case 1: I("directory \"%s\" exists and empty", getenv("PGDATA")); main_init(); break;
        case 2: E("directory \"%s\" exists and contains _only_ dot files", getenv("PGDATA")); break;
        case 3: E("directory \"%s\" exists and contains a mount point", getenv("PGDATA")); break;
        case 4: I("directory \"%s\" exists and not empty", getenv("PGDATA")); main_check(); break;
        case -1: E("pg_check_dir(\"%s\") == -1 and %m", getenv("PGDATA")); break;
    }
/*    //PRIMARY="$(chpst -u "$USER":"$GROUP" psql --quiet --tuples-only --no-align --dbname="$PRIMARY_CONNINFO" --command="SELECT current_setting('pg_save.hostname', false)" || echo)"
    primary = main_primary();
    I("host = %s", primary ? primary : "(null)");
    if (primary) free(primary);
    return 0;*/
}
