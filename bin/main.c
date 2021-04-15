#include "bin.h"

static const char *cluster_name;
static const char *hostname;
static const char *pgdata;
static const char *primary;
static const char *primary_conninfo;

static char *main_state(void) {
    char filename[MAXPGPATH];
    char *line = NULL;
    FILE *file;
    size_t len = 0;
    ssize_t read;
    static char state[MAXPGPATH];
    snprintf(filename, sizeof(filename), "%s/%s", pgdata, "postgresql.auto.conf");
    if (!(file = fopen(filename, "r"))) E("fopen(\"%s\") and %m", filename);
    while ((read = getline(&line, &len, file)) != -1) {
        if (read > sizeof("pg_save.state = '") - 1 && !strncmp(line, "pg_save.state = '", sizeof("pg_save.state = '") - 1)) {
            memcpy(state, line + sizeof("pg_save.state = '") - 1, read - (sizeof("pg_save.state = '") - 1) - 1);
            if (line) free(line);
            fclose(file);
            return state;
        }
    }
    if (line) free(line);
    fclose(file);
    return NULL;
}

static void main_update(void) {
    char *line = NULL;
    char namein[MAXPGPATH];
    char nameout[MAXPGPATH];
    FILE *fin, *fout;
    size_t len = 0;
    ssize_t read;
    snprintf(namein, sizeof(namein), "%s/%s", pgdata, "postgresql.auto.conf");
    snprintf(nameout, sizeof(nameout), "%s/%s", pgdata, "postgresql.auto.conf.new");
    if (!(fin = fopen(namein, "r"))) E("fopen(\"%s\") and %m", namein);
    if (!(fout = fopen(nameout, "w"))) E("fopen(\"%s\") and %m", nameout);
    while ((read = getline(&line, &len, fin)) != -1) {
        if (read > sizeof("primary_conninfo = '") - 1 && !strncmp(line, "primary_conninfo = '", sizeof("primary_conninfo = '") - 1)) {
            fprintf(fout, "primary_conninfo = '\"host=%s application_name=%s target_session_attrs=read-write\"'\n", primary, hostname);
        } else if (read > sizeof("pg_save.primary = '") - 1 && !strncmp(line, "pg_save.primary = '", sizeof("pg_save.primary = '") - 1)) {
            fprintf(fout, "pg_save.primary = '%s'\n", primary);
        } else if (read > sizeof("pg_save.wait_primary = '") - 1 && !strncmp(line, "pg_save.wait_primary = '", sizeof("pg_save.wait_primary = '") - 1)) {
            fprintf(fout, "pg_save.wait_primary = '%s'\n", primary);
        } else {
            fputs(line, fout);
        }
    }
    if (line) free(line);
    fclose(fout);
    fclose(fin);
    if (rename(nameout, namein)) E("rename(\"%s\", \"%s\") and %m", nameout, namein);
}

static void main_check(void) {
    char filename[MAXPGPATH];
    const char *state;
    struct stat sb;
    snprintf(filename, sizeof(filename), "%s/%s", pgdata, "standby.signal");
    if (!stat(filename, &sb) && S_ISREG(sb.st_mode)) {
        if (!primary) return;
        main_update();
    } else {
        if (!(state = main_state())) E("!main_state");
    }
}

static void main_conf(void) {
    FILE *file;
    char filename[MAXPGPATH];
    snprintf(filename, sizeof(filename), "%s/%s", pgdata, "postgresql.auto.conf");
    if (!(file = fopen(filename, "a"))) E("fopen(\"%s\") and %m", filename);
    fprintf(file,
        "archive_command = 'test ! -f \"$ARCLOG/%%f\" && gzip -9 <\"%%p\" >\"$ARCLOG/%%f\" || echo \"$ARCLOG/%%f already exists!\"'\n"
        "archive_mode = 'on'\n"
        "auto_explain.log_analyze = 'on'\n"
        "auto_explain.log_buffers = 'on'\n"
        "auto_explain.log_min_duration = '100'\n"
        "auto_explain.log_nested_statements = 'on'\n"
        "auto_explain.log_triggers = 'on'\n"
        "auto_explain.log_verbose = 'on'\n"
        "cluster_name = '%s'\n"
        "datestyle = 'iso, dmy'\n"
        "hot_standby_feedback = 'on'\n"
        "listen_addresses = '*'\n"
        "log_connections = 'on'\n"
        "log_hostname = 'on'\n"
        "log_line_prefix = '%%m [%%p] %%r %%u@%%d/%%a '\n"
        "log_min_messages = 'debug1'\n"
        "max_logical_replication_workers = '0'\n"
        "max_sync_workers_per_subscription = '0'\n"
        "max_wal_senders = '3'\n"
        "restore_command = 'test -f \"$ARCLOG/%%f\" && gunzip <\"$ARCLOG/%%f\" >\"%%p\" || echo \"$ARCLOG/%%f does not exists!\"'\n"
        "shared_preload_libraries = 'auto_explain,pg_async,pg_save'\n"
        "trace_notify = 'on'\n"
        "wal_compression = 'on'\n"
        "wal_level = 'replica'\n"
        "wal_log_hints = 'on'\n"
        "wal_receiver_create_temp_slot = 'on'\n"
    , cluster_name ? cluster_name : "");
    fclose(file);
}

static void main_hba(void) {
    FILE *file;
    char filename[MAXPGPATH];
    snprintf(filename, sizeof(filename), "%s/%s", pgdata, "pg_hba.conf");
    if (!(file = fopen(filename, "a"))) E("fopen(\"%s\") and %m", filename);
    fprintf(file,
        "host all all samenet trust\n"
        "host replication all samenet trust\n"
    );
    fclose(file);
}

static void main_initdb(void) {
    char str[MAXPGPATH];
    snprintf(str, sizeof(str), "initdb --pgdata=\"%s\"", pgdata);
    if (system(str)) E("system(\"%s\") and %m", str);
}

static void main_recovery(void) {
    FILE *file;
    char filename[MAXPGPATH];
    snprintf(filename, sizeof(filename), "%s/%s", pgdata, "postgresql.auto.conf");
    if (!(file = fopen(filename, "a"))) E("fopen(\"%s\") and %m", filename);
    fprintf(file, "primary_conninfo = '\"host=%s application_name=%s target_session_attrs=read-write\"'\n", primary, hostname);
    fclose(file);
    snprintf(filename, sizeof(filename), "%s/%s", pgdata, "standby.signal");
    if (!(file = fopen(filename, "w"))) E("fopen(\"%s\") and %m", filename);
    fclose(file);
}

static void main_backup(void) {
    char tmp[] = "XXXXXX";
    char str[MAXPGPATH];
    snprintf(str, sizeof(str), "pg_basebackup"
        "--dbname=\"host=%s application_name=%s target_session_attrs=read-write\""
        "--pgdata=\"%s\""
        "--progress"
        "--verbose"
        "--wal-method=stream", primary, hostname, mktemp(tmp));
    if (pg_mkdir_p(tmp, pg_dir_create_mode) == -1) E("pg_mkdir_p(\"%s\") == -1 and %m", tmp);
    if (system(str)) { rmtree(pgdata, true); E("system(\"%s\") and %m", str); }
    rmtree(pgdata, true);
    if (rename(tmp, pgdata)) E("rename(\"%s\", \"%s\") and %m", tmp, pgdata);
    main_recovery();
}

static void main_init(void) {
    I("host = %s", primary ? primary : "(null)");
    if (primary) {
        main_backup();
    } else {
        size_t count = strlen(hostname);
        char *err;
        PQconninfoOption *opts;
        if (!(opts = PQconninfoParse(primary_conninfo, &err))) E("!PQconninfoParse and %s", err);
        for (PQconninfoOption *opt = opts; opt->keyword; opt++) {
            if (!opt->val) continue;
            I("%s = %s", opt->keyword, opt->val);
            if (strcmp(opt->keyword, "host")) continue;
            if (*(opt->val + count) == ',' && strncmp(opt->val, hostname, count)) E("HOSTNAME = %s is not first in PRIMARY_CONNINFO = %s", hostname, primary_conninfo);
        }
        if (err) PQfreemem(err);
        PQconninfoFree(opts);
        main_initdb();
        main_conf();
        main_hba();
    }
}

static char *main_primary(void) {
    static char primary[MAXPGPATH];
    PGconn *conn;
    switch (PQping(primary_conninfo)) {
        case PQPING_NO_ATTEMPT: W("PQPING_NO_ATTEMPT"); return NULL;
        case PQPING_NO_RESPONSE: W("PQPING_NO_RESPONSE"); return NULL;
        case PQPING_OK: I("PQPING_OK"); break;
        case PQPING_REJECT: W("PQPING_REJECT"); return NULL;
    }
    if (!(conn = PQconnectdb(primary_conninfo))) { W("!PQconnectdb and %.*s", (int)strlen(PQerrorMessage(conn)) - 1, PQerrorMessage(conn)); return NULL; }
    strcpy(primary, PQhost(conn));
    PQfinish(conn);
    return primary;
}

int main(int argc, char *argv[]) {
    pg_logging_init(argv[0]);
    cluster_name = getenv("CLUSTER_NAME");
    hostname = getenv("HOSTNAME");
    pgdata = getenv("PGDATA");
    primary_conninfo = getenv("PRIMARY_CONNINFO");
    primary = main_primary();
    I("cluster_name = %s", cluster_name);
    I("hostname = %s", hostname);
    I("pgdata = %s", pgdata);
    I("primary_conninfo = %s", primary_conninfo);
    I("primary = %s", primary ? primary : "(null)");
    switch (pg_check_dir(pgdata)) {
        case 0: E("directory \"%s\" does not exist", pgdata); break;
        case 1: I("directory \"%s\" exists and empty", pgdata); main_init(); break;
        case 2: E("directory \"%s\" exists and contains _only_ dot files", pgdata); break;
        case 3: E("directory \"%s\" exists and contains a mount point", pgdata); break;
        case 4: I("directory \"%s\" exists and not empty", pgdata); main_check(); break;
        case -1: E("pg_check_dir(\"%s\") == -1 and %m", pgdata); break;
    }
    exit(EXIT_SUCCESS);
}
