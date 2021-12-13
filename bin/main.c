#include "bin.h"

static char pg_hba_conf[MAXPGPATH];
static char postgresql_auto_conf[MAXPGPATH];
static char standby_signal[MAXPGPATH];
static const char *arclog;
static const char *cluster_name;
static const char *hostname;
static const char *pgdata;
static const char *primary;
static const char *primary_conninfo;
static const char *progname;

static void main_recovery(void) {
    FILE *file;
    PQExpBufferData buf;
    if (!(file = fopen(postgresql_auto_conf, "a"))) pg_log_error("fopen(\"%s\") and %m", postgresql_auto_conf);
    initPQExpBuffer(&buf);
    appendPQExpBuffer(&buf, "primary_conninfo = 'host=%s application_name=%s target_session_attrs=read-write'\n", primary, hostname);
    if (fwrite(buf.data, buf.len, 1, file) != 1) pg_log_error("fwrite != 1 and %m");
    termPQExpBuffer(&buf);
    fclose(file);
    if (!(file = fopen(standby_signal, "w"))) pg_log_error("fopen(\"%s\") and %m", standby_signal);
    fclose(file);
}

static void main_backup(void) {
    char tmp[] = "XXXXXX";
    char str[MAXPGPATH];
    snprintf(str, sizeof(str), CMD(
        pg_basebackup
            --dbname="host=%s application_name=%s target_session_attrs=read-write"
            --pgdata="%s"
            --progress
            --verbose
            --wal-method=stream
    ), primary, hostname, mktemp(tmp));
    if (pg_mkdir_p(tmp, pg_dir_create_mode) == -1) pg_log_error("pg_mkdir_p(\"%s\") == -1 and %m", tmp);
    pg_log_info("%s", str);
    if (system(str)) { rmtree(pgdata, true); pg_log_error("system(\"%s\") and %m", str); }
    rmtree(pgdata, true);
    if (rename(tmp, pgdata)) pg_log_error("rename(\"%s\", \"%s\") and %m", tmp, pgdata);
}

static void main_rewind(void) {
    char str[MAXPGPATH];
    snprintf(str, sizeof(str), CMD(
        pg_rewind
            --progress
            --restore-target-wal
            --source-server="host=%s application_name=%s target_session_attrs=read-write"
            --target-pgdata="%s"
    ), primary, hostname, pgdata);
    pg_log_info("%s", str);
    if (system(str)) main_backup();
    main_recovery();
}

static char *main_state(void) {
    char *line = NULL;
    FILE *file;
    size_t len = 0;
    ssize_t read;
    static char state[MAXPGPATH];
    if (!(file = fopen(postgresql_auto_conf, "r"))) pg_log_error("fopen(\"%s\") and %m", postgresql_auto_conf);
    while ((read = getline(&line, &len, file)) != -1) {
        if (read > sizeof("pg_save.state = '") - 1 && !strncmp(line, "pg_save.state = '", sizeof("pg_save.state = '") - 1)) {
            memcpy(state, line + sizeof("pg_save.state = '") - 1, read - (sizeof("pg_save.state = '") - 1) - 1 - 1);
            if (line) free(line);
            fclose(file);
            pg_log_info("state = %s", state);
            return state;
        }
    }
    if (line) free(line);
    fclose(file);
    return NULL;
}

static void main_update(void) {
    char str[MAXPGPATH];
    snprintf(str, sizeof(str), CMD(sed -i "/^primary_conninfo/cprimary_conninfo = 'host=%s application_name=%s target_session_attrs=read-write'" "%s"), primary, hostname, postgresql_auto_conf);
    pg_log_info("%s", str);
    if (system(str)) pg_log_error("system(\"%s\") and %m", str);
    snprintf(str, sizeof(str), CMD(sed -i "/^pg_save.primary/cpg_save.primary = '%s'" "%s"), primary, postgresql_auto_conf);
    pg_log_info("%s", str);
    if (system(str)) pg_log_error("system(\"%s\") and %m", str);
    snprintf(str, sizeof(str), CMD(sed -i "/^pg_save.wait_primary/cpg_save.wait_primary = '%s'" "%s"), primary, postgresql_auto_conf);
    pg_log_info("%s", str);
    if (system(str)) pg_log_error("system(\"%s\") and %m", str);
}

static void main_check(void) {
    const char *state;
    struct stat sb;
    if (!stat(standby_signal, &sb) && S_ISREG(sb.st_mode)) {
        if (primary) main_update();
    } else {
        if (!(state = main_state())) pg_log_error("!main_state");
        if (!strcmp(state, "wait_standby") && !primary) pg_log_error("pg_save.state == wait_standby && !primary");
        if (primary) main_rewind();
    }
}

static void main_conf(void) {
    FILE *file;
    PQExpBufferData buf;
    if (!(file = fopen(postgresql_auto_conf, "a"))) pg_log_error("fopen(\"%s\") and %m", postgresql_auto_conf);
    initPQExpBuffer(&buf);
    if (arclog) appendPQExpBuffer(&buf, CONF(
        archive_command = 'gzip -cfk "%%p" >"%s/%%f.gz"'\n
        archive_mode = 'on'\n
    ), arclog);
    if (cluster_name) appendPQExpBuffer(&buf, CONF(cluster_name = '%s'\n), cluster_name);
    appendPQExpBufferStr(&buf, CONF(
        datestyle = 'iso, dmy'\n
        hot_standby_feedback = 'on'\n
        listen_addresses = '*'\n
        max_logical_replication_workers = '0'\n
        max_sync_workers_per_subscription = '0'\n
        max_wal_senders = '3'\n
    ));
    if (arclog) appendPQExpBuffer(&buf, CONF(restore_command = 'gunzip -cfk "%s/%%f.gz" >"%%p"'\n), arclog);
#if PG_VERSION_NUM >= 130000
    appendPQExpBufferStr(&buf, CONF(
        shared_preload_libraries = 'pg_async,pg_save'\n
    ));
#elif PG_VERSION_NUM >= 120000
    appendPQExpBufferStr(&buf, CONF(
        shared_preload_libraries = 'pg_save'\n
    ));
#endif
    appendPQExpBufferStr(&buf, CONF(
        wal_compression = 'on'\n
        wal_level = 'replica'\n
        wal_log_hints = 'on'\n
        wal_receiver_create_temp_slot = 'on'\n
    ));
    if (fwrite(buf.data, buf.len, 1, file) != 1) pg_log_error("fwrite != 1 and %m");
    termPQExpBuffer(&buf);
    fclose(file);
}

static void main_hba(void) {
    FILE *file;
    PQExpBufferData buf;
    if (!(file = fopen(pg_hba_conf, "a"))) pg_log_error("fopen(\"%s\") and %m", pg_hba_conf);
    initPQExpBuffer(&buf);
    appendPQExpBufferStr(&buf, CONF(
        host all all samenet trust\n
        host replication all samenet trust\n
    ));
    if (fwrite(buf.data, buf.len, 1, file) != 1) pg_log_error("fwrite != 1 and %m");
    termPQExpBuffer(&buf);
    fclose(file);
}

static void main_initdb(void) {
    char str[MAXPGPATH];
    snprintf(str, sizeof(str), CMD(initdb --data-checksums --pgdata="%s"), pgdata);
    pg_log_info("%s", str);
    if (system(str)) pg_log_error("system(\"%s\") and %m", str);
}

static void main_init(void) {
    pg_log_info("host = %s", primary ? primary : "(null)");
    if (primary) {
        main_backup();
        main_recovery();
    } else {
        if (primary_conninfo) {
            size_t count = strlen(hostname);
            char *err;
            PQconninfoOption *opts;
            if (!(opts = PQconninfoParse(primary_conninfo, &err))) pg_log_error("!PQconninfoParse and %s", err);
            for (PQconninfoOption *opt = opts; opt->keyword; opt++) {
                if (!opt->val) continue;
                pg_log_info("%s = %s", opt->keyword, opt->val);
                if (strcmp(opt->keyword, "host")) continue;
                if (*(opt->val + count) == ',' && strncmp(opt->val, hostname, count)) pg_log_error("HOSTNAME = %s is not first in PRIMARY_CONNINFO = %s", hostname, primary_conninfo);
            }
            if (err) PQfreemem(err);
            PQconninfoFree(opts);
        }
        main_initdb();
        main_conf();
        main_hba();
    }
}

static char *main_primary(void) {
    static char primary[MAXPGPATH];
    PGconn *conn;
    PGresult *result;
    if (!primary_conninfo) return NULL;
    switch (PQping(primary_conninfo)) {
        case PQPING_NO_ATTEMPT: W("PQPING_NO_ATTEMPT"); return NULL;
        case PQPING_NO_RESPONSE: W("PQPING_NO_RESPONSE"); return NULL;
        case PQPING_OK: pg_log_info("PQPING_OK"); break;
        case PQPING_REJECT: W("PQPING_REJECT"); return NULL;
    }
    if (!(conn = PQconnectdb(primary_conninfo))) { W("!PQconnectdb and %s", PQerrorMessageMy(conn)); return NULL; }
    if (!(result = PQexec(conn, "SELECT current_setting('pg_save.hostname', false) AS hostname"))) { W("!PQexec and %s", PQerrorMessageMy(conn)); PQfinish(conn); return NULL; }
    if (PQresultStatus(result) != PGRES_TUPLES_OK) { W("PQresultStatus = %s and %s", PQresStatus(PQresultStatus(result)), PQresultErrorMessageMy(result)); PQclear(result); PQfinish(conn); return NULL; }
    if (PQntuples(result) != 1) { W("PQntuples != 1"); PQclear(result); PQfinish(conn); return NULL; }
    strcpy(primary, PQgetvalue(result, 0, PQfnumber(result, "hostname")));
    PQclear(result);
    PQfinish(conn);
    return primary;
}

int main(int argc, char *argv[]) {
    pg_logging_init(argv[0]);
    progname = get_progname(argv[0]);
    set_pglocale_pgservice(argv[0], PG_TEXTDOMAIN("pg_save"));
    if (!(hostname = getenv("HOSTNAME"))) pg_log_error("!getenv(\"HOSTNAME\")");
    if (!(pgdata = getenv("PGDATA"))) pg_log_error("!getenv(\"PGDATA\")");
    arclog = getenv("ARCLOG");
    primary_conninfo = getenv("PRIMARY_CONNINFO");
    cluster_name = getenv("CLUSTER_NAME");
    primary = main_primary();
    if (arclog) pg_log_info("arclog = '%s'", arclog);
    if (cluster_name) pg_log_info("cluster_name = '%s'", cluster_name);
    pg_log_info("hostname = '%s'", hostname);
    pg_log_info("pgdata = '%s'", pgdata);
    if (primary_conninfo) pg_log_info("primary_conninfo = '%s'", primary_conninfo);
    if (primary) pg_log_info("primary = '%s'", primary);
    if (pg_mkdir_p((char *)pgdata, pg_dir_create_mode) == -1) pg_log_error("pg_mkdir_p(\"%s\") == -1 and %m", pgdata);
    if (arclog) {
        char filename[MAXPGPATH];
        snprintf(filename, sizeof(filename), "%s/%s", pgdata, arclog);
        if (pg_mkdir_p(filename, pg_dir_create_mode) == -1) pg_log_error("pg_mkdir_p(\"%s\") == -1 and %m", filename);
    }
    snprintf(pg_hba_conf, sizeof(pg_hba_conf), "%s/%s", pgdata, "pg_hba.conf");
    snprintf(postgresql_auto_conf, sizeof(postgresql_auto_conf), "%s/%s", pgdata, "postgresql.auto.conf");
    snprintf(standby_signal, sizeof(standby_signal), "%s/%s", pgdata, "standby.signal");
    switch (pg_check_dir(pgdata)) {
        case 0: pg_log_error("directory \"%s\" does not exist", pgdata); break;
        case 1: pg_log_info("directory \"%s\" exists and empty", pgdata); main_init(); break;
        case 2: pg_log_error("directory \"%s\" exists and contains _only_ dot files", pgdata); break;
        case 3: pg_log_error("directory \"%s\" exists and contains a mount point", pgdata); break;
        case 4: pg_log_info("directory \"%s\" exists and not empty", pgdata); main_check(); break;
        case -1: pg_log_error("pg_check_dir(\"%s\") == -1 and %m", pgdata); break;
    }
    execlp("postmaster", "postmaster", NULL);
    pg_log_error("execlp(\"postmaster\")");
}

#if PG_VERSION_NUM >= 120000
#else
#include "logging.c"
#endif

#if PG_VERSION_NUM >= 110000
#else
#include "file_perm.c"
#endif
