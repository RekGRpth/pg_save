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
}

static void main_init(void) {
    char *primary = main_primary();
    I("host = %s", primary ? primary : "(null)");
    if (primary) {
        main_backup(primary);
        free(primary);
    } else {
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
