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
    if (pg_mkdir_p(getenv("PGDATA"), pg_dir_create_mode) == -1) E("pg_mkdir_p(\"%s\") == -1 and %m", getenv("PGDATA"));
//    switch (pg_check_dir(getenv("PGDATA"))) {
//    }
}

int main(int argc, char *argv[]) {
    char *primary;
    pg_logging_init(argv[0]);
    I("PRIMARY_CONNINFO = %s", getenv("PRIMARY_CONNINFO"));
    //PRIMARY="$(chpst -u "$USER":"$GROUP" psql --quiet --tuples-only --no-align --dbname="$PRIMARY_CONNINFO" --command="SELECT current_setting('pg_save.hostname', false)" || echo)"
    primary = main_primary();
    I("host = %s", primary ? primary : "(null)");
    if (primary) free(primary);
    return 0;
}
