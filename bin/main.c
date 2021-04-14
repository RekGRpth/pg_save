#include "bin.h"

static void main_primary(void) {
    PGconn *conn;
    switch (PQping(getenv("PRIMARY_CONNINFO"))) {
        case PQPING_NO_ATTEMPT: W("PQPING_NO_ATTEMPT"); return;
        case PQPING_NO_RESPONSE: W("PQPING_NO_RESPONSE"); return;
        case PQPING_OK: I("PQPING_OK"); break;
        case PQPING_REJECT: W("PQPING_REJECT"); return;
    }
    if (!(conn = PQconnectdb(getenv("PRIMARY_CONNINFO")))) { W("!PQconnectdb and %.*s", (int)strlen(PQerrorMessage(conn)) - 1, PQerrorMessage(conn)); return; }
    I("host = %s", PQhost(conn));
    PQfinish(conn);
}

int main(int argc, char *argv[]) {
    pg_logging_init(argv[0]);
    I("PRIMARY_CONNINFO = %s", getenv("PRIMARY_CONNINFO"));
    //PRIMARY="$(chpst -u "$USER":"$GROUP" psql --quiet --tuples-only --no-align --dbname="$PRIMARY_CONNINFO" --command="SELECT current_setting('pg_save.hostname', false)" || echo)"
    main_primary();
    return 0;
}
