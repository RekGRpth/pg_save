#include "common.h"

char *PQerrorMessageMy(const PGconn *conn) {
    char *err = PQerrorMessage(conn);
    int len;
    if (!err) return err;
    len = strlen(err);
    if (!len) return err;
    if (err[len - 1] == '\n') err[len - 1] = '\0';
    return err;
}

char *PQresultErrorMessageMy(const PGresult *res) {
    char *err = PQresultErrorMessage(res);
    int len;
    if (!err) return err;
    len = strlen(err);
    if (!len) return err;
    if (err[len - 1] == '\n') err[len - 1] = '\0';
    return err;
}
