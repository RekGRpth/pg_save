#ifndef _COMMON_H_
#define _COMMON_H_

#include <libpq-fe.h>
#include <string.h>

#define countof(array) (sizeof(array)/sizeof(array[0]))
#define CMD(...) #__VA_ARGS__
#define CONF(...) #__VA_ARGS__
#define SQL(...) #__VA_ARGS__

#define STATE_MAP(XX) \
    XX(unknown) \
    XX(initial) \
    XX(single) \
    XX(wait_primary) \
    XX(primary) \
    XX(wait_standby) \
    XX(sync) \
    XX(potential) \
    XX(quorum) \
    XX(async)

char *PQerrorMessageMy(const PGconn *conn);
char *PQresultErrorMessageMy(const PGresult *res);

#endif // _COMMON_H_
