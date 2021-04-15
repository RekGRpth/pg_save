#ifndef _BIN_H_
#define _BIN_H_

#include "common.h"

#define D(fmt, ...) do { if (unlikely(__pg_log_level <= PG_LOG_DEBUG)) pg_log_generic(PG_LOG_DEBUG, GET_FORMAT(fmt, ##__VA_ARGS__), ##__VA_ARGS__); } while(0)
#define E(fmt, ...) do { if (likely(__pg_log_level <= PG_LOG_ERROR)) pg_log_generic(PG_LOG_ERROR, GET_FORMAT(fmt, ##__VA_ARGS__), ##__VA_ARGS__); exit(EXIT_FAILURE); } while(0)
#define F(fmt, ...) do { if (likely(__pg_log_level <= PG_LOG_FATAL)) pg_log_generic(PG_LOG_FATAL, GET_FORMAT(fmt, ##__VA_ARGS__), ##__VA_ARGS__); exit(EXIT_FAILURE); } while(0)
#define I(fmt, ...) do { if (likely(__pg_log_level <= PG_LOG_INFO)) pg_log_generic(PG_LOG_INFO, GET_FORMAT(fmt, ##__VA_ARGS__), ##__VA_ARGS__); } while(0)
#define W(fmt, ...) do { if (likely(__pg_log_level <= PG_LOG_WARNING)) pg_log_generic(PG_LOG_WARNING,GET_FORMAT(fmt, ##__VA_ARGS__), ##__VA_ARGS__); } while(0)

#include <postgres.h>

#include <common/file_perm.h>
#include <common/logging.h>
#include <libpq-fe.h>
#include <unistd.h>

#endif // _BIN_H_
