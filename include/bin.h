#ifndef _BIN_H_
#define _BIN_H_

#include "common.h"

#define D(fmt, ...) do { if (unlikely(__pg_log_level <= PG_LOG_DEBUG)) pg_log_generic(PG_LOG_DEBUG, GET_FORMAT(fmt, ##__VA_ARGS__), ##__VA_ARGS__); } while(0)
#define E(fmt, ...) do { if (likely(__pg_log_level <= PG_LOG_ERROR)) pg_log_generic(PG_LOG_ERROR, GET_FORMAT(fmt, ##__VA_ARGS__), ##__VA_ARGS__); exit(EXIT_FAILURE); } while(0)
#define F(fmt, ...) do { if (likely(__pg_log_level <= PG_LOG_FATAL)) pg_log_generic(PG_LOG_FATAL, GET_FORMAT(fmt, ##__VA_ARGS__), ##__VA_ARGS__); exit(EXIT_FAILURE); } while(0)
#define I(fmt, ...) do { if (likely(__pg_log_level <= PG_LOG_INFO)) pg_log_generic(PG_LOG_INFO, GET_FORMAT(fmt, ##__VA_ARGS__), ##__VA_ARGS__); } while(0)
#define W(fmt, ...) do { if (likely(__pg_log_level <= PG_LOG_WARNING)) pg_log_generic(PG_LOG_WARNING, GET_FORMAT(fmt, ##__VA_ARGS__), ##__VA_ARGS__); } while(0)

#include <postgres.h>

#if PG_VERSION_NUM >= 110000
#include <common/file_perm.h>
#else
#include "file_perm.h"
#endif
#if PG_VERSION_NUM >= 120000
#include <common/logging.h>
#else
#include "logging.h"
#endif
#if PG_VERSION_NUM >= 100000
#else
#if __GNUC__ >= 3
#define likely(x)	__builtin_expect((x) != 0, 1)
#define unlikely(x) __builtin_expect((x) != 0, 0)
#else
#define likely(x)	((x) != 0)
#define unlikely(x) ((x) != 0)
#endif
#endif
#include <pqexpbuffer.h>
#include <unistd.h>

#endif // _BIN_H_
