#ifndef _BIN_H_
#define _BIN_H_

#include "common.h"

#define FORMAT_0(fmt, ...) "%s(%s:%d): %s", __func__, __FILE__, __LINE__, fmt
#define FORMAT_1(fmt, ...) "%s(%s:%d): " fmt,  __func__, __FILE__, __LINE__
#define GET_FORMAT(fmt, ...) GET_FORMAT_PRIVATE(fmt, 0, ##__VA_ARGS__, 1, \
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, \
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, \
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, \
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, \
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, \
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, \
    1, 1, 1, 1, 1, 1, 1, 1, 1, 0)
#define GET_FORMAT_PRIVATE(fmt, \
      _0,  _1,  _2,  _3,  _4,  _5,  _6,  _7,  _8,  _9, \
     _10, _11, _12, _13, _14, _15, _16, _17, _18, _19, \
     _20, _21, _22, _23, _24, _25, _26, _27, _28, _29, \
     _30, _31, _32, _33, _34, _35, _36, _37, _38, _39, \
     _40, _41, _42, _43, _44, _45, _46, _47, _48, _49, \
     _50, _51, _52, _53, _54, _55, _56, _57, _58, _59, \
     _60, _61, _62, _63, _64, _65, _66, _67, _68, _69, \
     _70, format, ...) FORMAT_ ## format(fmt)

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
