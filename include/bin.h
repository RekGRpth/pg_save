#ifndef _BIN_H_
#define _BIN_H_

#include "common.h"

#define D(...) pg_log_debug(__VA_ARGS__)
#define E(...) pg_log_error(__VA_ARGS__)
#define F(...) pg_log_fatal(__VA_ARGS__)
#define I(...) pg_log_info(__VA_ARGS__)
#define W(...) pg_log_warning(__VA_ARGS__)

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
