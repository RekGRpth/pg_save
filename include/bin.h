#ifndef _BIN_H_
#define _BIN_H_

#include <postgres.h>

#include "common.h"
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
