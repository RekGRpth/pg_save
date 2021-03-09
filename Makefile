EXTENSION = pg_save
MODULE_big = $(EXTENSION)
OBJS = init.o save.o spi.o primary.o standby.o backend.o recovery_gen.o string_utils.o
PG_CONFIG = pg_config
DATA = $(EXTENSION)--1.0.sql
SHLIB_LINK = $(libpq) -lpgcommon_shlib
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
