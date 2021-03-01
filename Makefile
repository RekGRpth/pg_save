MODULE_big = pg_save
OBJS = init.o save.o spi.o
PG_CONFIG = pg_config
DATA = pg_save--1.0.sql
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
