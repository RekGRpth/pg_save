EXTENSION = pg_save
MODULE_big = $(EXTENSION)
OBJS = init.o save.o spi.o
PG_CONFIG = pg_config
DATA = $(EXTENSION)--1.0.sql
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
