$(OBJS): Makefile
OBJS = main.o ../fe-exec.o
PG_CONFIG = pg_config
PG_CPPFLAGS += -I$(libpq_srcdir)
PG_CPPFLAGS += -I../include
PG_LIBS += $(libpq)
PG_LIBS += $(libpq_pgport)
PGXS = $(shell $(PG_CONFIG) --pgxs)
PROGRAM = pg_save
include $(PGXS)
