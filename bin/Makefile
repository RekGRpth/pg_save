PROGRAM = pg_save
OBJS = main.o ../fe-exec.o
PG_CONFIG = pg_config
PG_LIBS += $(libpq)
PG_LIBS += $(libpq_pgport)
PG_CPPFLAGS += -I$(libpq_srcdir)
PG_CPPFLAGS += -I../include
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
