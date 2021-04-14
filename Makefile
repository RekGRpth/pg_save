PG_CONFIG = pg_config
EXTENSION = pg_save

SUBDIRS = bin lib

all install installdirs uninstall distprep clean distclean maintainer-clean debug:
	@for dir in $(SUBDIRS); do \
		$(MAKE) -C $$dir $@ || exit; \
	done

# We'd like check operations to run all the subtests before failing.
check installcheck:
	@CHECKERR=0; for dir in $(SUBDIRS); do \
		$(MAKE) -C $$dir $@ || CHECKERR=$$?; \
	done; \
	exit $$CHECKERR

#EXTENSION = pg_save
#MODULE_big = $(EXTENSION)
#OBJS = init.o save.o spi.o primary.o standby.o backend.o varlena.o stringinfo.o
#PG_CONFIG = pg_config
#SHLIB_LINK = $(libpq)
#PGXS := $(shell $(PG_CONFIG) --pgxs)
#include $(PGXS)
