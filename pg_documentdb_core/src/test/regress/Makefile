
BASEPATH:=../../../

REGRESS := 1

PG_CONFIG ?= pg_config

# export pg_config for child make commands (check etc)
export PG_CONFIG

PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

EXTENSIONLOAD := --load-extension=vector --load-extension=postgis --load-extension=documentdb_core

MAKEFILE_DIR := $(dir $(realpath $(firstword $(MAKEFILE_LIST))))
export PATH := $(MAKEFILE_DIR)bin:$(PATH)
export PG_REGRESS_DIFF_OPTS = -dU10

export PGISOLATIONTIMEOUT = 60

.PHONY: check-bson-basic check-bson-minimal

define common_test
	$(top_builddir)/src/test/regress/pg_regress --encoding=UTF8 --dlpath=$(BASEPATH) $(EXTENSIONLOAD) --temp-instance ./tmp --temp-config ./postgresql.conf --host localhost --port 58090 $(1) $(2) || (cat regression.diffs && false)
endef

define isolation_test
	$(top_builddir)/src/test/isolation/pg_isolation_regress --encoding=UTF8 --dlpath=$(BASEPATH) $(EXTENSIONLOAD) --temp-instance ./tmp --temp-config ./isolation_postgresql.conf --host localhost --port 58091 $(1) $(2) || (cat regression.diffs && false)
endef

check-basic:
	$(call common_test,--schedule=./basic_schedule)

check-bson-minimal:
	$(call common_test,--schedule=./minimal_schedule, $(EXTRA_TESTS))

all: check-basic
