REGRESS = dummy

REGRESS_OPTS = --schedule=diskquota_schedule_upgrade  --schedule=diskquota_schedule_downgrade --init-file=init_file
PGXS := $(shell pg_config --pgxs)
include $(PGXS)
