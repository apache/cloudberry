CREATE TYPE diskquota.db_status AS (
	"id"   smallint,
	"dbid" oid,
	"workerid" smallint,
	"status" smallint,
	"last_run_time" timestamptz,
	"cost" smallint,
	"next_run_time" timestamptz,
	"epoch" int8 
);
CREATE FUNCTION diskquota.db_status() RETURNS setof diskquota.db_status AS '$libdir/diskquota-2.1.so',  'db_status' LANGUAGE C VOLATILE;
