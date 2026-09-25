-- Resolving a volume: who may use it, whose credentials are used, and what
-- happens when a path does not belong to it.  A file volume needs no service
-- to talk to, so this runs everywhere.
SET client_min_messages = warning;
CREATE EXTENSION IF NOT EXISTS datalake_fdw;
CREATE EXTENSION IF NOT EXISTS datalake_fdw_test;
RESET client_min_messages;

COPY (SELECT 1) TO PROGRAM
	'rm -rf /tmp/datalake_fdw_volume && mkdir -p /tmp/datalake_fdw_volume/inside';

DROP SERVER IF EXISTS dlvol CASCADE;
DROP SERVER IF EXISTS dlvol_other CASCADE;
DROP ROLE IF EXISTS dlvol_user;
CREATE ROLE dlvol_user LOGIN;

CREATE SERVER dlvol FOREIGN DATA WRAPPER iceberg_volume_fdw
	OPTIONS (base_path 'file:///tmp/datalake_fdw_volume/inside');
CREATE SERVER dlvol_other FOREIGN DATA WRAPPER iceberg_volume_fdw
	OPTIONS (base_path 'file:///tmp/datalake_fdw_volume/elsewhere');

-- Through the volume, as its owner.
SELECT datalake_parquet_write('file:///tmp/datalake_fdw_volume/inside/a.parquet',
							  'SELECT 42 AS id', 0, '', 'dlvol') AS rows_written;
SELECT id FROM datalake_parquet_read(
	'file:///tmp/datalake_fdw_volume/inside/a.parquet', 0, 0, '{}', 'dlvol')
	AS t(id int);

-- A path outside the volume is refused, even though the volume would have
-- been happy to lend its settings to it.
SELECT datalake_parquet_write('file:///tmp/datalake_fdw_volume/outside.parquet',
							  'SELECT 1 AS id', 0, '', 'dlvol');
-- And so is one under a different volume.
SELECT datalake_parquet_write('file:///tmp/datalake_fdw_volume/inside/b.parquet',
							  'SELECT 1 AS id', 0, '', 'dlvol_other');

-- A volume that does not exist.
SELECT id FROM datalake_parquet_read(
	'file:///tmp/datalake_fdw_volume/inside/a.parquet', 0, 0, '{}',
	'no_such_volume') AS t(id int);

-- Using a volume takes USAGE on it.
GRANT EXECUTE ON FUNCTION datalake_parquet_read(text, int, int, int[], text)
	TO dlvol_user;
SET ROLE dlvol_user;
SELECT id FROM datalake_parquet_read(
	'file:///tmp/datalake_fdw_volume/inside/a.parquet', 0, 0, '{}', 'dlvol')
	AS t(id int);
RESET ROLE;

GRANT USAGE ON FOREIGN SERVER dlvol TO dlvol_user;
SET ROLE dlvol_user;
SELECT id FROM datalake_parquet_read(
	'file:///tmp/datalake_fdw_volume/inside/a.parquet', 0, 0, '{}', 'dlvol')
	AS t(id int);
RESET ROLE;

-- A PUBLIC mapping is what a user without one of their own gets.  The file
-- backend ignores credentials, so what is asserted is that resolution finds
-- the mapping and still reaches the file, not that the values did anything.
CREATE USER MAPPING FOR PUBLIC SERVER dlvol
	OPTIONS (access_key_id 'public-key', secret_access_key 'public-secret-value');
SET ROLE dlvol_user;
SELECT id FROM datalake_parquet_read(
	'file:///tmp/datalake_fdw_volume/inside/a.parquet', 0, 0, '{}', 'dlvol')
	AS t(id int);
RESET ROLE;

CREATE USER MAPPING FOR dlvol_user SERVER dlvol
	OPTIONS (access_key_id 'user-key', secret_access_key 'user-secret-value');
SET ROLE dlvol_user;
SELECT id FROM datalake_parquet_read(
	'file:///tmp/datalake_fdw_volume/inside/a.parquet', 0, 0, '{}', 'dlvol')
	AS t(id int);
RESET ROLE;

-- A base_path is quoted back when it is rejected, and a URI can carry a
-- password in its userinfo.  Neither the message nor the detail may reproduce
-- one: if this case ever prints "hunter2", the rejection leaked a credential
-- into the server log.
CREATE SERVER dlvol_secret FOREIGN DATA WRAPPER iceberg_volume_fdw
	OPTIONS (base_path 's3://reader:hunter2@bucket/prefix');
CREATE SERVER dlvol_signed FOREIGN DATA WRAPPER iceberg_volume_fdw
	OPTIONS (base_path 's3://bucket/prefix?X-Amz-Signature=deadbeef');

DROP SERVER dlvol CASCADE;
DROP SERVER dlvol_other CASCADE;
DROP ROLE dlvol_user;
COPY (SELECT 1) TO PROGRAM 'rm -rf /tmp/datalake_fdw_volume';
