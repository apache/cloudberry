/* parallel_foreign_scan_test_fdw--1.0.sql */

CREATE FUNCTION parallel_foreign_scan_test_fdw_handler()
RETURNS fdw_handler
AS '$libdir/parallel_foreign_scan_test_fdw'
LANGUAGE C STRICT;

CREATE FOREIGN DATA WRAPPER parallel_foreign_scan_test_fdw
  HANDLER parallel_foreign_scan_test_fdw_handler;
