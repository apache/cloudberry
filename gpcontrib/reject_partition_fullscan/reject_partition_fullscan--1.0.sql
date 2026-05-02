/* gpcontrib/reject_partition_fullscan/reject_partition_fullscan--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION reject_partition_fullscan" to load this file. \quit

-- Extension is loaded via shared_preload_libraries or LOAD command.
-- No SQL objects needed; the planner hook and GUCs are registered
-- automatically in _PG_init().
