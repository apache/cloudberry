\getenv abs_builddir PG_ABS_BUILDDIR
\set test_hook :abs_builddir '/hooktest/test_hook.so'
LOAD :'test_hook';
-----------------------------------
-- Test planner hook
-----------------------------------
SET client_min_messages='log';
SELECT 1 AS a;
