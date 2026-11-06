# Copyright (c) 2026, PostgreSQL Global Development Group

# SQL access belongs to an extension, including counters collected while the
# extended vacuum statistics are disabled. Check installation in another schema
# and removal/reinstallation without altering the system statistics views.
use strict;
use warnings FATAL => 'all';
use PostgresNode;
use TestLib;
use Test::More;

my $node = get_new_node('extension_catalog');
$node->init;
$node->append_conf('postgresql.conf', "autovacuum = off\n");
$node->start;

my $catalog_query = q{
SELECT c.relname, pg_get_viewdef(c.oid), a.attnum, a.attname, a.atttypid
FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum > 0
WHERE n.nspname = 'pg_catalog' AND c.relkind = 'v'
  AND (c.relname LIKE 'pg_stat_%' OR c.relname LIKE 'gp_stat_%')
ORDER BY c.relname, a.attnum
};
my $catalog_before = $node->safe_psql('postgres', $catalog_query);
$node->safe_psql('postgres', q{
CREATE SCHEMA maintenance;
CREATE EXTENSION vacuum_stats SCHEMA maintenance;
});
is($node->safe_psql('postgres', $catalog_query), $catalog_before,
   'installing the extension preserves system statistics view definitions and columns');
is($node->safe_psql('postgres', q{
SELECT count(*) FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
WHERE n.nspname = 'pg_catalog'
  AND p.proname IN ('pg_stat_get_frozen_page_marks_cleared',
    'pg_stat_get_visible_page_marks_cleared',
    'pg_stat_get_total_vacuum_time', 'pg_stat_get_total_autovacuum_time',
    'pg_stat_get_total_analyze_time', 'pg_stat_get_total_autoanalyze_time',
    'pg_stat_get_total_vacuum_delay_time', 'pg_stat_get_total_autovacuum_delay_time',
    'pg_stat_get_vacuum_failsafe_count',
    'pg_stat_get_db_frozen_page_marks_cleared',
    'pg_stat_get_db_visible_page_marks_cleared',
    'pg_stat_get_db_total_vacuum_time', 'pg_stat_get_db_total_autovacuum_time',
    'pg_stat_get_db_total_vacuum_delay_time', 'pg_stat_get_db_total_autovacuum_delay_time',
    'pg_stat_get_db_vacuum_failsafe_count', 'pg_stat_get_db_vacuum_interrupt_count')
}), '0', 'new statistics getters are not installed as built-in functions');

is($node->safe_psql('postgres', 'SHOW track_vacuum_statistics'), 'off',
   'ordinary maintenance timing is tested without extended statistics storage');
$node->safe_psql('postgres', q{
CREATE TABLE analyze_time (id int);
INSERT INTO analyze_time SELECT generate_series(1, 10000);
ANALYZE analyze_time;
});
$node->poll_query_until('postgres', q{
SELECT total_analyze_time > 0 FROM maintenance.pg_stat_vacuum_tables
WHERE relname = 'analyze_time'
}) or BAIL_OUT('ANALYZE timing report did not reach the collector');
my $analyze_query = q{
SELECT total_analyze_time, total_autoanalyze_time
FROM maintenance.pg_stat_vacuum_tables WHERE relname = 'analyze_time'
};
my $analyze_before = $node->safe_psql('postgres', $analyze_query);
is($node->safe_psql('postgres', q{
SELECT total_analyze_time > 0 AND total_autoanalyze_time = 0
FROM maintenance.gp_stat_vacuum_tables WHERE relname = 'analyze_time'
}), 't', 'cluster view resolves extension getters in a non-default schema');
is($node->safe_psql('postgres', q{
SELECT count(*) FROM maintenance.pg_stat_vacuum_database
WHERE datid = 0 AND datname IS NULL
}), '1', 'local database view exposes the shared-relation entry');
is($node->safe_psql('postgres', q{
SELECT count(*) FROM maintenance.gp_stat_vacuum_database
WHERE datid = 0 AND datname IS NULL
}), '1', 'cluster database view exposes the shared entry once in utility mode');

$node->safe_psql('postgres', 'SELECT maintenance.vacuum_stats_reset()');
# Drain the asynchronous reset before testing fields it must preserve.
$node->restart;
is($node->safe_psql('postgres', $analyze_query), $analyze_before,
   'dedicated vacuum reset preserves ANALYZE times');
$node->safe_psql('postgres', 'DROP EXTENSION vacuum_stats');
is($node->safe_psql('postgres', $catalog_query), $catalog_before,
   'dropping the extension preserves system statistics views');
$node->safe_psql('postgres', 'CREATE EXTENSION vacuum_stats SCHEMA maintenance');
is($node->safe_psql('postgres', $analyze_query), $analyze_before,
   'reinstalling the extension reads the same collector statistics');

$node->stop;
done_testing();
