# Copyright (c) 2026, PostgreSQL Global Development Group

# Test the total_vacuum_time counter of pg_stat_vacuum_indexes: the time vacuum
# spent processing each index, accumulated over bulkdelete and cleanup passes,
# mirroring the table-level counter in pg_stat_vacuum_tables.

use strict;
use warnings FATAL => 'all';
use PostgresNode;
use TestLib;
use Test::More;

my $node = get_new_node('main');
$node->init;
$node->append_conf(
	'postgresql.conf', qq[
autovacuum = off
track_cost_delay_timing = on
]);
$node->start;
$node->safe_psql('postgres', 'CREATE EXTENSION vacuum_stats');

# The index has to span enough pages for its vacuum passes to exceed
# vacuum_cost_limit on buffer hits alone (its pages are still dirty from the
# load, so dirtying them costs nothing).  Scale the row count with the block
# size, so that builds with larger blocks (32kB in Cloudberry) keep the page
# count of the default 8kB.
my $nrows = 100000 *
  ($node->safe_psql('postgres', 'SHOW block_size') / 8192);

# Enough rows that the bulkdelete pass over the index takes measurable time;
# a small vacuum_cost_delay adds deterministic delay time on fast machines.
$node->safe_psql(
	'postgres', qq[
CREATE TABLE vactime_t (id int PRIMARY KEY, v text) WITH (autovacuum_enabled = off);
INSERT INTO vactime_t SELECT g, repeat('x', 10) FROM generate_series(1, $nrows) g;
DELETE FROM vactime_t WHERE id % 2 = 0;
]);
$node->safe_psql(
	'postgres', qq[
SET vacuum_cost_delay = '1ms';
SET vacuum_cost_limit = 200;
VACUUM vactime_t;
]);

# The collector receives the ordinary vacuum and index-pass reports asynchronously.
$node->poll_query_until('postgres', q{
SELECT total_vacuum_time > 0 AND total_vacuum_delay_time > 0
FROM pg_stat_vacuum_indexes WHERE indexrelname = 'vactime_t_pkey'
}) or BAIL_OUT('index timing report did not reach the collector');
$node->poll_query_until('postgres', q{
SELECT total_vacuum_time > 0 AND total_vacuum_delay_time > 0
FROM pg_stat_vacuum_database WHERE datname = current_database()
}) or BAIL_OUT('database timing report did not reach the collector');

# The upper bound guards against garbage such as an epoch-based elapsed time
# leaking into the counter.
is( $node->safe_psql(
		'postgres', qq[
SELECT total_vacuum_time > 0 AND total_vacuum_time < 600000
  FROM pg_stat_vacuum_indexes WHERE indexrelname = 'vactime_t_pkey']),
	't',
	'total_vacuum_time advanced sanely for the index in pg_stat_vacuum_indexes');

is( $node->safe_psql(
		'postgres', qq[
SELECT total_vacuum_delay_time > 0 AND total_vacuum_delay_time <= total_vacuum_time
  FROM pg_stat_vacuum_indexes WHERE indexrelname = 'vactime_t_pkey']),
	't',
	'total_vacuum_delay_time advanced and not above total_vacuum_time');

is( $node->safe_psql(
		'postgres', qq[
SELECT total_autovacuum_time = 0 FROM pg_stat_vacuum_indexes
 WHERE indexrelname = 'vactime_t_pkey']),
	't',
	'manual vacuum did not count into total_autovacuum_time');

# The same run must have accumulated into the database-wide totals.
is( $node->safe_psql(
		'postgres', qq[
SELECT total_vacuum_time > 0 AND total_vacuum_time < 600000
   AND total_vacuum_delay_time > 0
   AND total_vacuum_delay_time <= total_vacuum_time
  FROM pg_stat_vacuum_database WHERE datname = current_database()]),
	't',
	'database-wide vacuum times advanced sanely in pg_stat_vacuum_database');

# The database total is the sum of table runs, which already include index
# work.  Adding the positive per-index time again would break this equality.
# Cast each millisecond value to numeric before summing to avoid floating-point
# rounding differences.  Shared relations belong to the database with OID zero.
is($node->safe_psql('postgres', q{
SELECT d.total_vacuum_time::numeric = t.elapsed
   AND d.total_vacuum_delay_time::numeric = t.delay
FROM pg_stat_vacuum_database d CROSS JOIN (
  SELECT sum(s.total_vacuum_time::numeric) AS elapsed,
         sum(s.total_vacuum_delay_time::numeric) AS delay
  FROM pg_stat_vacuum_tables s JOIN pg_class c ON c.oid = s.relid
  WHERE NOT c.relisshared
) t
WHERE d.datname = current_database()
}), 't', 'database totals include index work exactly once');

# Turning off delay timing must preserve the accumulated delay while elapsed
# time keeps advancing.  Keep cost delays enabled to exercise the timing GUC.
my $manual_times = q{
SELECT total_vacuum_time, total_vacuum_delay_time
FROM pg_stat_vacuum_indexes WHERE indexrelname = 'vactime_t_pkey'
};
my ($elapsed_before, $delay_before) =
  split /\|/, $node->safe_psql('postgres', $manual_times);
$node->safe_psql('postgres', q{
SET track_cost_delay_timing = off;
SET vacuum_cost_delay = '1ms';
SET vacuum_cost_limit = 200;
DELETE FROM vactime_t WHERE id % 4 = 1;
VACUUM (INDEX_CLEANUP ON) vactime_t;
});
$node->poll_query_until('postgres',
  "SELECT total_vacuum_time > $elapsed_before FROM pg_stat_vacuum_indexes WHERE indexrelname = 'vactime_t_pkey'")
  or BAIL_OUT('index elapsed time did not advance with delay timing disabled');
my ($elapsed_after, $delay_after) =
  split /\|/, $node->safe_psql('postgres', $manual_times);
is($delay_after, $delay_before, 'disabled delay timing preserves the index delay total');

# Exercise ANALYZE and VACUUM in the same backend, where the process-local
# delay accumulator survives between statements. Check their SQL totals
# independently rather than inferring one from the other.
# Drain earlier asynchronous vacuum reports before checking what ANALYZE
# alone changes.
$node->restart;
my $table_manual_times = q{
SELECT total_vacuum_time, total_vacuum_delay_time
FROM pg_stat_vacuum_tables WHERE relname = 'vactime_t'
};
my $db_manual_times = q{
SELECT total_vacuum_time, total_vacuum_delay_time
FROM pg_stat_vacuum_database WHERE datname = current_database()
};
my @timing_queries = ($table_manual_times, $manual_times, $db_manual_times);
my @timing_labels = ('table', 'index', 'database');
my @before_analyze = map { $node->safe_psql('postgres', $_) } @timing_queries;
$node->safe_psql('postgres', q{
SET vacuum_cost_delay = '1ms';
SET vacuum_cost_limit = 200;
ANALYZE vactime_t;
});
$node->poll_query_until('postgres', q{
SELECT total_analyze_time > 0 FROM pg_stat_vacuum_tables
WHERE relname = 'vactime_t'
}) or BAIL_OUT('ANALYZE timing report did not reach the collector');
for my $i (0..2)
{
    is($node->safe_psql('postgres', $timing_queries[$i]), $before_analyze[$i],
       "ANALYZE preserves $timing_labels[$i] vacuum elapsed and delay times");
}
my $analyze_before = $node->safe_psql('postgres', q{
SELECT total_analyze_time FROM pg_stat_vacuum_tables WHERE relname = 'vactime_t'
});
my ($table_elapsed, $table_delay) = split /\|/, $before_analyze[0];
$node->safe_psql('postgres', q{
SET vacuum_cost_delay = '1ms';
SET vacuum_cost_limit = 200;
ANALYZE vactime_t;
VACUUM (ANALYZE, DISABLE_PAGE_SKIPPING, INDEX_CLEANUP ON) vactime_t;
ANALYZE vactime_t;
});
$node->poll_query_until('postgres', qq{
SELECT total_vacuum_time > $table_elapsed
   AND total_vacuum_delay_time > $table_delay
   AND total_analyze_time > $analyze_before
FROM pg_stat_vacuum_tables WHERE relname = 'vactime_t'
}) or BAIL_OUT('VACUUM ANALYZE did not report both maintenance phases');
is($node->safe_psql('postgres', q{
SELECT total_vacuum_delay_time <= total_vacuum_time
   AND total_autovacuum_time = 0 AND total_autoanalyze_time = 0
FROM pg_stat_vacuum_tables WHERE relname = 'vactime_t'
}), 't', 'VACUUM ANALYZE records separate manual timing totals');

# Cloudberry autovacuums catalogs.  Churn comments to create dead catalog
# tuples and index entries, then wait for a real worker to process them.
$node->safe_psql('postgres', q{
CREATE TABLE vactime_autovacuum (id int) WITH (autovacuum_enabled = off);
COMMENT ON TABLE vactime_autovacuum IS 'initial';
VACUUM (INDEX_CLEANUP ON) pg_description;
});
$node->poll_query_until('postgres', q{
SELECT total_vacuum_time > 0 FROM pg_stat_vacuum_tables WHERE relname = 'pg_description'
}) or BAIL_OUT('catalog vacuum timing did not reach the collector');
my $table_time = "SELECT total_vacuum_time, total_autovacuum_time FROM pg_stat_vacuum_tables WHERE relname = 'pg_description'";
my $index_time = "SELECT total_vacuum_time, total_autovacuum_time FROM pg_stat_vacuum_indexes WHERE indexrelname = 'pg_description_o_c_o_index'";
my $db_time = "SELECT total_vacuum_time, total_autovacuum_time FROM pg_stat_vacuum_database WHERE datname = current_database()";
my @manual_before = map { (split /\|/, $node->safe_psql('postgres', $_))[0] }
  ($table_time, $index_time, $db_time);
$node->safe_psql('postgres', q{
DO $$ BEGIN
  FOR i IN 1..200 LOOP
    EXECUTE 'COMMENT ON TABLE vactime_autovacuum IS NULL';
    EXECUTE format('COMMENT ON TABLE vactime_autovacuum IS %L', i::text);
  END LOOP;
END $$;
ALTER SYSTEM SET autovacuum_naptime = '1s';
ALTER SYSTEM SET autovacuum_vacuum_threshold = 0;
ALTER SYSTEM SET autovacuum_vacuum_scale_factor = 0;
ALTER SYSTEM SET autovacuum_vacuum_insert_threshold = -1;
ALTER SYSTEM SET autovacuum = on;
});
$node->reload;
$node->poll_query_until('postgres', q{
SELECT total_autovacuum_time > 0 FROM pg_stat_vacuum_tables WHERE relname = 'pg_description'
}) or BAIL_OUT('autovacuum did not report catalog time');
$node->poll_query_until('postgres', q{
SELECT total_autovacuum_time > 0 FROM pg_stat_vacuum_indexes
WHERE indexrelname = 'pg_description_o_c_o_index'
}) or BAIL_OUT('autovacuum did not report catalog index time');
$node->safe_psql('postgres', 'ALTER SYSTEM SET autovacuum = off');
# Drain the collector on a clean shutdown before comparing exact totals.
# A worker disappearing from pg_stat_activity alone does not prove that
# its final UDP reports have reached the collector.
$node->restart;
my @auto_before;
my @queries = ($table_time, $index_time, $db_time);
my @labels = ('table', 'index', 'database');
for my $i (0..2)
{
    my ($manual, $automatic) = split /\|/, $node->safe_psql('postgres', $queries[$i]);
    is($manual, $manual_before[$i], "autovacuum preserves manual $labels[$i] time");
    cmp_ok($automatic, '>', 0, "autovacuum records separate $labels[$i] time");
    push @auto_before, $automatic;
}
$node->safe_psql('postgres', 'VACUUM (INDEX_CLEANUP ON) pg_description');
$node->poll_query_until('postgres',
    "SELECT total_vacuum_time > $manual_before[0] FROM pg_stat_vacuum_tables WHERE relname = 'pg_description'")
  or BAIL_OUT('manual catalog vacuum did not report new time');
for my $i (0..2)
{
    my ($manual, $automatic) = split /\|/, $node->safe_psql('postgres', $queries[$i]);
    cmp_ok($manual, '>', $manual_before[$i], "manual vacuum adds $labels[$i] time");
    is($automatic, $auto_before[$i], "manual vacuum preserves autovacuum $labels[$i] time");
}
my @before_restart = map { $node->safe_psql('postgres', $_) } @queries;
$node->restart;
for my $i (0..2)
{
    is($node->safe_psql('postgres', $queries[$i]), $before_restart[$i],
       "both $labels[$i] timing counters survive restart");
}

$node->stop;

done_testing();
