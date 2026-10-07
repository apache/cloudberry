# Copyright (c) 2026, PostgreSQL Global Development Group

# Test the total_vacuum_time counter of pg_stat_all_indexes: the time vacuum
# spent processing each index, accumulated over bulkdelete and cleanup passes,
# mirroring the table-level counter in pg_stat_all_tables.

use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('main');
$node->init;
$node->append_conf(
	'postgresql.conf', qq[
autovacuum = off
track_cost_delay_timing = on
]);
$node->start;

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

# The upper bound guards against garbage such as an epoch-based elapsed time
# leaking into the counter.
is( $node->safe_psql(
		'postgres', qq[
SELECT total_vacuum_time > 0 AND total_vacuum_time < 600000
  FROM pg_stat_all_indexes WHERE indexrelname = 'vactime_t_pkey']),
	't',
	'total_vacuum_time advanced sanely for the index in pg_stat_all_indexes');

is( $node->safe_psql(
		'postgres', qq[
SELECT total_vacuum_delay_time > 0 AND total_vacuum_delay_time <= total_vacuum_time
  FROM pg_stat_all_indexes WHERE indexrelname = 'vactime_t_pkey']),
	't',
	'total_vacuum_delay_time advanced and not above total_vacuum_time');

is( $node->safe_psql(
		'postgres', qq[
SELECT total_autovacuum_time = 0 FROM pg_stat_all_indexes
 WHERE indexrelname = 'vactime_t_pkey']),
	't',
	'manual vacuum did not count into total_autovacuum_time');

# The same run must have accumulated into the database-wide totals.
is( $node->safe_psql(
		'postgres', qq[
SELECT total_vacuum_time > 0 AND total_vacuum_time < 600000
   AND total_vacuum_delay_time > 0
   AND total_vacuum_delay_time <= total_vacuum_time
  FROM pg_stat_vacuum WHERE datname = current_database()]),
	't',
	'database-wide vacuum times advanced sanely in pg_stat_vacuum');

$node->stop;

done_testing();
