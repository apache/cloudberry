# Copyright (c) 2026, PostgreSQL Global Development Group

# Test that vacuums entering the wraparound failsafe mode are counted in
# pg_stat_vacuum_tables.vacuum_failsafe_count and aggregated per database in
# pg_stat_vacuum_database.vacuum_failsafe_count.
use strict;
use warnings FATAL => 'all';
use PostgresNode;
use TestLib;
use Test::More;

my $node = get_new_node('main');
$node->init;
# The failsafe cutoff is clamped to 1.05 * autovacuum_freeze_max_age, so use
# the minimum allowed value to keep the number of XIDs to burn small.
$node->append_conf(
	'postgresql.conf', qq[
autovacuum = off
autovacuum_freeze_max_age = 100000
]);
$node->start;
$node->safe_psql('postgres', 'CREATE EXTENSION vacuum_stats');

$node->safe_psql(
	'postgres', qq[
	CREATE TABLE tab_failsafe (i int);
	INSERT INTO tab_failsafe SELECT generate_series(1, 100);
]);

# A vacuum without failsafe pressure must not bump the counter.
$node->safe_psql('postgres', 'VACUUM FREEZE tab_failsafe;');
$node->poll_query_until('postgres', q{
SELECT vacuum_count = 1 FROM pg_stat_all_tables_internal WHERE relname = 'tab_failsafe'
}) or BAIL_OUT('normal vacuum report did not reach the collector');
my $count = $node->safe_psql('postgres',
	q[SELECT vacuum_failsafe_count FROM pg_stat_vacuum_tables WHERE relname = 'tab_failsafe';]
);
is($count, '0', 'aggressive VACUUM FREEZE does not count as failsafe');

# Age the table past 1.05 * autovacuum_freeze_max_age: burn XIDs with
# aborted subtransactions (each aborted subxact consumes an assigned XID).
$node->safe_psql(
	'postgres', qq[
	CREATE TABLE burn_xids (i int);
	DO \$\$
	BEGIN
		FOR i IN 1..110000 LOOP
			BEGIN
				INSERT INTO burn_xids VALUES (1);
				RAISE EXCEPTION 'burn';
			EXCEPTION WHEN OTHERS THEN
			END;
		END LOOP;
	END \$\$;
]);

# vacuum_failsafe_age = 0 makes the (clamped) cutoff kick in immediately.
$node->safe_psql(
	'postgres', qq[
	SET vacuum_failsafe_age = 0;
	SET vacuum_multixact_failsafe_age = 0;
	VACUUM tab_failsafe;
]);

$node->poll_query_until('postgres', q{
SELECT vacuum_failsafe_count = 1 FROM pg_stat_vacuum_tables WHERE relname = 'tab_failsafe'
}) or BAIL_OUT('failsafe vacuum report did not reach the collector');

$count = $node->safe_psql('postgres',
	q[SELECT vacuum_failsafe_count FROM pg_stat_vacuum_tables WHERE relname = 'tab_failsafe';]
);
is($count, '1', 'failsafe vacuum counted in pg_stat_vacuum_tables');

my $db_count = $node->safe_psql('postgres',
	q[SELECT vacuum_failsafe_count FROM pg_stat_vacuum_database WHERE datname = 'postgres';]
);
is($db_count, '1', 'failsafe vacuum counted in pg_stat_vacuum_database');

# Once the table has been frozen, another vacuum must not count the same
# failsafe event again.  Wait for that run's report, not for an unchanged value.
$node->safe_psql('postgres', 'VACUUM tab_failsafe');
$node->poll_query_until('postgres', q{
SELECT vacuum_count = 3 FROM pg_stat_all_tables_internal WHERE relname = 'tab_failsafe'
}) or BAIL_OUT('subsequent vacuum report did not reach the collector');
is($node->safe_psql('postgres', q{
SELECT t.vacuum_failsafe_count, d.vacuum_failsafe_count
FROM pg_stat_vacuum_tables t CROSS JOIN pg_stat_vacuum_database d
WHERE t.relname = 'tab_failsafe' AND d.datname = current_database()
}), '1|1', 'subsequent ordinary vacuum does not count the failsafe again');

$node->restart;
is($node->safe_psql('postgres', q{
SELECT t.vacuum_failsafe_count, d.vacuum_failsafe_count
FROM pg_stat_vacuum_tables t CROSS JOIN pg_stat_vacuum_database d
WHERE t.relname = 'tab_failsafe' AND d.datname = current_database()
}), '1|1', 'relation and database failsafe counts survive a clean restart');

$node->safe_psql('postgres',
    q{SELECT pg_stat_reset_single_table_counters('tab_failsafe'::regclass)});
$node->poll_query_until('postgres', q{
SELECT vacuum_failsafe_count = 0 FROM pg_stat_vacuum_tables WHERE relname = 'tab_failsafe'
}) or BAIL_OUT('relation failsafe counter was not reset');
is($node->safe_psql('postgres', q{
SELECT vacuum_failsafe_count FROM pg_stat_vacuum_database WHERE datname = current_database()
}), '1', 'resetting a relation preserves the database failsafe total');

$node->safe_psql('postgres', 'SELECT pg_stat_reset()');
ok($node->poll_query_until('postgres', q{
SELECT vacuum_failsafe_count = 0 FROM pg_stat_vacuum_database WHERE datname = current_database()
}), 'database reset clears the failsafe total');

$node->stop;
done_testing();
