# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# Test vacuum counter semantics, controls and lifecycle in one TAP suite.
# Adapt the repeatable-read and visibility-map scenarios from v44:
# https://www.postgresql.org/message-id/flat/cb305107-5935-4c34-9847-6ff0fef89f06%40yandex.ru#8b78d4ab399e37afc5eb2cee8a54d2f6
# This branch has a UDP collector and the older pg_stat_vacuum_* API:
# dead_tuples corresponds to recently_dead_tuples, and the page counters
# expose freezing and VM transitions. Polling uses a fresh connection, so
# it does not reuse a cached statistics snapshot. Core VM clearing semantics
# are covered by 004_visibility_map_stats.pl; here check vacuum
# reports, the extension views, controls and lifecycle.
# PostgresNode runs in utility/maintenance mode; use the local views.
# Read ordinary counters from pg_stat_all_tables_internal: the public
# pg_stat_all_tables gathers segment data and has no user-table rows here.

use strict;
use warnings;
use PostgresNode;
use TestLib;
use Test::More;

my $node = get_new_node('vacuum_stats');
$node->init;
$node->append_conf('postgresql.conf', q{
autovacuum = off
track_counts = on
vacuum_cost_delay = 0
});
$node->start;
$node->safe_psql('postgres', q{
CREATE EXTENSION vacuum_stats;
CREATE TABLE vstat_barrier (id int) WITH (autovacuum_enabled = off);
});

my @counter_names = qw(tuples_deleted dead_tuples pages_deleted bytes_removed dead_pages
  pages_frozen pages_all_visible frozen_page_marks_cleared visible_page_marks_cleared
  freeze_age_vacuum_count vacuum_failsafe_count total_vacuum_time total_autovacuum_time
  total_vacuum_delay_time total_autovacuum_delay_time);
my $all_counters = join(', ', @counter_names);
my $all_zero = join(' AND ', map { "$_ = 0" } @counter_names);
my $vacuum_zero = join(' AND ', map { "$_ = 0" } grep { !/_page_marks_cleared$|^total_.*time$|^vacuum_failsafe_count$/ } @counter_names);
my $vm_columns = 'frozen_page_marks_cleared, visible_page_marks_cleared';

sub wait_for_stats
{
	my ($sql, $description, $expected) = @_;
	$expected = 't' unless defined $expected;
	$node->poll_query_until('postgres', $sql, $expected)
	  or BAIL_OUT("timed out waiting for $description: $sql");
}

# VACUUM sends vacuum_count before its extended counters.  Instead of
# using that earlier report, send ANALYZE from the same backend AFTER the
# command and wait for its report.  ANALYZE adds no vacuum counters, so it
# also works for database totals, disabled tracking, resets and VACUUM FULL.
# DML counters are buffered separately; wait for the affected table's DML
# report explicitly when checking VM clearing counters.
sub run_and_wait
{
	my ($sql) = @_;
	my $count = $node->safe_psql('postgres',
		"SELECT analyze_count FROM pg_stat_all_tables_internal WHERE relname = 'vstat_barrier'");
	BAIL_OUT("missing local ANALYZE count for vstat_barrier")
	  unless $count =~ /^\d+$/;
	my ($result, $stdout, $stderr) = $node->psql('postgres',
		"$sql;\nANALYZE vstat_barrier;", on_error_die => 1);
	wait_for_stats(
		"SELECT analyze_count = $count + 1 FROM pg_stat_all_tables_internal WHERE relname = 'vstat_barrier'",
		"collector report after $sql");
	return $stderr;
}

sub wait_for_updates
{
	my ($table, $count) = @_;
	wait_for_stats(
		"SELECT n_tup_upd = $count FROM pg_stat_all_tables_internal WHERE relname = '$table'",
		"UPDATE report for $table");
}

sub populated_pages
{
	my ($table, $predicate) = @_;
	$predicate ||= 'true';
	return $node->safe_psql('postgres',
		"SELECT count(DISTINCT split_part(ctid::text, ',', 1)) FROM $table WHERE $predicate");
}

sub vacuum_table
{
	my ($table, $options, $settings) = @_;
	$options ||= '';
	$settings ||= '';
	return run_and_wait("$settings VACUUM $options $table");
}

sub counters
{
	my ($table, $columns) = @_;
	$columns ||= $all_counters;
	return $node->safe_psql('postgres',
		"SELECT $columns FROM pg_stat_vacuum_tables WHERE relname = '$table'");
}

sub index_counters
{
	my ($index, $columns) = @_;
	$columns ||= $all_counters;
	return $node->safe_psql('postgres',
		"SELECT $columns FROM pg_stat_vacuum_indexes WHERE indexrelname = '$index'");
}

sub database_counters
{
	my ($columns) = @_;
	$columns ||= $all_counters;
	return $node->safe_psql('postgres',
		"SELECT $columns FROM pg_stat_vacuum_database WHERE datname = 'postgres'");
}

# Read the same statistics snapshot before measuring its hash allocations.
sub snapshot_bytes
{
	return $node->safe_psql('postgres', q{
BEGIN;
DO $$ BEGIN PERFORM count(*) FROM pg_stat_all_tables_internal WHERE n_tup_ins >= 0; END $$;
SELECT sum(used_bytes) FROM pg_backend_memory_contexts
WHERE name IN ('Databases hash', 'Per-database table');
COMMIT;
});
}

subtest 'tracking disabled and enabled' => sub {
	$node->safe_psql('postgres', q{
CREATE TABLE vstat_off (id int) WITH (autovacuum_enabled = off);
INSERT INTO vstat_off SELECT generate_series(1, 1000);
DELETE FROM vstat_off;
});
	my $verbose = vacuum_table('vstat_off', 'VERBOSE');
	like($verbose, qr/table "vstat_off": vacuum statistics.*?cost-based delay: 0\.000 ms/s,
		'VERBOSE reports measurements even with statistics tracking disabled');
	is(counters('vstat_off', $vacuum_zero), 't',
		'extended counters stay zero while tracking is disabled');
	is(counters('vstat_off', 'total_vacuum_time > 0, total_autovacuum_time = 0'),
		't|t', 'core timing remains enabled independently of extended counters');
	is($node->safe_psql('postgres',
		"SELECT vacuum_count FROM pg_stat_all_tables_internal WHERE relname = 'vstat_off'"),
		'1', 'ordinary vacuum statistics are still collected');
	for my $am ('ao_row', 'ao_column')
	{
		my $table = "vstat_off_$am";
		$node->safe_psql('postgres', qq{
CREATE TABLE $table (id int) USING $am;
CREATE INDEX ${table}_idx ON $table (id);
INSERT INTO $table SELECT generate_series(1, 10000);
DELETE FROM $table WHERE id % 2 = 0;
});
		my $verbose = vacuum_table($table, 'VERBOSE', q{
SET track_cost_delay_timing = on;
SET vacuum_cost_delay = '1ms';
SET vacuum_cost_limit = 1;
});
		is(counters($table, "$vacuum_zero AND total_file_segs = 0"), 't',
			"$am extended counters and segment snapshot stay zero with tracking off");
		is(index_counters("${table}_idx", $vacuum_zero), 't',
			"$am index work counters stay zero with tracking off");
		is(counters($table, 'total_vacuum_time > 0 AND total_vacuum_delay_time > 0'),
			't', "$am table timing remains independent of extended tracking");
		is(index_counters("${table}_idx", 'total_vacuum_time > 0 AND total_vacuum_delay_time > 0'),
			't', "$am compaction index timing remains independent of extended tracking");
		like($verbose, qr/[1-9]\d* bytes truncated; \d+ file segments remain\./,
			"$am VERBOSE retains byte and segment measurements with tracking off");

		# The following run has no obsolete segments and uses scan_index().
		my $index_time = index_counters("${table}_idx", 'total_vacuum_time');
		vacuum_table($table);
		is(index_counters("${table}_idx", "total_vacuum_time > $index_time"), 't',
			"$am cleanup-only index timing advances with extended tracking off");
		is(index_counters("${table}_idx", $vacuum_zero), 't',
			"$am cleanup-only index work counters remain disabled");
	}
	is(database_counters($vacuum_zero), 't',
		'heap and AO work do not populate extended database totals with tracking off');
	# VM changes are ordinary DML statistics even with vacuum tracking off.
	$node->safe_psql('postgres', q{
CREATE TABLE vstat_vm_off (id int PRIMARY KEY) WITH (autovacuum_enabled = off);
INSERT INTO vstat_vm_off SELECT generate_series(1, 1000);
});
	vacuum_table('vstat_vm_off', 'FREEZE');
	my $pages = $node->safe_psql('postgres',
		"SELECT pg_relation_size('vstat_vm_off') / current_setting('block_size')::bigint");
	my ($db_frozen, $db_visible) = split /\|/, database_counters($vm_columns);
	$node->safe_psql('postgres', 'UPDATE vstat_vm_off SET id = id + 1000');
	wait_for_updates('vstat_vm_off', 1000);
	is(counters('vstat_vm_off', $vm_columns), "$pages|$pages",
		'DML counts exact VM clearings while vacuum tracking is off');
	is(database_counters("frozen_page_marks_cleared - $db_frozen, visible_page_marks_cleared - $db_visible"),
		"$pages|$pages", 'database VM totals receive the same disabled-tracking DML report');
	is(counters('vstat_vm_off', $vacuum_zero), 't',
		'VM collection does not enable vacuum counters');
	$node->restart;
	is(counters('vstat_vm_off', $vm_columns), "$pages|$pages",
		'VM counters survive a clean restart with tracking off');
	# dynahash allocates entries in batches.  With only a few relations,
	# a larger entry can use a slightly smaller batch and appear cheaper.
	# Populate enough ordinary entries to exceed that allocation rounding;
	# none of these relations has been vacuumed.
	$node->safe_psql('postgres', q{
CREATE SCHEMA vstat_memory;
DO $$ BEGIN
    FOR i IN 1..1024 LOOP
        EXECUTE format('CREATE TABLE vstat_memory.t%s (id int) WITH (autovacuum_enabled = off)', i);
        EXECUTE format('INSERT INTO vstat_memory.t%s VALUES (1)', i);
    END LOOP;
END $$;
});
	wait_for_stats(q{
SELECT count(*) = 1024 AND bool_and(n_tup_ins = 1)
FROM pg_stat_all_tables_internal WHERE schemaname = 'vstat_memory'
}, 'ordinary statistics for the memory-allocation test');
	my $off_bytes = snapshot_bytes();
	is($node->safe_psql('postgres',
		"SELECT context FROM pg_settings WHERE name = 'track_vacuum_statistics'"),
		'postmaster', 'tracking is fixed at server startup');
	my ($result, $stdout, $stderr) = $node->psql('postgres',
		'SET track_vacuum_statistics = on');
	is($result, 3, 'a session cannot enable tracking');
	like($stderr, qr/cannot be changed without restarting the server/,
		'the error explains that a restart is required');
	$node->safe_psql('postgres', 'ALTER SYSTEM SET track_vacuum_statistics = on');
	$node->reload;
	wait_for_stats(q{SELECT pending_restart FROM pg_settings WHERE name = 'track_vacuum_statistics'},
		'configuration reload to notice the startup setting');
	is($node->safe_psql('postgres', 'SHOW track_vacuum_statistics'), 'off',
		'reload leaves tracking disabled');
	$node->restart;
	is($node->safe_psql('postgres', 'SHOW track_vacuum_statistics'), 'on',
		'restart enables tracking');
	is(counters('vstat_vm_off', $vm_columns), "$pages|$pages",
		'enabling vacuum tracking preserves VM counters collected while off');
	is(counters('vstat_off', $vacuum_zero), 't',
		'new vacuum blocks start at zero when reading ordinary-only statistics');
	cmp_ok(snapshot_bytes(), '>', $off_bytes,
		'enabled snapshots allocate vacuum counters with ordinary relation and DB entries');

};

subtest 'removed tuples and truncated pages' => sub {
	$node->safe_psql('postgres', q{
CREATE TABLE vstat_heap (id int PRIMARY KEY) WITH (autovacuum_enabled = off);
INSERT INTO vstat_heap SELECT generate_series(1, 10000);
DELETE FROM vstat_heap WHERE id % 2 = 0;
});
	is(counters('vstat_heap', $all_zero), 't',
		'all counters are zero before the first vacuum');
	# Heap insertion can pre-extend the relation with empty tail pages.
	# Disable truncation in this run to make its zero counter deterministic.
	my $verbose = vacuum_table('vstat_heap', '(VERBOSE, TRUNCATE false)');
	is(counters('vstat_heap', 'tuples_deleted, dead_tuples, pages_deleted, dead_pages'),
		'5000|0|0|0', 'vacuum removes exactly half the rows without truncation');
	like($verbose, qr/pages with dead tuples not yet removable: 0\n/,
		'VERBOSE reports zero pages retaining dead tuples');
	like($verbose, qr/pages with tuples frozen: 0\n/,
		'VERBOSE reports zero pages frozen with the default freeze age');
	my $visible = counters('vstat_heap', 'pages_all_visible');
	like($verbose, qr/pages marked all-visible: \Q$visible\E\n/,
		'VERBOSE all-visible count matches the first vacuum report');
	like($verbose, qr/scanned index "vstat_heap_pkey".*?cost-based delay: 0\.000 ms/s,
		'VERBOSE reports the index cost delay');
	is(index_counters('vstat_heap_pkey', 'tuples_deleted, pages_deleted'),
		'5000|0', 'every other index key remains; no index pages are deleted');
	is(counters('vstat_heap', 'total_vacuum_time > 0, total_autovacuum_time = 0, total_vacuum_delay_time = 0'),
		't|t|t', 'heap vacuum takes time but has no cost delay');
	is(index_counters('vstat_heap_pkey', 'total_vacuum_time > 0, total_autovacuum_time = 0, total_vacuum_delay_time = 0'),
		't|t|t', 'index vacuum takes time but has no cost delay');

	$node->safe_psql('postgres', 'DELETE FROM vstat_heap');
	my $pages_before = $node->safe_psql('postgres',
		"SELECT pg_relation_size('vstat_heap') / current_setting('block_size')::bigint");
	my $quiet = vacuum_table('vstat_heap');
	unlike($quiet, qr/vacuum statistics|pages marked all-visible|cost-based delay/,
		'ordinary VACUUM does not emit VERBOSE statistics at the default message level');
	is(counters('vstat_heap', 'tuples_deleted, dead_tuples, pages_deleted'),
		"10000|0|$pages_before", 'the second vacuum removes the remaining rows and truncates every heap page');
	is(counters('vstat_heap', "bytes_removed = pages_deleted * current_setting('block_size')::bigint"),
		't', 'heap truncation reports exact bytes');
	is(index_counters('vstat_heap_pkey', 'tuples_deleted, pages_deleted > 0'),
		'10000|t', 'index tuple and page deletion counters accumulate');
};

subtest 'cluster views return local rows once in utility mode' => sub {
	is($node->safe_psql('postgres', 'SHOW gp_role'), 'utility',
		'this scenario uses a direct utility connection');
	for my $view (
		['tables', 'relid, schemaname, relname', "relname = 'vstat_heap'"],
		['indexes', 'relid, indexrelid, schemaname, relname, indexrelname',
		 "relname = 'vstat_heap'"],
		['database', 'datid, datname', "datname = 'postgres'"])
	{
		my ($suffix, $keys, $filter) = @$view;
		my $columns = "$keys, $all_counters";
		$columns .= ', total_analyze_time, total_autoanalyze_time' if $suffix eq 'tables';
		$columns .= ', total_file_segs' if $suffix eq 'tables';
		$columns .= ', vacuum_interrupt_count' if $suffix eq 'database';
		my $local = "SELECT $columns FROM pg_stat_vacuum_$suffix WHERE $filter";
		my $cluster = "SELECT $columns FROM gp_stat_vacuum_$suffix WHERE $filter";
		is($node->safe_psql('postgres', qq{
SELECT count(*) = 1 AND bool_and(gp_segment_id = gp_execution_segment())
FROM gp_stat_vacuum_$suffix WHERE $filter
}), 't', "$suffix view returns one row identified by the connected node");
		is($node->safe_psql('postgres', qq{
SELECT NOT EXISTS (
    ($cluster EXCEPT ALL $local)
    UNION ALL
    ($local EXCEPT ALL $cluster)
)
}), 't', "$suffix cluster and local views have identical rows and counters");
	}
};

subtest 'a repeatable-read snapshot prevents removal' => sub {
	$node->safe_psql('postgres', q{
CREATE TABLE vstat_snapshot (id int PRIMARY KEY, val int)
    WITH (autovacuum_enabled = off);
INSERT INTO vstat_snapshot SELECT i, i FROM generate_series(1, 1000) g(i);
});
	my $dead_pages = populated_pages('vstat_snapshot', 'id > 900');
	my ($in, $out) = ('', '');
	my $timer = IPC::Run::timeout($TestLib::timeout_default);
	my $reader = $node->background_psql('postgres', \$in, \$out, $timer);
	$out = '';
	$in = "BEGIN ISOLATION LEVEL REPEATABLE READ;\n"
	  . "SELECT count(*) FROM vstat_snapshot;\n\\echo snapshot_ready\n";
	pump_until($reader, $timer, \$out, qr/^snapshot_ready\r?$/m)
	  or BAIL_OUT('reader did not acquire its snapshot');
	like($out, qr/^1000\r?$/m, 'reader sees all original rows');

	# Updating the indexed column prevents HOT, making index removals exact.
	$node->safe_psql('postgres', 'UPDATE vstat_snapshot SET id = id + 1000 WHERE id > 900');
	my $verbose = vacuum_table('vstat_snapshot', 'VERBOSE');
	is(counters('vstat_snapshot', 'tuples_deleted, dead_tuples, dead_pages, pages_frozen'),
		"0|100|$dead_pages|0", '100 old tuple versions remain on exactly the affected pages');
	like($verbose, qr/pages with dead tuples not yet removable: \Q$dead_pages\E\n/,
		'VERBOSE reports the pages held back by the snapshot');
	is(index_counters('vstat_snapshot_pkey', 'tuples_deleted'),
		'0', 'index entries needed by the reader are retained');

	$in = "COMMIT;\n\\q\n";
	$reader->finish;
	vacuum_table('vstat_snapshot');
	is(counters('vstat_snapshot', 'tuples_deleted, dead_tuples, pages_frozen'),
		'100|100|0', 'after commit, 100 versions are removed; cumulative dead_tuples stays at 100');
	is(index_counters('vstat_snapshot_pkey', 'tuples_deleted'), '100',
		'after commit, the index removes exactly the 100 obsolete entries');
};

subtest 'heap page reports and core VM counters in extension views' => sub {
	$node->safe_psql('postgres', q{
CREATE TABLE vstat_vm (id int PRIMARY KEY, val int)
    WITH (autovacuum_enabled = off, fillfactor = 50);
INSERT INTO vstat_vm SELECT i, i FROM generate_series(1, 5000) g(i);
});
	my $pages = populated_pages('vstat_vm');
	my $verbose = vacuum_table('vstat_vm', 'VERBOSE',
		'SET vacuum_freeze_min_age = 1000000000; SET vacuum_freeze_table_age = 1000000000;');
	is(counters('vstat_vm', 'pages_frozen, pages_all_visible, frozen_page_marks_cleared, visible_page_marks_cleared'),
		"0|$pages|0|0", 'ordinary vacuum marks each populated page visible without freezing it');
	like($verbose, qr/pages with tuples frozen: 0\n/,
		'VERBOSE reports no freezing with a high freeze age');
	like($verbose, qr/pages marked all-visible: \Q$pages\E\n/,
		'VERBOSE reports the exact number of newly visible pages');

	# Leave enough room for new versions on the same pages and prevent HOT.
	# The distinct counter values detect swapped columns in extension views.
	$node->safe_psql('postgres', 'UPDATE vstat_vm SET id = id + 10000');
	wait_for_updates('vstat_vm', 5000);
	# Do not scan the heap here: that could prune the obsolete versions before
	# VACUUM gets to count their removal.
	is($node->safe_psql('postgres',
		"SELECT pg_relation_size('vstat_vm') / current_setting('block_size')::bigint"),
		$pages, 'updated versions fit on the original pages');
	is(counters('vstat_vm', 'frozen_page_marks_cleared, visible_page_marks_cleared'),
		"0|$pages", 'extension view reports only all-visible clearings for unfrozen pages');

	$verbose = vacuum_table('vstat_vm', '(FREEZE, VERBOSE)');
	my $visible = 2 * $pages;
	is(counters('vstat_vm', 'tuples_deleted, pages_frozen, pages_all_visible'),
		"5000|$pages|$visible", 'FREEZE reports removals, freezing and restored visibility separately');
	is(counters('vstat_vm', 'frozen_page_marks_cleared, visible_page_marks_cleared'),
		"0|$pages", 'restoring VM flags adds no clearings');
	like($verbose, qr/pages with tuples frozen: \Q$pages\E\n/,
		'VERBOSE reports exactly one freeze per populated page');
	like($verbose, qr/pages marked all-visible: \Q$pages\E\n/,
		'VERBOSE reports this run\'s visibility work, not the cumulative total');
	is(counters('vstat_vm', 'freeze_age_vacuum_count'), '1',
		'FREEZE counts the vacuum made aggressive by the freeze age');
	like($verbose, qr/aggressive scan required by freeze age: yes/,
		'VERBOSE explains freeze-age-driven aggressive scanning');

	$verbose = vacuum_table('vstat_vm', '(FREEZE, VERBOSE)');
	is(counters('vstat_vm', 'pages_frozen, pages_all_visible'),
		"$pages|$visible", 'another FREEZE adds no freezing or visibility work');
	like($verbose, qr/pages with tuples frozen: 0\n/,
		'VERBOSE reports no repeated freezing');
	like($verbose, qr/pages marked all-visible: 0\n/,
		'VERBOSE reports no repeated visibility changes');

	$node->safe_psql('postgres', 'DELETE FROM vstat_vm');
	wait_for_stats(q{
SELECT n_tup_del = 5000 FROM pg_stat_all_tables_internal WHERE relname = 'vstat_vm'
}, 'DELETE report for frozen pages');
	is(counters('vstat_vm', 'frozen_page_marks_cleared, visible_page_marks_cleared'),
		"$pages|$visible", 'extension view exposes both core VM counters with distinct totals');
};

subtest 'append-optimized compaction' => sub {
	for my $am ('ao_row', 'ao_column')
	{
		my $table = "vstat_$am";
		$node->safe_psql('postgres', qq{
CREATE TABLE $table (id int, val int) USING $am;
CREATE INDEX ${table}_idx ON $table (id);
INSERT INTO $table SELECT i, i FROM generate_series(1, 10000) g(i);
DELETE FROM $table WHERE id % 2 = 0;
});
		# Preserve the zero-freeze-age case: AO still must not count a
		# freeze-age vacuum, unlike its auxiliary heap relations.
		my $verbose = vacuum_table($table, 'VERBOSE', 'SET vacuum_freeze_table_age = 0;');
		is(counters($table, 'tuples_deleted, dead_tuples, pages_frozen, pages_all_visible, freeze_age_vacuum_count'),
			'5000|0|0|0|0', "$am compaction removes 5000 rows and has no heap VM or freezing work");
		# Compaction relocates surviving tuples, so all original index
		# entries, including those of surviving rows, become obsolete.
		is(index_counters("${table}_idx", 'tuples_deleted'),
			'10000', "$am index cleanup removes the old TIDs");
		is(counters($table, 'total_vacuum_time > 0, total_autovacuum_time = 0, total_vacuum_delay_time = 0'),
			't|t|t', "$am compaction takes time without cost delay");
		my $bytes = counters($table, 'bytes_removed');
		my $segrel = $node->safe_psql('postgres',
			"SELECT segrelid::regclass FROM pg_appendonly WHERE relid = '$table'::regclass");
		my $segs = $node->safe_psql('postgres', "SELECT count(*) FROM $segrel");
		is(counters($table, 'total_file_segs'), $segs,
			"$am reports the remaining segment metadata entries");
		is($node->safe_psql('postgres',
			"SELECT total_file_segs FROM gp_stat_vacuum_tables WHERE relname = '$table'"),
			$segs, "$am cluster view exposes the segment snapshot in utility mode");
		like($verbose, qr/\Q$bytes bytes truncated; $segs file segments remain.\E/,
			"$am VERBOSE byte and segment counts match the report");
		like($verbose,
			qr/append-optimized table "\Q$table\E": vacuum statistics\nDETAIL:  \Q0 dead tuples remain.\E\n\d+ bytes truncated; \d+ file segments remain\.\nelapsed: \d+\.\d{3} ms, cost-based delay: 0\.000 ms/,
			"$am VERBOSE reports remaining dead tuples and accumulated phase time");
		is(counters($table, 'pages_deleted > 0, dead_pages, frozen_page_marks_cleared, visible_page_marks_cleared'),
			't|0|0|0', "$am reports freed space without heap page or VM counters");

		# Without obsolete segment files, AO still runs index cleanup.
		# Its time must be reported even if the AM returns no page statistics.
		my $index_time = index_counters("${table}_idx", 'total_vacuum_time');
		vacuum_table($table);
		is(counters($table, 'bytes_removed, total_file_segs'), "$bytes|$segs",
			"$am idle vacuum neither adds reclaimed bytes nor sums segment counts");
		is(index_counters("${table}_idx", "tuples_deleted, total_vacuum_time > $index_time, total_vacuum_delay_time = 0"),
			'10000|t|t', "$am reports index cleanup without deleting more TIDs");

		$node->safe_psql('postgres', "DELETE FROM $table WHERE id <= 200");
		vacuum_table($table, '', 'SET gp_appendonly_compaction = off;');
		is(counters($table, 'tuples_deleted, dead_tuples'),
			'5000|100', "$am counts hidden rows left when compaction is disabled");
	}
};

subtest 'AO truncate reports exact bytes beyond the 32-bit boundary' => sub {
	my $block_size = $node->safe_psql('postgres', 'SHOW block_size');
	for my $am ('ao_row', 'ao_column')
	{
		my $table = "vstat_tail_$am";
		$node->safe_psql('postgres', qq{
CREATE TABLE $table (id int) USING $am;
INSERT INTO $table SELECT generate_series(1, 10000);
CHECKPOINT;
});
		my $relpath = $node->safe_psql('postgres',
			"SELECT pg_relation_filepath('$table')");
		my @files = grep { -f $_ && -s $_ }
			glob($node->data_dir . '/' . $relpath . '*');
		my ($file) = grep { /\Q$relpath\E(?:\.\d+)?$/ } @files;
		defined($file) or BAIL_OUT("no data file for $table");
		my $original_size = -s $file;
		my $tail_size = 2**31 + 17;
		# Model an aborted insert's tail in this disposable cluster with a
		# sparse file. This exercises real truncate without writing 2 GiB.
		open(my $fh, '+<', $file) or die "$file: $!";
		truncate($fh, $original_size + $tail_size) or die "truncate: $!";
		close($fh) or die "close: $!";
		vacuum_table($table);
		is(-s $file, $original_size, "$am removes the physical tail");
		my $pages = int(($tail_size + $block_size - 1) / $block_size);
		is(counters($table, 'bytes_removed, pages_deleted'), "$tail_size|$pages",
			"$am preserves exact bytes and rounds the block equivalent up");
		my $segs = counters($table, 'total_file_segs');
		cmp_ok($segs, '>', 0, "$am has a segment snapshot");
		$node->restart;
		is(counters($table, 'bytes_removed, pages_deleted, total_file_segs'),
			"$tail_size|$pages|$segs", "$am counters and state survive restart");
		vacuum_table($table);
		is(counters($table, 'bytes_removed, pages_deleted, total_file_segs'),
			"$tail_size|$pages|$segs", "$am repeated vacuum does not recount the tail or segments");
		run_and_wait("SELECT vacuum_stats_reset('$table'::regclass::oid)");
		is(counters($table, 'bytes_removed, total_file_segs'), '0|0',
			"$am relation reset clears both cumulative bytes and segment state");
	}
};

subtest 'SP-GiST does not recount reusable pages' => sub {
	for my $am ('heap', 'ao_row', 'ao_column')
	{
		my $table = "vstat_spg_$am";
		$node->safe_psql('postgres', qq{
CREATE TABLE $table (id int, p point) USING $am;
CREATE INDEX ${table}_idx ON $table USING spgist (p);
INSERT INTO $table SELECT i, point(i,i) FROM generate_series(1, 10000) g(i);
DELETE FROM $table;
});
		vacuum_table($table, '(INDEX_CLEANUP ON)');
		my $pages = index_counters("${table}_idx", 'pages_deleted');
		is(index_counters("${table}_idx", 'tuples_deleted'), '10000',
			"$am SP-GiST removes the original index entries");
		vacuum_table($table, '(INDEX_CLEANUP ON)');
		is(index_counters("${table}_idx", 'pages_deleted'), $pages,
			"$am SP-GiST does not count already empty pages again");
		is(index_counters("${table}_idx", 'bytes_removed'), '0',
			"$am SP-GiST page reuse returns no bytes to the filesystem");
	}
};

subtest 'VACUUM FULL leaves the extended counters unchanged' => sub {
	$node->safe_psql('postgres', q{
CREATE TABLE vstat_full (id int PRIMARY KEY) WITH (autovacuum_enabled = off);
INSERT INTO vstat_full SELECT generate_series(1, 10000);
DELETE FROM vstat_full WHERE id % 2 = 0;
});
	vacuum_table('vstat_full');
	is(counters('vstat_full', 'tuples_deleted'), '5000',
		'plain vacuum establishes nonzero counters');
	$node->safe_psql('postgres', 'DELETE FROM vstat_full');
	wait_for_stats(
		"SELECT n_tup_del = 10000 FROM pg_stat_all_tables_internal WHERE relname = 'vstat_full'",
		'DELETE report before VACUUM FULL');
	my $table_before = counters('vstat_full');
	my $index_before = index_counters('vstat_full_pkey');
	run_and_wait('VACUUM FULL vstat_full');
	is(counters('vstat_full'), $table_before, 'VACUUM FULL leaves table counters unchanged');
	is(index_counters('vstat_full_pkey'), $index_before, 'VACUUM FULL leaves index counters unchanged');
};

subtest 'cost-based delay is part of total time' => sub {
	$node->safe_psql('postgres', q{
CREATE TABLE vstat_delay (id int) WITH (autovacuum_enabled = off);
INSERT INTO vstat_delay SELECT generate_series(1, 10000);
DELETE FROM vstat_delay;
});
	# Session-local settings do not leak into later scenarios.
	my $verbose = vacuum_table('vstat_delay', 'VERBOSE',
		'SET track_cost_delay_timing = on; SET vacuum_cost_delay = 1; SET vacuum_cost_limit = 1;');
	is(counters('vstat_delay', 'total_vacuum_delay_time > 0 AND total_vacuum_delay_time <= total_vacuum_time'),
		't', 'the measured cost delay is positive and included in total time');
	my $delay = sprintf('%.3f', counters('vstat_delay', 'total_vacuum_delay_time'));
	like($verbose, qr/elapsed: \d+\.\d{3} ms, cost-based delay: \Q$delay\E ms/,
		'VERBOSE reports elapsed time and the same measured cost delay');
};

subtest 'index pages are not counted again by later vacuums' => sub {
	$node->safe_psql('postgres', q{
CREATE TABLE vstat_idx (id int PRIMARY KEY) WITH (autovacuum_enabled = off);
INSERT INTO vstat_idx SELECT generate_series(1, 100000);
DELETE FROM vstat_idx WHERE id <= 90000;
});
	vacuum_table('vstat_idx');
	my $pages = index_counters('vstat_idx_pkey', 'pages_deleted');
	cmp_ok($pages, '>', 0, 'the first vacuum deleted index pages');
	$node->safe_psql('postgres', 'DELETE FROM vstat_idx WHERE id > 99000');
	vacuum_table('vstat_idx');
	is(index_counters('vstat_idx_pkey', 'tuples_deleted'), '91000',
		'the next vacuum removed exactly 1000 more index entries');
	is(index_counters('vstat_idx_pkey', "pages_deleted >= $pages AND pages_deleted - $pages < $pages / 2"),
		't', 'deleted-page totals accumulate without recounting the first run');
};

subtest 'reset preserves ordinary statistics and other relations' => sub {
	$node->safe_psql('postgres', q{
CREATE TABLE vstat_reset (id int PRIMARY KEY) WITH (autovacuum_enabled = off);
CREATE TABLE vstat_keep (id int) WITH (autovacuum_enabled = off);
INSERT INTO vstat_reset SELECT generate_series(1, 1000);
INSERT INTO vstat_keep SELECT generate_series(1, 1000);
DELETE FROM vstat_keep;
});
	vacuum_table('vstat_keep');
	vacuum_table('vstat_reset', 'FREEZE');
	$node->safe_psql('postgres', 'UPDATE vstat_reset SET id = id + 1000');
	wait_for_updates('vstat_reset', 1000);
	vacuum_table('vstat_reset', 'FREEZE');
	is(counters('vstat_reset',
		'tuples_deleted, frozen_page_marks_cleared > 0, visible_page_marks_cleared > 0'),
		'1000|t|t', 'populate removal and VM revision counters before reset');
	my $keep_before = counters('vstat_keep');
	my $index_before = index_counters('vstat_reset_pkey');
	my $db_before = database_counters();
	my $ordinary_query = "SELECT vacuum_count, n_tup_upd FROM pg_stat_all_tables_internal WHERE relname = 'vstat_reset'";
	my $ordinary_before = $node->safe_psql('postgres', $ordinary_query);
	my $reset_before = counters('vstat_reset');

	my ($result, $stdout, $stderr) = $node->psql('postgres',
		'SELECT vacuum_stats_reset(0::oid)');
	is($result, 3, 'an invalid relation OID is rejected');
	like($stderr, qr/invalid relation OID: 0/, 'invalid OID has a specific error');
	run_and_wait('SELECT vacuum_stats_reset(NULL::oid)');
	is(counters('vstat_reset'), $reset_before,
		'invalid and NULL OIDs do not reset a relation');
	is(database_counters(), $db_before,
		'invalid and NULL OIDs do not reset database totals');

	run_and_wait("SELECT vacuum_stats_reset('vstat_reset'::regclass::oid)");
	is(counters('vstat_reset', $all_zero), 't', 'relation reset clears every vacuum counter including VM clearing counters');
	is(counters('vstat_keep'), $keep_before, 'relation reset preserves another table');
	is(index_counters('vstat_reset_pkey'), $index_before, 'relation reset preserves the index counters');
	is(database_counters(), $db_before, 'relation reset preserves database totals');
	is($node->safe_psql('postgres', $ordinary_query), $ordinary_before,
		'relation reset preserves ordinary statistics');

	# Refill the reset relation, including both VM revision counters,
	# before checking the database-wide reset.
	$node->safe_psql('postgres', 'UPDATE vstat_reset SET id = id + 1000');
	wait_for_updates('vstat_reset', 2000);
	vacuum_table('vstat_reset', 'FREEZE');
	is(counters('vstat_reset',
		'tuples_deleted, frozen_page_marks_cleared > 0, visible_page_marks_cleared > 0'),
		'1000|t|t', 'the counters are nonzero again before database reset');
	$ordinary_before = $node->safe_psql('postgres', $ordinary_query);
	run_and_wait('SELECT vacuum_stats_reset()');
	is($node->safe_psql('postgres', "SELECT bool_and($all_zero) FROM pg_stat_vacuum_tables"),
		't', 'database reset clears all table counters');
	is($node->safe_psql('postgres', 'SELECT bool_and(total_file_segs = 0) FROM pg_stat_vacuum_tables'),
		't', 'database reset clears AO segment snapshots');
	is($node->safe_psql('postgres', "SELECT bool_and($all_zero) FROM pg_stat_vacuum_indexes"),
		't', 'database reset clears all index counters');
	is(database_counters($all_zero), 't', 'database reset clears database totals');
	is($node->safe_psql('postgres', $ordinary_query), $ordinary_before,
		'database reset preserves ordinary statistics');


};

subtest 'database totals do not count index work twice' => sub {
	$node->safe_psql('postgres', q{
CREATE TABLE vstat_db (id int PRIMARY KEY, val int) WITH (autovacuum_enabled = off);
CREATE INDEX ON vstat_db (val);
INSERT INTO vstat_db SELECT i, i FROM generate_series(1, 10000) g(i);
DELETE FROM vstat_db WHERE id % 2 = 0;
});
	run_and_wait('SELECT vacuum_stats_reset()');
	vacuum_table('vstat_db');
	is($node->safe_psql('postgres',
		"SELECT sum(tuples_deleted) FROM pg_stat_vacuum_indexes WHERE relname = 'vstat_db'"),
		'10000', 'both indexes report 5000 removed entries');
	is(database_counters('tuples_deleted'), '5000', 'database totals count only heap tuples');
	is(database_counters(), counters('vstat_db'),
		'all database vacuum counters match the only table vacuumed since reset');
};

subtest 'statistics snapshots release their vacuum counters' => sub {
	# Keep the last snapshot alive until the context count is read.
	# Outside this transaction its context would already have been freed.
	is($node->safe_psql('postgres', q{
BEGIN;
DO $$
BEGIN
    FOR i IN 1..50 LOOP
        PERFORM pg_stat_clear_snapshot();
        PERFORM count(*) FROM pg_stat_vacuum_tables WHERE tuples_deleted > 0;
    END LOOP;
END
$$;
SELECT count(*) FROM pg_backend_memory_contexts
WHERE name = 'Databases hash';
COMMIT;
}), '1', 'only the current snapshot owns statistics entries with vacuum counters');
};

subtest 'reset targets shared catalogs independently' => sub {
	# Shared catalogs have a separate collector entry, not the current DB's.
	# Create actual dead index entries instead of timing an empty cleanup.
	$node->safe_psql('postgres', 'CREATE DATABASE vstat_shared_reset');
	$node->safe_psql('postgres', 'DROP DATABASE vstat_shared_reset');
	vacuum_table('pg_database', '(FREEZE, INDEX_CLEANUP ON)');
	is(counters('pg_database', 'total_vacuum_time > 0'), 't',
		'populate vacuum counters for a shared catalog');
	my $shared_before = counters('pg_database');
	run_and_wait('SELECT vacuum_stats_reset()');
	is(counters('pg_database'), $shared_before,
		'database reset leaves shared catalog counters alone');

	vacuum_table('vstat_reset');
	my $reset_before = counters('vstat_reset');
	my $db_before = database_counters();
	run_and_wait("SELECT vacuum_stats_reset('pg_database'::regclass::oid)");
	is(counters('pg_database', $all_zero), 't',
		'relation reset reaches the shared catalog entry');
	is(counters('vstat_reset'), $reset_before,
		'shared relation reset leaves current-database relations alone');
	is(database_counters(), $db_before,
		'shared relation reset leaves current-database totals alone');

	# Indexes are separate reset targets, including shared catalog indexes.
	is(index_counters('pg_database_oid_index', 'tuples_deleted > 0'), 't',
		'resetting the shared table preserves its index counters');
	run_and_wait(q{
SELECT vacuum_stats_reset(indexrelid) FROM pg_index
WHERE indrelid = 'pg_database'::regclass
});
	is(index_counters('pg_database_oid_index', $all_zero), 't',
		'relation reset also reaches shared index counters');
};

subtest 'disabling tracking omits vacuum counters and preserves ordinary statistics' => sub {
	$node->safe_psql('postgres', q{
CREATE TABLE vstat_startup (id int PRIMARY KEY) WITH (autovacuum_enabled = off);
INSERT INTO vstat_startup SELECT generate_series(1, 1000);
});
	vacuum_table('vstat_startup', 'FREEZE');
	$node->safe_psql('postgres', 'UPDATE vstat_startup SET id = id + 1000');
	wait_for_updates('vstat_startup', 1000);
	vacuum_table('vstat_startup', 'FREEZE');
	is(counters('vstat_startup',
		'tuples_deleted, frozen_page_marks_cleared > 0, visible_page_marks_cleared > 0'),
		'1000|t|t', 'populate vacuum and VM revision counters before disabling');
	my $ordinary_query = "SELECT vacuum_count, n_tup_upd FROM pg_stat_all_tables_internal WHERE relname = 'vstat_startup'";
	my $ordinary_before = $node->safe_psql('postgres', $ordinary_query);
	my $vm_before = counters('vstat_startup', $vm_columns);
	my $timing_columns = 'total_vacuum_time, total_autovacuum_time, total_vacuum_delay_time, total_autovacuum_delay_time';
	my $timing_before = counters('vstat_startup', $timing_columns);
	my $db_vm_before = database_counters($vm_columns);
	my $on_bytes = snapshot_bytes();
	$node->safe_psql('postgres', 'ALTER SYSTEM SET track_vacuum_statistics = off');
	$node->restart;
	is($node->safe_psql('postgres', 'SHOW track_vacuum_statistics'), 'off',
		'restart disables tracking');
	is(counters('vstat_startup', $vm_columns), $vm_before,
		'disabling vacuum tracking preserves relation VM counters');
	is(counters('vstat_startup', $timing_columns), $timing_before,
		'disabling extended tracking preserves core vacuum timing');
	is(database_counters($vm_columns), $db_vm_before,
		'disabling vacuum tracking preserves database VM totals');
	is(counters('vstat_startup', $vacuum_zero), 't', 'disabled table counters read as zero');
	is(index_counters('vstat_startup_pkey', $vacuum_zero), 't', 'disabled index counters read as zero');
	is(database_counters($vacuum_zero), 't', 'disabled database counters read as zero');
	is($node->safe_psql('postgres', 'SELECT bool_and(total_file_segs = 0) FROM pg_stat_vacuum_tables'),
		't', 'disabled AO segment snapshots read as zero');
	is($node->safe_psql('postgres', $ordinary_query), $ordinary_before,
		'reading statistics with tracking disabled preserves ordinary counters');
	cmp_ok(snapshot_bytes(), '<', $on_bytes,
		'disabled snapshots omit vacuum storage but retain ordinary VM counters');
	run_and_wait("SELECT vacuum_stats_reset('vstat_startup'::regclass::oid)");
	is(counters('vstat_startup', $vm_columns), '0|0',
		'relation reset clears VM counters with vacuum tracking off');
	is(database_counters($vm_columns), $db_vm_before,
		'relation reset preserves database VM totals with vacuum tracking off');
	run_and_wait('SELECT vacuum_stats_reset()');
	is(database_counters($vm_columns), '0|0',
		'database reset clears VM totals with vacuum tracking off');
	is($node->safe_psql('postgres', $ordinary_query), $ordinary_before,
		'vacuum reset while disabled leaves ordinary counters intact');
	$node->safe_psql('postgres', 'ALTER SYSTEM SET track_vacuum_statistics = on');
	$node->restart;
	is(counters('vstat_startup', $vacuum_zero), 't',
		're-enabling does not restore discarded table vacuum counters');
	is(database_counters($vacuum_zero), 't',
		're-enabling does not restore discarded database vacuum counters');
	is($node->safe_psql('postgres', $ordinary_query), $ordinary_before,
		'ordinary statistics survive both changes in record size');
	vacuum_table('vstat_startup', 'FREEZE');
	is(counters('vstat_startup', 'total_vacuum_time > 0'), 't',
		'vacuum reporting resumes after tracking is enabled again');
};

subtest 'clean restart preserves statistics; crash recovery resets them' => sub {
	$node->safe_psql('postgres', q{
CREATE TABLE vstat_restart (id int PRIMARY KEY) WITH (autovacuum_enabled = off);
INSERT INTO vstat_restart SELECT generate_series(1, 1000);
DELETE FROM vstat_restart WHERE id % 2 = 0;
});
	vacuum_table('vstat_restart', 'FREEZE');
	is(counters('vstat_restart', 'tuples_deleted'), '500', 'table counters are populated before restart');
	is(index_counters('vstat_restart_pkey', 'tuples_deleted'), '500', 'index counters are populated before restart');
	my $table_before = counters('vstat_restart');
	my $index_before = index_counters('vstat_restart_pkey');
	my $db_before = database_counters();
	$node->restart;
	is(counters('vstat_restart'), $table_before, 'table counters survive a clean restart');
	is(index_counters('vstat_restart_pkey'), $index_before, 'index counters survive a clean restart');
	is(database_counters(), $db_before, 'database counters survive a clean restart');
	$node->stop('immediate');
	$node->start;
	is(counters('vstat_restart', $all_zero), 't', 'crash recovery resets table counters');
	is(index_counters('vstat_restart_pkey', $all_zero), 't', 'crash recovery resets index counters');
	is(database_counters($all_zero), 't', 'crash recovery resets database counters');
};

$node->stop;
done_testing();
