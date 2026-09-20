# Copyright (c) 2026, PostgreSQL Global Development Group

# VM flag clearings are ordinary relation/database statistics.  This test
# reads them through the vacuum_stats extension without changing core views.
# Adapt the v44 VM stability scenario to the collector used by Cloudberry:
# https://www.postgresql.org/message-id/attachment/204710/v44-0009-Track-table-VM-stability.patch
use strict;
use warnings FATAL => 'all';
use PostgresNode;
use TestLib;
use Test::More;

my $node = get_new_node('vm_stats');
$node->init;
$node->append_conf('postgresql.conf', q{
autovacuum = off
track_counts = on
});
$node->start;
$node->safe_psql('postgres', 'CREATE EXTENSION vacuum_stats');

sub wait_for_stats
{
	my ($sql, $description) = @_;
	$node->poll_query_until('postgres', $sql)
	  or BAIL_OUT("timed out waiting for $description");
}

my $columns = 'frozen_page_marks_cleared, visible_page_marks_cleared';
my $table_query = "SELECT $columns FROM pg_stat_vacuum_tables WHERE relname = 'vm_heap'";
my $visible_query = "SELECT $columns FROM pg_stat_vacuum_tables WHERE relname = 'vm_visible'";
my $db_query = "SELECT $columns FROM pg_stat_vacuum_database WHERE datname = current_database()";

sub populated_pages
{
	my ($table) = @_;
	return $node->safe_psql('postgres',
		"SELECT count(DISTINCT split_part(ctid::text, ',', 1)) FROM $table");
}

$node->safe_psql('postgres', q{
CREATE TABLE vm_heap (id int PRIMARY KEY) WITH (fillfactor = 70);
INSERT INTO vm_heap SELECT generate_series(1, 1000);
CREATE TABLE vm_visible (id int);
INSERT INTO vm_visible SELECT generate_series(1, 1000);
});
wait_for_stats(q{
SELECT n_tup_ins = 1000 FROM pg_stat_all_tables_internal WHERE relname = 'vm_heap'
}, 'insert report');
$node->safe_psql('postgres', 'VACUUM FREEZE vm_heap');
wait_for_stats(q{
SELECT vacuum_count = 1 FROM pg_stat_all_tables_internal WHERE relname = 'vm_heap'
}, 'vacuum report');
# A normal vacuum marks the new tuples visible, without freezing their XIDs.
$node->safe_psql('postgres', q{
SET vacuum_freeze_min_age = 1000000000;
SET vacuum_freeze_table_age = 1000000000;
VACUUM vm_visible;
});
wait_for_stats(q{
SELECT vacuum_count = 1 AND n_tup_ins = 1000
FROM pg_stat_all_tables_internal WHERE relname = 'vm_visible'
}, 'non-freezing vacuum and insert reports');

# Drain reports from table/index creation before taking the database baseline.
$node->restart;
is($node->safe_psql('postgres', $table_query), '0|0',
	'setting VM flags does not count as clearing them');
my ($db_frozen, $db_visible) = split /\|/,
	$node->safe_psql('postgres', $db_query);
my $pages = populated_pages('vm_heap');
cmp_ok($pages, '>', 0, 'test table has populated heap pages');

# Count each physical transition once, even though UPDATE touches many tuples
# on each page. DML and VM counters travel in the same collector message, so
# wait for the DML report before checking exact values, including zeroes.
$node->safe_psql('postgres', 'UPDATE vm_heap SET id = id + 1000');
wait_for_stats(q{
SELECT n_tup_upd = 1000 FROM pg_stat_all_tables_internal WHERE relname = 'vm_heap'
}, 'first update report');
is($node->safe_psql('postgres', $table_query), "$pages|$pages",
	'UPDATE clears one all-visible and all-frozen mark per populated page');
is($node->safe_psql('postgres', qq{
SELECT frozen_page_marks_cleared - $db_frozen,
       visible_page_marks_cleared - $db_visible
FROM pg_stat_vacuum_database WHERE datname = current_database()
}), "$pages|$pages", 'database totals include exactly the table transitions');

$node->safe_psql('postgres', 'UPDATE vm_heap SET id = id + 1000');
wait_for_stats(q{
SELECT n_tup_upd = 2000 FROM pg_stat_all_tables_internal WHERE relname = 'vm_heap'
}, 'second update report');
is($node->safe_psql('postgres', $table_query), "$pages|$pages",
	'updates on pages whose marks are already clear add nothing');
is($node->safe_psql('postgres', qq{
SELECT pg_stat_get_frozen_page_marks_cleared('vm_heap'::regclass),
       pg_stat_get_visible_page_marks_cleared('vm_heap'::regclass)
}), "$pages|$pages", 'extension getters and table view expose the same counters');

# Distinguish the two counters and verify that rolling back DML does not undo
# the physical clearing of VM bits. Utility-mode SELECT FOR UPDATE takes a
# table lock, so use unfrozen tuples to exercise independent bit accounting.
my $visible_pages = populated_pages('vm_visible');
cmp_ok($visible_pages, '>', 0, 'unfrozen table has populated heap pages');
$node->safe_psql('postgres', 'BEGIN; DELETE FROM vm_visible; ROLLBACK;');
wait_for_stats(q{
SELECT n_tup_del = 1000 FROM pg_stat_all_tables_internal WHERE relname = 'vm_visible'
}, 'aborted delete report');
is($node->safe_psql('postgres', 'SELECT count(*) FROM vm_visible'), '1000',
	'aborted DELETE preserves the rows');
is($node->safe_psql('postgres', $visible_query), "0|$visible_pages",
	'aborted DELETE counts all-visible clearings without inventing all-frozen clearings');
is($node->safe_psql('postgres', qq{
SELECT frozen_page_marks_cleared - $db_frozen,
       visible_page_marks_cleared - $db_visible
FROM pg_stat_vacuum_database WHERE datname = current_database()
}), $pages . '|' . ($pages + $visible_pages),
	'database totals distinguish the bits and include aborted DML');

# Re-establish flags, then modify the table while a reader holds both an MVCC
# snapshot and a statistics snapshot. The statistics snapshot postpones
# observing the clearings; the VM bits themselves are cleared by DELETE,
# before the reader commits.
$node->safe_psql('postgres', 'VACUUM FREEZE vm_heap');
wait_for_stats(q{
SELECT vacuum_count = 2 FROM pg_stat_all_tables_internal WHERE relname = 'vm_heap'
}, 'second vacuum report');
is($node->safe_psql('postgres', $table_query), "$pages|$pages",
	'restoring VM flags does not increase clearing counters');
my $refrozen_pages = populated_pages('vm_heap');
my $cleared = $pages + $refrozen_pages;

my ($in, $out) = ('', '');
my $timer = IPC::Run::timeout($TestLib::timeout_default);
my $reader = $node->background_psql('postgres', \$in, \$out, $timer);
my $reader_query = sub {
	my ($sql) = @_;
	$out = '';
	$in = "$sql;\n\\echo vm_query_done\n";
	pump_until($reader, $timer, \$out, qr/^vm_query_done\r?$/m)
	  or BAIL_OUT('reader did not complete its query');
	$out =~ s/\r//g;
	$out =~ s/^vm_query_done\n?//m;
	$out =~ s/^\n+|\n+$//g;
	return $out;
};
is($reader_query->("BEGIN ISOLATION LEVEL REPEATABLE READ; $table_query"),
	"$pages|$pages", 'reader caches the pre-delete statistics');
is($reader_query->('SELECT count(*) FROM vm_heap'), '1000',
	'reader holds an MVCC snapshot of the rows');

$node->safe_psql('postgres', 'DELETE FROM vm_heap');
wait_for_stats(q{
SELECT n_tup_del = 1000 FROM pg_stat_all_tables_internal WHERE relname = 'vm_heap'
}, 'concurrent delete report');
is($node->safe_psql('postgres', $table_query), "$cleared|$cleared",
	'DELETE counts fresh VM clearings while the reader transaction is open');
is($reader_query->($table_query), "$pages|$pages",
	'cached statistics retain their earlier values');
is($reader_query->("SELECT pg_stat_clear_snapshot(); $table_query"),
	"$cleared|$cleared", 'clearing the statistics snapshot exposes the new counters');
is($reader_query->('SELECT count(*) FROM vm_heap'), '1000',
	'refreshing statistics leaves the MVCC snapshot unchanged');
$in = "COMMIT;\n\\q\n";
$reader->finish;
is($node->safe_psql('postgres', $table_query), "$cleared|$cleared",
	'committing the reader adds no clearings');
is($node->safe_psql('postgres', qq{
SELECT frozen_page_marks_cleared - $db_frozen,
       visible_page_marks_cleared - $db_visible
FROM pg_stat_vacuum_database WHERE datname = current_database()
}), $cleared . '|' . ($cleared + $visible_pages),
	'database totals accumulate both rounds of heap clearings');

my $db_totals = $node->safe_psql('postgres', $db_query);
$node->restart;
is($node->safe_psql('postgres', $table_query), "$cleared|$cleared",
	'table VM counters survive a clean restart');
is($node->safe_psql('postgres', $visible_query), "0|$visible_pages",
	'counts from aborted DML survive a clean restart');
is($node->safe_psql('postgres', $db_query), $db_totals,
	'database VM counters survive a clean restart');

$node->safe_psql('postgres',
	q{SELECT pg_stat_reset_single_table_counters('vm_heap'::regclass)});
wait_for_stats(q{
SELECT frozen_page_marks_cleared = 0 AND visible_page_marks_cleared = 0
FROM pg_stat_vacuum_tables WHERE relname = 'vm_heap'
}, 'relation reset');
is($node->safe_psql('postgres', $table_query), '0|0',
	'relation reset clears both VM counters');
is($node->safe_psql('postgres', $visible_query), "0|$visible_pages",
	'relation reset preserves another table\'s counters');
is($node->safe_psql('postgres', $db_query), $db_totals,
	'relation reset preserves database totals');

$node->safe_psql('postgres', 'SELECT pg_stat_reset()');
wait_for_stats(q{
SELECT frozen_page_marks_cleared = 0 AND visible_page_marks_cleared = 0
FROM pg_stat_vacuum_database WHERE datname = current_database()
}, 'database reset');
is($node->safe_psql('postgres', $db_query), '0|0',
	'database reset clears both VM counters');
is($node->safe_psql('postgres', $visible_query), '0|0',
	'database reset also clears relation VM counters');

# The SQL interface belongs to the extension, not the system catalog.
is($node->safe_psql('postgres', q{
SELECT count(*) FROM pg_attribute
WHERE attrelid IN ('pg_stat_all_tables'::regclass, 'pg_stat_database'::regclass)
  AND attname IN ('frozen_page_marks_cleared', 'visible_page_marks_cleared')
  AND NOT attisdropped
}), '0', 'core statistics views retain their original columns');

$node->stop;
done_testing();
