# Copyright (c) 2026, PostgreSQL Global Development Group

# AO pre-cleanup and post-cleanup can both scan an index. Only the final
# state contributes dead_pages; the AO table itself has no heap dead pages.
use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('vacstat_ao_dead_pages');
$node->init;
$node->append_conf('postgresql.conf', q{
autovacuum = off
gp_appendonly_compaction_threshold = 1
});
$node->start;
my $nrows = 20000 * ($node->safe_psql('postgres', 'SHOW block_size') / 8192);
for my $orientation ('row', 'column')
{
    my $table = "ao_pages_$orientation";
    my $index = "${table}_idx";
    $node->safe_psql('postgres', qq{
CREATE TABLE $table (id int) WITH (appendonly = true, orientation = $orientation);
CREATE INDEX $index ON $table (id);
INSERT INTO $table SELECT generate_series(1, $nrows);
DELETE FROM $table WHERE id <= $nrows / 2;
});
    # This snapshot defers recycling the segment compacted by the first run.
    my $old = $node->background_psql('postgres');
    $old->query_safe("BEGIN ISOLATION LEVEL REPEATABLE READ; SELECT count(*) FROM $table");
    $node->safe_psql('postgres', "VACUUM $table");
    is($node->safe_psql('postgres', qq{
SELECT dead_pages = 0 AND tuples_deleted = $nrows / 2
  FROM pg_stat_vacuum_tables WHERE relname = '$table'}), 't',
       "$orientation AO compaction leaves the heap-only dead-pages counter zero");
    $old->query_safe('COMMIT');
    $old->quit;

    # This later snapshot permits segment recycling, but holds back reuse
    # of index pages newly deleted during the second run's pre-cleanup.
    my $hold = $node->background_psql('postgres');
    $hold->query_safe('BEGIN ISOLATION LEVEL REPEATABLE READ; SELECT txid_current_snapshot()');
    $node->safe_psql('postgres', "VACUUM $table");
    my $deleted = $node->safe_psql('postgres', qq{
SELECT pages_deleted FROM pg_stat_vacuum_indexes WHERE indexrelname = '$index'});
    cmp_ok($deleted, '>', 0, "$orientation AO recycling deletes index pages");
    my $count_sql = qq{
SELECT dead_pages FROM pg_stat_vacuum_indexes WHERE indexrelname = '$index'};
    is($node->safe_psql('postgres', $count_sql), $deleted,
       "$orientation AO counts non-reusable index pages once across both cleanup phases");
    $node->safe_psql('postgres', "VACUUM $table");
    is($node->safe_psql('postgres', $count_sql), 2 * $deleted,
       "$orientation AO accumulates index pages still waiting for reuse");
    $hold->query_safe('COMMIT');
    $hold->quit;
    $node->safe_psql('postgres', 'SELECT txid_current()');
    $node->safe_psql('postgres', "VACUUM $table");
    # Cloudberry's BTPageIsRecyclable deliberately permits recycling only
    # for heap relations. AO index tombstones remain non-reusable even after
    # the snapshot ends, and each subsequent cleanup observes them once.
    is($node->safe_psql('postgres', $count_sql), 3 * $deleted,
       "$orientation AO retains non-reusable index pages after the snapshot ends");
}

$node->stop;
done_testing();
