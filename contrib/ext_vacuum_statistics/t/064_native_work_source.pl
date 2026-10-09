# Copyright (c) 2026, PostgreSQL Global Development Group

# Extension views reuse native work history; collection and resets of
# extension resources must never change native counters.
use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('vacstat_native_source');
$node->init;
$node->append_conf('postgresql.conf', q{
shared_preload_libraries = 'ext_vacuum_statistics'
autovacuum = off
});
$node->start;
$node->safe_psql('postgres', q{
CREATE EXTENSION ext_vacuum_statistics;
CREATE TABLE native_heap (id int PRIMARY KEY);
INSERT INTO native_heap SELECT generate_series(1, 1000);
DELETE FROM native_heap WHERE id <= 100;
SET vacuum_statistics.enabled = off;
VACUUM (FREEZE) native_heap;
SELECT pg_stat_force_next_flush();
});

is($node->safe_psql('postgres', q{
SELECT count(*) FROM ext_vacuum_statistics.pg_stats_get_vacuum_tables(
  (SELECT oid FROM pg_database WHERE datname = current_database()),
  'native_heap'::regclass)
}), '0', 'disabled first vacuum creates no extension resource entry');
is($node->safe_psql('postgres', q{
SELECT tuples_deleted = 100 AND pages_scanned > 0 AND tuples_frozen = 900
       AND total_blks_read = 0 AND total_blks_hit = 0 AND wal_records = 0
  FROM ext_vacuum_statistics.pg_stats_vacuum_tables
 WHERE relname = 'native_heap'
}), 't', 'view exposes native work even without a resource entry');
is($node->safe_psql('postgres', q{
SELECT tuples_deleted = 100 AND total_blks_read = 0 AND total_blks_hit = 0
  FROM ext_vacuum_statistics.pg_stats_vacuum_indexes
 WHERE indexrelname = 'native_heap_pkey'
}), 't', 'index view exposes native work without a resource entry');

for my $orientation ('row', 'column')
{
    $node->safe_psql('postgres', qq{
CREATE TABLE native_$orientation (id int)
  WITH (appendonly = true, orientation = $orientation);
INSERT INTO native_$orientation SELECT generate_series(1, 1000);
DELETE FROM native_$orientation WHERE id <= 100;
SET gp_appendonly_compaction = off;
VACUUM native_$orientation;
VACUUM native_$orientation;
SELECT pg_stat_force_next_flush();
});
    is($node->safe_psql('postgres', qq{
SELECT recently_dead_tuples FROM ext_vacuum_statistics.pg_stats_vacuum_tables
 WHERE relname = 'native_$orientation'
}), '200', "$orientation AO shows the native cumulative hidden-row count");
}

my @table_fields = qw(tuples_deleted pages_scanned pages_removed tuples_frozen
    recently_dead_tuples missed_dead_pages missed_dead_tuples pages_frozen
    pages_all_visible total_file_segs compacted_segments tuples_moved
    dead_pages freeze_age_vacuum_count);
my @index_fields = qw(tuples_deleted pages_deleted dead_pages);
for my $consistency ('none', 'cache', 'snapshot')
{
    my $tables_match = join(' AND ', map { "e.$_ = n.$_" } @table_fields);
    my $indexes_match = join(' AND ', map { "e.$_ = n.$_" } @index_fields);
    is($node->safe_psql('postgres', qq{
SET stats_fetch_consistency = '$consistency';
SELECT count(*) = 3 AND bool_and($tables_match)
  FROM ext_vacuum_statistics.pg_stats_vacuum_tables e
  CROSS JOIN LATERAL pg_stat_get_vacuum_stats(e.relid) n
 WHERE e.relname IN ('native_heap', 'native_row', 'native_column');
}), 't', "$consistency: all heap and AO work fields match native counters");
    is($node->safe_psql('postgres', qq{
SET stats_fetch_consistency = '$consistency';
SELECT $indexes_match
  FROM ext_vacuum_statistics.pg_stats_vacuum_indexes e
  CROSS JOIN LATERAL pg_stat_get_vacuum_stats(e.indexrelid) n
 WHERE e.indexrelname = 'native_heap_pkey';
}), 't', "$consistency: all index work fields match native counters");
}

my $work = q{
SELECT row_to_json(n) FROM pg_stat_get_vacuum_stats('native_heap'::regclass) n
};
my $resources = q{
SELECT total_blks_read + total_blks_hit
  FROM ext_vacuum_statistics.pg_stats_vacuum_tables WHERE relname = 'native_heap'
};
my $work_before = $node->safe_psql('postgres', $work);
$node->safe_psql('postgres', q{
SET track_counts = off;
VACUUM (DISABLE_PAGE_SKIPPING) native_heap;
});
is($node->safe_psql('postgres', $work), $work_before,
   'track_counts disables native work collection independently');
cmp_ok($node->safe_psql('postgres', $resources), '>', 0,
   'resource collection continues with track_counts off');

my $dboid = $node->safe_psql('postgres',
    q[SELECT oid FROM pg_database WHERE datname = current_database()]);
for my $call ("extvac_reset_entry($dboid, 'native_heap'::regclass)",
              "extvac_reset_db_entry($dboid)", 'vacuum_statistics_reset()')
{
    $node->safe_psql('postgres', q{
VACUUM (DISABLE_PAGE_SKIPPING) native_heap;
SELECT pg_stat_force_next_flush();
});
    my $native = $node->safe_psql('postgres', $work);
    my $db_native_sql = qq{
SELECT row_to_json(n) FROM pg_stat_get_vacuum_database_stats($dboid) n
};
    my $db_native = $node->safe_psql('postgres', $db_native_sql);
    my $other_sql = q{
SELECT n_tup_ins, n_tup_del, vacuum_count, last_vacuum
  FROM pg_stat_all_tables WHERE relname = 'native_heap'
};
    my $other = $node->safe_psql('postgres', $other_sql);
    cmp_ok($node->safe_psql('postgres', $resources), '>', 0,
           "$call: resources collected before reset");
    $node->safe_psql('postgres', "SELECT ext_vacuum_statistics.$call");
    is($node->safe_psql('postgres', $resources), '0', "$call clears resources");
    is($node->safe_psql('postgres', $work), $native, "$call preserves native relation work");
    is($node->safe_psql('postgres', $db_native_sql), $db_native,
       "$call preserves native database work");
    is($node->safe_psql('postgres', $other_sql), $other,
       "$call preserves other native relation statistics");
}

$node->safe_psql('postgres', 'VACUUM (DISABLE_PAGE_SKIPPING) native_heap');
my $resources_before = $node->safe_psql('postgres', $resources);
$node->safe_psql('postgres',
    q[SELECT pg_stat_reset_vacuum_stats('native_heap'::regclass)]);
is($node->safe_psql('postgres', q{
SELECT tuples_deleted = 0 AND pages_scanned = 0 AND tuples_frozen = 0
  FROM ext_vacuum_statistics.pg_stats_vacuum_tables WHERE relname = 'native_heap'
}), 't', 'native reset is visible in the extension work columns');
is($node->safe_psql('postgres', $resources), $resources_before,
   'native relation reset preserves extension resources');

$node->stop;
done_testing();
