# Copyright (c) 2026, PostgreSQL Global Development Group

# Resource instrumentation has a core consumer even without an extension.
use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

# psql -f - prefixes diagnostics with psql:<stdin>:<line>:.  A phase
# must not borrow counters from the next diagnostic or resource report.
sub resource_report
{
    my ($stderr, $table, $phase) = @_;
    my ($report) = $stderr =~
      /vacuum resource usage for "public\.\Q$table\E" \(\Q$phase\E\):\n(.*?)(?=\n(?:psql:[^\n]*?:\s*)?(?:INFO|CONTEXT|WARNING|ERROR):|\n[^\n]*vacuum resource usage for|\z)/s;
    return $report;
}

my $diagnostics = <<'REPORTS';
psql:<stdin>:1: INFO:  vacuum resource usage for "public.first" (phase):
buffer usage: -1 hits, 0 misses, 0 dirtied, 0 written
psql:<stdin>:2: INFO:  vacuum resource usage for "public.second" (phase):
buffer usage: 1 hits, 0 misses, 0 dirtied, 0 written
WAL usage: 1 records, 0 full page images, 10 bytes
I/O timings: read: 0.000 ms, write: 0.000 ms
REPORTS
my $first_report = resource_report($diagnostics, 'first', 'phase');
like($first_report, qr/buffer usage: -1 hits/,
     'phase capture retains its own invalid counters');
unlike($first_report, qr/WAL usage:|I\/O timings:|buffer usage: 1 hits/,
       'phase capture cannot borrow valid counters from the next report');

my $node = PostgreSQL::Test::Cluster->new('vacuum_resource_verbose');
$node->init;
$node->append_conf('postgresql.conf', q{
autovacuum = off
track_io_timing = on
gp_appendonly_compaction_threshold = 1
});
$node->start;
is($node->safe_psql('postgres', 'SHOW shared_preload_libraries'), '',
   'VERBOSE resource reporting requires no preloaded extension');

for my $storage ('heap', 'row', 'column')
{
    my $table = "verbose_$storage";
    my $options = $storage eq 'heap' ? '' :
      "WITH (appendonly = true, orientation = $storage)";
    $node->safe_psql('postgres', qq{
CREATE TABLE $table (id int, padding text) $options;
CREATE INDEX ${table}_idx ON $table (id);
CREATE INDEX ${table}_expression_idx ON $table ((id + 1));
INSERT INTO $table SELECT g, repeat('x', 100) FROM generate_series(1, 4000) g;
DELETE FROM $table WHERE id <= 1000;
});
    my ($stdout, $stderr);
    is($node->psql('postgres', "VACUUM $table",
                  stdout => \$stdout, stderr => \$stderr), 0,
       "$storage plain vacuum succeeds");
    unlike($stderr, qr/vacuum resource usage for/,
           "$storage plain vacuum does not emit resource reports");
    $node->safe_psql('postgres', "DELETE FROM $table WHERE id <= 2000");
    is($node->psql('postgres', "VACUUM (VERBOSE, INDEX_CLEANUP ON) $table",
                  stdout => \$stdout, stderr => \$stderr), 0,
       "$storage verbose vacuum succeeds");
    my @phases = $storage eq 'heap' ? ('heap (excluding indexes)') :
      ('AO pre-cleanup (excluding indexes)', 'AO compaction (excluding indexes)',
       'AO post-cleanup (excluding indexes)');
    for my $phase (@phases)
    {
        my $report = resource_report($stderr, $table, $phase);
        ok(defined $report, "$storage reports $phase without an extension");
        $report //= '';
        like($report, qr/buffer usage: \d+ hits, \d+ misses, \d+ dirtied, \d+ written/,
             "$phase reports nonnegative buffer counters");
        like($report, qr/WAL usage: \d+ records, \d+ full page images, \d+ bytes/,
             "$phase reports WAL counters");
        like($report, qr/I\/O timings: read: \d+\.\d+ ms, write: \d+\.\d+ ms/,
             "$phase reports I/O times in milliseconds");
        like($report, qr/WAL usage: [1-9]\d* records/,
             'compaction produces WAL') if $phase eq 'AO compaction (excluding indexes)';
    }
    if ($storage ne 'heap')
    {
        my $inserts = resource_report($stderr, $table,
          'AO compaction index inserts (all indexes)');
        ok(defined $inserts, "$storage reports compaction index insertions separately");
        $inserts //= '';
        like($inserts, qr/buffer usage: [1-9]\d* hits,/,
             "$storage compaction index insertions access buffers");
        my ($records) = $inserts =~ /WAL usage: (\d+) records,/;
        cmp_ok($records // 0, '>=', 4000,
               "$storage accounts for both indexes on 2000 relocated tuples");
    }
    my $index_phase = $storage eq 'heap' ? 'index bulk delete' : 'AO index vacuum';
    like($stderr, qr/vacuum resource usage for "public\.\Q${table}_idx\E" \(\Q$index_phase\E\):/,
         "$storage reports index resources separately");
}

# Parallel-safe btrees/GIN and leader-only BRIN bulk deletion must be excluded
# from the heap report after worker usage has been merged into the leader.
$node->safe_psql('postgres', q{
CREATE TABLE verbose_parallel (id int, a int[]) WITH (autovacuum_enabled = off);
INSERT INTO verbose_parallel SELECT g, ARRAY[g % 100] FROM generate_series(1, 20000) g;
CREATE INDEX verbose_parallel_btree ON verbose_parallel (id);
CREATE INDEX verbose_parallel_expr ON verbose_parallel ((id + 1));
CREATE INDEX verbose_parallel_gin ON verbose_parallel USING gin (a);
CREATE INDEX verbose_parallel_brin ON verbose_parallel USING brin (id);
DELETE FROM verbose_parallel WHERE id % 2 = 0;
});
SKIP:
{
    my ($stdout, $stderr);
    is($node->psql('postgres', q{
SET min_parallel_index_scan_size = 0;
SET max_parallel_maintenance_workers = 2;
VACUUM (VERBOSE, PARALLEL 2, INDEX_CLEANUP ON) verbose_parallel;
}, stdout => \$stdout, stderr => \$stderr), 0,
       'parallel verbose vacuum succeeds');
    skip 'Parallel VACUUM is disabled in this build', 21
      if $stderr =~ /disabling parallel option of vacuum.*cannot vacuum tables in parallel/;
    like($stderr, qr/launched [1-9]\d* parallel vacuum worker/,
         'parallel accounting test actually launches workers');

    my $reported_wal = 0;
    my $index_wal = 0;
    for my $index ('verbose_parallel_btree', 'verbose_parallel_expr',
                   'verbose_parallel_gin', 'verbose_parallel_brin')
    {
        for my $phase ('index bulk delete', 'index cleanup')
        {
            my $report = resource_report($stderr, $index, $phase);
            ok(defined $report, "$index reports $phase");
            my ($wal) = ($report // '') =~ /^WAL usage: (\d+) records,/m;
            ok(defined $wal, "$index $phase has its own WAL counter");
            $index_wal += $wal // 0;
        }
    }
    cmp_ok($index_wal, '>', 0, 'index passes generate WAL');
    my $heap = resource_report($stderr, 'verbose_parallel', 'heap (excluding indexes)');
    my ($heap_wal) = ($heap // '') =~ /^WAL usage: (\d+) records,/m;
    ok(defined $heap_wal, 'parallel heap report has a nonnegative WAL counter');
    $reported_wal = $index_wal + ($heap_wal // 0);
    my ($summary) = $stderr =~ /finished vacuuming "postgres\.public\.verbose_parallel":(.*?)(?=\n(?:psql:[^\n]*?:\s*)?INFO:|\z)/s;
    my ($total_wal) = ($summary // '') =~ /^WAL usage: (\d+) records,/m;
    ok(defined $total_wal, 'overall VACUUM WAL counter is available');
    cmp_ok($reported_wal, '<=', $total_wal // 0,
           'heap and index reports do not count parallel index WAL twice');
}

# Timing output is conditional, while collection of buffer/WAL counters is not.
my ($stdout, $stderr);
is($node->psql('postgres',
  'SET track_io_timing = off; VACUUM (VERBOSE) verbose_heap',
  stdout => \$stdout, stderr => \$stderr), 0, 'verbose vacuum with I/O timing disabled');
like($stderr, qr/vacuum resource usage for "public\.verbose_heap"/,
     'resource report remains available without I/O timing');
unlike($stderr, qr/I\/O timings:/, 'disabled I/O timing is not printed');

$node->stop;
done_testing();
