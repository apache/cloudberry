# Copyright (c) 2026, PostgreSQL Global Development Group

# Resource instrumentation has a core consumer even without an extension.
use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

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
        my ($report) = $stderr =~
          /vacuum resource usage for "public\.\Q$table\E" \(\Q$phase\E\):\n(.*?)(?=\n(?:INFO|CONTEXT|WARNING):|\z)/s;
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
    my $index_phase = $storage eq 'heap' ? 'index bulk delete' : 'AO index vacuum';
    like($stderr, qr/vacuum resource usage for "public\.\Q${table}_idx\E" \(\Q$index_phase\E\):/,
         "$storage reports index resources separately");
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
