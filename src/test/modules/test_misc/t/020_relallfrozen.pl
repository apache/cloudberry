# Copyright (c) 2026, PostgreSQL Global Development Group

# Exercise the catalog update paths independently of vacuum reporting.
use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('relallfrozen');
$node->init;
$node->append_conf('postgresql.conf', 'autovacuum = off');
$node->start;
$node->safe_psql('postgres', q{
CREATE EXTENSION pg_visibility;
CREATE TABLE frozen_heap (id int, padding text);
INSERT INTO frozen_heap SELECT i, repeat('x', 100) FROM generate_series(1, 2000) i;
VACUUM (FREEZE) frozen_heap;
});

sub check_map
{
    my ($label) = @_;
    is($node->safe_psql('postgres', q{
SELECT relallfrozen = all_frozen AND relallvisible = all_visible
       AND relallfrozen <= relallvisible AND relallvisible <= relpages
FROM pg_class, pg_visibility_map_summary('frozen_heap')
WHERE oid = 'frozen_heap'::regclass;
}), 't', $label);
}

check_map('VACUUM FREEZE records both visibility-map counts');
is($node->safe_psql('postgres', q{
SELECT relallfrozen > 0 FROM pg_class WHERE oid = 'frozen_heap'::regclass
}), 't', 'frozen heap has all-frozen pages');
$node->safe_psql('postgres', q{
UPDATE frozen_heap SET padding = 'updated' WHERE id = 1;
ANALYZE frozen_heap;
});
check_map('ANALYZE refreshes both counts after a VM bit is cleared');
$node->safe_psql('postgres', 'CREATE INDEX frozen_heap_idx ON frozen_heap(id)');
check_map('CREATE INDEX preserves heap visibility-map counts');
is($node->safe_psql('postgres', q{
SELECT relallfrozen = 0 AND relallvisible = 0
FROM pg_class WHERE oid = 'frozen_heap_idx'::regclass
}), 't', 'index has zero visibility-map counts');
$node->safe_psql('postgres', 'VACUUM (FULL) frozen_heap');
check_map('VACUUM FULL swaps visibility-map statistics with the new heap');
$node->safe_psql('postgres', 'TRUNCATE frozen_heap');
check_map('TRUNCATE resets both visibility-map counts');
for my $orientation ('row', 'column')
{
    $node->safe_psql('postgres', qq{
CREATE TABLE frozen_ao_$orientation (id int)
WITH (appendoptimized=true, orientation=$orientation);
INSERT INTO frozen_ao_$orientation SELECT generate_series(1, 100);
CREATE INDEX frozen_ao_${orientation}_idx ON frozen_ao_$orientation(id);
VACUUM (ANALYZE) frozen_ao_$orientation;
});
    is($node->safe_psql('postgres', qq{
SELECT bool_and(relallfrozen = 0 AND relallvisible = 0)
FROM pg_class WHERE oid IN ('frozen_ao_$orientation'::regclass,
                            'frozen_ao_${orientation}_idx'::regclass)
}), 't', "AO $orientation table and index have no visibility map");
}
$node->stop;
done_testing();
