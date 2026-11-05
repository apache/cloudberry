# Copyright (c) 2026, PostgreSQL Global Development Group

# Completed-vacuum work is cumulative and does not require relallfrozen.
use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('vacuum_vm_pages');
$node->init;
$node->append_conf('postgresql.conf', 'autovacuum = off');
$node->start;
$node->safe_psql('postgres', q{
CREATE TABLE vm_pages (id int, padding text);
INSERT INTO vm_pages SELECT g, repeat('x', 100) FROM generate_series(1, 4000) g;
VACUUM (FREEZE) vm_pages;
});
my $first = $node->safe_psql('postgres', q{
SELECT pages_frozen, pages_all_visible FROM pg_stat_all_tables
WHERE relid = 'vm_pages'::regclass
});
my ($frozen, $visible) = split /\|/, $first;
cmp_ok($frozen, '>', 0, 'VACUUM FREEZE records pages with freezing work');
cmp_ok($visible, '>', 0, 'VACUUM records new all-visible marks');
$node->safe_psql('postgres', 'VACUUM (FREEZE, DISABLE_PAGE_SKIPPING) vm_pages');
is($node->safe_psql('postgres', q{
SELECT pages_frozen, pages_all_visible FROM pg_stat_all_tables
WHERE relid = 'vm_pages'::regclass
}), $first, 'rescanning frozen visible pages does not recount them');
$node->safe_psql('postgres', q{
UPDATE vm_pages SET padding = 'changed';
VACUUM (FREEZE) vm_pages;
});
is($node->safe_psql('postgres', qq{
SELECT pages_frozen > $frozen AND pages_all_visible > $visible
       AND visible_page_marks_cleared > 0 AND frozen_page_marks_cleared > 0
FROM pg_stat_all_tables WHERE relid = 'vm_pages'::regclass
}), 't', 'new freezing and visibility work accumulates alongside cleared marks');
$node->safe_psql('postgres', q{
SELECT pg_stat_reset_single_table_counters('vm_pages'::regclass);
});
is($node->safe_psql('postgres', q{
SELECT pages_frozen = 0 AND pages_all_visible = 0
FROM pg_stat_all_tables WHERE relid = 'vm_pages'::regclass
}), 't', 'relation statistics reset clears both work counters');
is($node->safe_psql('postgres', q{
SELECT NOT EXISTS (SELECT FROM pg_attribute
                   WHERE attrelid = 'pg_class'::regclass
                     AND attname = 'relallfrozen')
}), 't', 'no relallfrozen catalog field is required');
$node->stop;
done_testing();
