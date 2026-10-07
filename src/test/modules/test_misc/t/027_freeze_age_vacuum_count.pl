# Copyright (c) 2026, PostgreSQL Global Development Group

use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('vacstat_freeze_age');
$node->init;
$node->append_conf('postgresql.conf', q{
autovacuum = off
});
$node->start;
$node->safe_psql('postgres', q{
CREATE TABLE freeze_age_heap (id int);
INSERT INTO freeze_age_heap SELECT generate_series(1, 100);
VACUUM freeze_age_heap;
});
my $count_sql = q{
SELECT freeze_age_vacuum_count FROM pg_stat_vacuum_tables
 WHERE relname = 'freeze_age_heap'};
is($node->safe_psql('postgres', $count_sql), '0',
   'ordinary vacuum of a young table does not count as freeze-age driven');

$node->safe_psql('postgres', 'VACUUM (DISABLE_PAGE_SKIPPING) freeze_age_heap');
is($node->safe_psql('postgres', $count_sql), '0',
   'DISABLE_PAGE_SKIPPING alone does not count despite an aggressive scan');

$node->safe_psql('postgres', q{
SET vacuum_freeze_table_age = 0;
VACUUM freeze_age_heap;
});
is($node->safe_psql('postgres', $count_sql), '1',
   'XID freeze age counts a manual vacuum');

# safe_psql opens a new connection, restoring the default XID freeze age.
$node->safe_psql('postgres', q{
SET vacuum_multixact_freeze_table_age = 0;
VACUUM freeze_age_heap;
});
is($node->safe_psql('postgres', $count_sql), '2',
   'MultiXact freeze age independently counts a vacuum');

$node->safe_psql('postgres', 'VACUUM (FREEZE) freeze_age_heap');
is($node->safe_psql('postgres', $count_sql), '3',
   'VACUUM FREEZE counts once even when both age conditions hold');
$node->safe_psql('postgres', 'VACUUM (FREEZE, DISABLE_PAGE_SKIPPING) freeze_age_heap');
is($node->safe_psql('postgres', $count_sql), '4',
   'explicit page scanning does not double count a freeze-age vacuum');
$node->safe_psql('postgres', 'VACUUM freeze_age_heap');
is($node->safe_psql('postgres', $count_sql), '4',
   'a later ordinary vacuum does not reuse the preceding classification');
$node->safe_psql('postgres', 'VACUUM (FULL, FREEZE) freeze_age_heap');
is($node->safe_psql('postgres', $count_sql), '4',
   'VACUUM FULL does not enter the lazy-vacuum counter');

$node->safe_psql('postgres', q{
SET track_counts = off;
VACUUM (FREEZE) freeze_age_heap;
});
is($node->safe_psql('postgres', $count_sql), '4',
   'disabled collection does not count a freeze-age vacuum');
$node->restart;
is($node->safe_psql('postgres', $count_sql), '4',
   'freeze-age vacuum count survives a clean restart');

$node->safe_psql('postgres', q{
SELECT pg_stat_reset_single_table_counters('freeze_age_heap'::regclass)});
is($node->safe_psql('postgres', $count_sql), '0',
   'relation reset clears the freeze-age vacuum count');
$node->safe_psql('postgres', 'VACUUM (FREEZE) freeze_age_heap');
is($node->safe_psql('postgres', $count_sql), '1',
   'a freeze-age vacuum after reset starts a new total');

$node->stop;
done_testing();
