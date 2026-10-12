# Copyright (c) 2026, PostgreSQL Global Development Group

# VACUUM work has a native reset that does not clear other statistics.
use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('vacuum_work_reset');
$node->init;
$node->append_conf('postgresql.conf', 'autovacuum = off');
$node->start;
$node->safe_psql('postgres', 'CREATE DATABASE other_work');
for my $db ('postgres', 'other_work')
{
    $node->safe_psql($db, q{
CREATE TABLE reset_work (id int PRIMARY KEY);
INSERT INTO reset_work SELECT generate_series(1, 1000);
DELETE FROM reset_work WHERE id <= 100;
ANALYZE reset_work;
VACUUM (FREEZE) reset_work;
SELECT pg_stat_force_next_flush();
});
}
my $work_sql = q{
SELECT row_to_json(s) FROM pg_stat_get_vacuum_stats('reset_work'::regclass) s
};
my $db_work_sql = q{
SELECT row_to_json(s) FROM pg_stat_get_vacuum_database_stats(
 (SELECT oid FROM pg_database WHERE datname = current_database())) s
};
my $other_sql = q{
SELECT n_tup_ins, n_tup_del, n_mod_since_analyze, vacuum_count, analyze_count,
       last_vacuum, last_analyze, total_vacuum_time, total_vacuum_delay_time
  FROM pg_stat_all_tables WHERE relname = 'reset_work'
};
my $zero_sql = q{
SELECT bool_and(value::bigint = 0)
  FROM pg_stat_get_vacuum_stats('reset_work'::regclass) s,
       LATERAL jsonb_each_text(to_jsonb(s))
};
my $before = $node->safe_psql('postgres', $work_sql);
my $other_db_before = $node->safe_psql('other_work', $work_sql);
my $db_before = $node->safe_psql('postgres', $db_work_sql);
my $other_before = $node->safe_psql('postgres', $other_sql);

$node->safe_psql('postgres', 'CREATE ROLE work_reader');
for my $call ('pg_stat_reset_vacuum_stats()',
              q[pg_stat_reset_vacuum_stats('reset_work'::regclass)],
              'gp_stat_reset_vacuum_stats()')
{
    my ($stdout, $stderr);
    my $ret = $node->psql('postgres', "SET ROLE work_reader; SELECT $call",
        stdout => \$stdout, stderr => \$stderr);
    isnt($ret, 0, "$call denied without reset privilege");
    like($stderr, qr/permission denied for function/, 'function ACL protects reset');
}
is($node->safe_psql('postgres', $work_sql), $before, 'denied reset preserves work');
$node->safe_psql('postgres', q{
SELECT pg_stat_reset_vacuum_stats(0);
SELECT pg_stat_reset_vacuum_stats(4294967294);
});
is($node->safe_psql('postgres', $work_sql), $before,
   'missing relation and OID zero do not reset the database');

$node->safe_psql('postgres', q{
GRANT EXECUTE ON FUNCTION pg_stat_reset_vacuum_stats(oid) TO work_reader;
SET ROLE work_reader;
SELECT pg_stat_reset_vacuum_stats('reset_work'::regclass);
});
is($node->safe_psql('postgres', $zero_sql), 't', 'relation reset clears every work field');
is($node->safe_psql('postgres', $other_sql), $other_before,
   'relation reset preserves timing, invocation counts and other statistics');
is($node->safe_psql('postgres', $db_work_sql), $db_before,
   'relation reset preserves database work aggregate');
is($node->safe_psql('postgres', q{
SELECT tuples_deleted FROM pg_stat_get_vacuum_stats('reset_work_pkey'::regclass)
}), '100', 'relation reset leaves index work untouched');
is($node->safe_psql('other_work', $work_sql), $other_db_before,
   'relation reset leaves other databases untouched');

$node->safe_psql('postgres', q{
SELECT pg_stat_reset_vacuum_stats('reset_work_pkey'::regclass);
VACUUM (DISABLE_PAGE_SKIPPING) reset_work;
VACUUM pg_shdepend;
SELECT pg_stat_force_next_flush();
});
is($node->safe_psql('postgres', q{
SELECT pages_scanned > 0 FROM pg_stat_get_vacuum_stats('reset_work'::regclass)
}), 't', 'work accumulation resumes after reset');
my $shared_sql = q{
SELECT row_to_json(s) FROM pg_stat_get_vacuum_stats('pg_shdepend'::regclass) s
};
my $shared_before = $node->safe_psql('postgres', $shared_sql);
$other_before = $node->safe_psql('postgres', $other_sql);
$node->safe_psql('postgres', 'SELECT pg_stat_reset_vacuum_stats()');
is($node->safe_psql('postgres', $zero_sql), 't', 'database reset clears relation work');
is($node->safe_psql('postgres', q{
SELECT bool_and(value::bigint = 0)
  FROM pg_stat_get_vacuum_database_stats(
    (SELECT oid FROM pg_database WHERE datname = current_database())) s,
    LATERAL jsonb_each_text(to_jsonb(s))
}), 't', 'database reset clears every database work field');
is($node->safe_psql('postgres', $other_sql), $other_before,
   'database reset preserves other relation statistics');
is($node->safe_psql('postgres', $shared_sql), $shared_before,
   'database reset leaves shared catalogs untouched');
is($node->safe_psql('other_work', $work_sql), $other_db_before,
   'database reset leaves another database untouched');
$node->safe_psql('postgres',
    q[SELECT pg_stat_reset_vacuum_stats('pg_shdepend'::regclass)]);
is($node->safe_psql('other_work', q{
SELECT pages_scanned FROM pg_stat_get_vacuum_stats('pg_shdepend'::regclass)
}), '0', 'explicit shared-catalog reset is visible from another database');

# A database reset must not republish this backend's pre-reset pending work.
$node->safe_psql('postgres', q{
VACUUM (DISABLE_PAGE_SKIPPING) reset_work;
SELECT pg_stat_reset_vacuum_stats(NULL);
SELECT pg_stat_force_next_flush();
});
is($node->safe_psql('postgres', q{
SELECT pages_scanned FROM pg_stat_get_vacuum_database_stats(
 (SELECT oid FROM pg_database WHERE datname = current_database()))
}), '0', 'reset discards local pending database work before a later flush');
$node->restart;
is($node->safe_psql('postgres', $zero_sql), 't', 'work stays reset after a clean restart');
is($node->safe_psql('other_work', $work_sql), $other_db_before,
   'untouched database history survives restart');
$node->stop;
done_testing();
