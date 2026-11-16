# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed
# with this work for additional information regarding copyright
# ownership. The ASF licenses this file to You under the Apache
# License, Version 2.0 (the "License"); you may not use this file
# except in compliance with the License. You may obtain a copy of the
# License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied. See the License for the specific language governing
# permissions and limitations under the License.

# Test that vacuums entering the wraparound failsafe mode are counted in
# pg_stat_all_tables.vacuum_failsafe_count and aggregated per database in
# pg_stat_vacuum.vacuum_failsafe_count.
use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('main');
$node->init;
# The failsafe cutoff is clamped to 1.05 * autovacuum_freeze_max_age, so use
# the minimum allowed value to keep the number of XIDs to burn small.
$node->append_conf(
	'postgresql.conf', qq[
autovacuum = off
autovacuum_freeze_max_age = 100000
]);
$node->start;

$node->safe_psql(
	'postgres', qq[
	CREATE TABLE tab_failsafe (i int);
	INSERT INTO tab_failsafe SELECT generate_series(1, 100);
]);

# A vacuum without failsafe pressure must not bump the counter.
$node->safe_psql('postgres', 'VACUUM tab_failsafe;');
my $count = $node->safe_psql('postgres',
	q[SELECT vacuum_failsafe_count FROM pg_stat_all_tables WHERE relname = 'tab_failsafe';]
);
is($count, '0', 'normal vacuum does not count as failsafe');

# Age the table past 1.05 * autovacuum_freeze_max_age: burn XIDs with
# aborted subtransactions (each aborted subxact consumes an assigned XID).
$node->safe_psql(
	'postgres', qq[
	CREATE TABLE burn_xids (i int);
	DO \$\$
	BEGIN
		FOR i IN 1..110000 LOOP
			BEGIN
				INSERT INTO burn_xids VALUES (1);
				RAISE EXCEPTION 'burn';
			EXCEPTION WHEN OTHERS THEN
			END;
		END LOOP;
	END \$\$;
]);

# vacuum_failsafe_age = 0 makes the (clamped) cutoff kick in immediately.
$node->safe_psql(
	'postgres', qq[
	SET vacuum_failsafe_age = 0;
	SET vacuum_multixact_failsafe_age = 0;
	VACUUM tab_failsafe;
]);

$count = $node->safe_psql('postgres',
	q[SELECT vacuum_failsafe_count FROM pg_stat_all_tables WHERE relname = 'tab_failsafe';]
);
is($count, '1', 'failsafe vacuum counted in pg_stat_all_tables');

my $db_count = $node->safe_psql('postgres',
	q[SELECT vacuum_failsafe_count FROM pg_stat_vacuum WHERE datname = 'postgres';]
);
is($db_count, '1', 'failsafe vacuum counted in pg_stat_vacuum');

done_testing();
