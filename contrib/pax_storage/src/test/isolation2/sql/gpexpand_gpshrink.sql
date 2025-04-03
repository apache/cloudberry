!\retcode yes | gpexpand -c;

!\retcode gpshrink -c;

!\retcode rm -r /tmp/datadirs/;

-- expand one segment and shrink 
!\retcode echo "localhost|localhost|7008|/tmp/datadirs/dbfast4/demoDataDir3|9|3|p
localhost|localhost|7009|/tmp/datadirs/dbfast_mirror4/demoDataDir3|10|3|m" > /tmp/testexpand;

create table test(a int);

insert into test select i from generate_series(1,100) i;

select gp_segment_id, count(*) from test group by gp_segment_id;

select count(*) from gp_segment_configuration;

select policytype, numsegments, distkey from gp_distribution_policy d, pg_class c where c.relname='test' and  c.oid=d.localoid;

!\retcode gpexpand -i /tmp/testexpand;

!\retcode gpexpand -i /tmp/testexpand;

select gp_segment_id, count(*) from test group by gp_segment_id;

select count(*) from gp_segment_configuration;

select policytype, numsegments, distkey from gp_distribution_policy d, pg_class c where c.relname='test' and  c.oid=d.localoid;

!\retcode gpshrink -i /tmp/testexpand;

!\retcode gpshrink -i /tmp/testexpand;

select gp_segment_id, count(*) from test group by gp_segment_id;

select count(*) from gp_segment_configuration;

select policytype, numsegments, distkey from gp_distribution_policy d, pg_class c where c.relname='test' and  c.oid=d.localoid;

!\retcode yes | gpexpand -c;

!\retcode gpshrink -c;

!\retcode rm -r /tmp/datadirs/;

drop table test;


-- expand one segment and shrink 
!\retcode echo "localhost|localhost|7008|/tmp/datadirs/dbfast4/demoDataDir3|9|3|p
localhost|localhost|7009|/tmp/datadirs/dbfast_mirror4/demoDataDir3|10|3|m" > /tmp/testexpand;

create table test_partitioned (a int, b int) partition by range (b) (start(1) end(101) every(20),default partition def);

insert into test_partitioned select i,i from generate_series(1,100) i;

select gp_segment_id, count(*) from test_partitioned group by gp_segment_id;

select count(*) from gp_segment_configuration;

select policytype, numsegments, distkey from gp_distribution_policy d, pg_class c where c.relname='test_partitioned' and  c.oid=d.localoid;

select policytype, numsegments, distkey from gp_distribution_policy d, pg_class c where c.relname='test_partitioned_1_prt_2' and  c.oid=d.localoid;

!\retcode gpexpand -i /tmp/testexpand;

!\retcode gpexpand -i /tmp/testexpand;

select gp_segment_id, count(*) from test_partitioned group by gp_segment_id;

select count(*) from gp_segment_configuration;

select policytype, numsegments, distkey from gp_distribution_policy d, pg_class c where c.relname='test_partitioned' and  c.oid=d.localoid;

select policytype, numsegments, distkey from gp_distribution_policy d, pg_class c where c.relname='test_partitioned_1_prt_2' and  c.oid=d.localoid;

!\retcode gpshrink -i /tmp/testexpand;

!\retcode gpshrink -i /tmp/testexpand;

select gp_segment_id, count(*) from test_partitioned group by gp_segment_id;

select count(*) from gp_segment_configuration;

select policytype, numsegments, distkey from gp_distribution_policy d, pg_class c where c.relname='test_partitioned' and  c.oid=d.localoid;

select policytype, numsegments, distkey from gp_distribution_policy d, pg_class c where c.relname='test_partitioned_1_prt_2' and  c.oid=d.localoid;

drop table test_partitioned;

!\retcode yes | gpexpand -c;

!\retcode gpshrink -c;

!\retcode rm -r /tmp/datadirs/;


-- expand two segment and shrink
!\retcode echo "localhost|localhost|7008|/tmp/datadirs/dbfast4/demoDataDir3|9|3|p
localhost|localhost|7009|/tmp/datadirs/dbfast_mirror4/demoDataDir3|10|3|m
localhost|localhost|7010|/tmp/datadirs/dbfast5/demoDataDir4|11|4|p
localhost|localhost|7011|/tmp/datadirs/dbfast_mirror5/demoDataDir4|12|4|m" > /tmp/testexpand;

create table test(a int);

insert into test select i from generate_series(1,100) i;

select gp_segment_id, count(*) from test group by gp_segment_id;

select count(*) from gp_segment_configuration;

select policytype, numsegments, distkey from gp_distribution_policy d, pg_class c where c.relname='test' and  c.oid=d.localoid;

!\retcode gpexpand -i /tmp/testexpand;

!\retcode gpexpand -i /tmp/testexpand;

select gp_segment_id, count(*) from test group by gp_segment_id;

select count(*) from gp_segment_configuration;

select policytype, numsegments, distkey from gp_distribution_policy d, pg_class c where c.relname='test' and  c.oid=d.localoid;

!\retcode gpshrink -i /tmp/testexpand;

!\retcode gpshrink -i /tmp/testexpand;

select gp_segment_id, count(*) from test group by gp_segment_id;

select count(*) from gp_segment_configuration;

select policytype, numsegments, distkey from gp_distribution_policy d, pg_class c where c.relname='test' and  c.oid=d.localoid;

drop table test;

!\retcode yes | gpexpand -c;

!\retcode gpshrink -c;

!\retcode rm -r /tmp/datadirs/;