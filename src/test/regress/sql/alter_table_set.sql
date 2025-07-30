-- https://github.com/GreengageDB/greengage/issues/1109
--
-- ALTER TABLE ... SET WITH (REORGANIZE = true); should always redistribute the
-- data even with matching distribution policy
create table ats_dist_by_c (c int, d int) distributed by (c);
create table ats_dist_by_d (c int, d int) distributed by (d);

insert into ats_dist_by_c select i, 0 from generate_series(1, 47) i;
copy ats_dist_by_c to '/tmp/ats_dist_by_c<SEGID>' on segment;

-- load the data back from the segment file, but wrong distribution
set gp_enable_segment_copy_checking = false;
show gp_enable_segment_copy_checking;
copy ats_dist_by_d from '/tmp/ats_dist_by_c<SEGID>' on segment;

-- try to use the reorganize = true to fix it
alter table ats_dist_by_d set with (reorganize = true);
-- construct expected table
create table ats_expected_by_d (c int, d int) distributed by (d);
insert into ats_expected_by_d select * from ats_dist_by_c;
-- expect to see data distributed in the same way as the freshly constructed
-- table
select count(*) = 0 as has_same_distribution from
(select gp_segment_id, * from ats_dist_by_d except
	select gp_segment_id, * from ats_expected_by_d) t;

-- reload for random distribution test
truncate table ats_dist_by_d;
copy ats_dist_by_d from '/tmp/ats_dist_by_c<SEGID>' on segment;

-- we expect the new random distribution to differ from both the
-- distributed-by-c table and the distributed-by-d table
alter table ats_dist_by_d set with (reorganize = true) distributed randomly;
select count(*) > 0 as has_different_distribution from
(select gp_segment_id, * from ats_dist_by_d except
	select gp_segment_id, * from ats_dist_by_c) t;
select count(*) > 0 as has_different_distribution from
(select gp_segment_id, * from ats_dist_by_d except
	select gp_segment_id, * from ats_expected_by_d) t;

-- ALTER TABLE SET also shouldn't change table access method because of
-- changes in gp_default_storage_options.

-- Heap -> AOCO
create table tb_heap(id int, s1 text, s2 text);
insert into tb_heap select v, v::text, v::text FROM generate_series(1, 100) v;

alter table tb_heap drop column s1;
set gp_default_storage_options = 'appendonly=true, orientation=column';
alter table tb_heap set with (reorganize = true);

select max(id) from tb_heap;
select relstorage from pg_class where relname = 'tb_heap';

-- Heap -> AO
alter table tb_heap drop column s2;
set gp_default_storage_options = 'appendonly=true, orientation=row';
alter table tb_heap set with (reorganize = true);

select max(id) from tb_heap;
select relstorage from pg_class where relname = 'tb_heap';

-- AOCO -> Heap
set gp_default_storage_options = 'appendonly=true, orientation=column';
create table tb_aoco(id int, s1 text, s2 text);
insert into tb_aoco select v, v::text, v::text FROM generate_series(1, 100) v;

alter table tb_aoco drop column s1;
set gp_default_storage_options = 'appendonly=false';
alter table tb_aoco set with (reorganize = true);

select max(id) from tb_aoco;
select relstorage from pg_class where relname = 'tb_aoco';

-- AOCO -> AO
alter table tb_aoco drop column s2;
set gp_default_storage_options = 'appendonly=true, orientation=row';
alter table tb_aoco set with (reorganize = true);

select max(id) from tb_aoco;
select relstorage from pg_class where relname = 'tb_aoco';

-- AO -> Heap
set gp_default_storage_options = 'appendonly=true, orientation=row';
create table tb_ao(id int, s1 text, s2 text);
insert into tb_ao select v, v::text, v::text FROM generate_series(1, 100) v;

alter table tb_ao drop column s1;
set gp_default_storage_options = 'appendonly=false';
alter table tb_ao set with (reorganize = true);

select max(id) from tb_ao;
select relstorage from pg_class where relname = 'tb_ao';

-- AO -> AOCO
alter table tb_ao drop column s2;
set gp_default_storage_options = 'appendonly=true, orientation=column';
alter table tb_ao set with (reorganize = true);

select max(id) from tb_ao;
select relstorage from pg_class where relname = 'tb_ao';

-- Cleanup
drop table tb_heap, tb_aoco, tb_ao;
