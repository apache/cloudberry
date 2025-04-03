set default_table_access_method = pax;
CREATE TABLE list_parted (a numeric, b int, c int8) PARTITION BY list (a);
CREATE TABLE sub_parted PARTITION OF list_parted for VALUES in (1) PARTITION BY list (b);

CREATE TABLE sub_part1(b int, c int8, a numeric);
alter table sub_part1 set distributed by (a);
ALTER TABLE sub_parted ATTACH PARTITION sub_part1 for VALUES in (1);
CREATE TABLE sub_part2(b int, c int8, a numeric);
alter table sub_part2 set distributed by (a);
ALTER TABLE sub_parted ATTACH PARTITION sub_part2 for VALUES in (2);

CREATE TABLE list_part1(a numeric, b int, c int8);
ALTER TABLE list_parted ATTACH PARTITION list_part1 for VALUES in (2,3);

INSERT into list_parted VALUES (2,5,50);
INSERT into list_parted VALUES (3,6,60);
INSERT into sub_parted VALUES (1,1,60);
INSERT into sub_parted VALUES (1,2,10);

CREATE TABLE non_parted (id int);
INSERT into non_parted VALUES (1), (1), (1), (2), (2), (2), (3), (3), (3);
select gp_segment_id,seq from gp_dist_random('pg_ext_aux.pg_pax_fastsequence') where 
    objid = 'list_part1'::regclass::oid or objid = 'sub_part1'::regclass::oid or objid = 'sub_part2'::regclass::oid;
ALTER TABLE list_parted SET DISTRIBUTED BY (c);
select gp_segment_id,seq from gp_dist_random('pg_ext_aux.pg_pax_fastsequence') where 
    objid = 'list_part1'::regclass::oid or objid = 'sub_part1'::regclass::oid or objid = 'sub_part2'::regclass::oid;

UPDATE list_parted t1 set a = 2 FROM non_parted t2 WHERE t1.a = t2.id and a = 1;

drop table list_parted;
drop table non_parted;


CREATE TABLE list_parted (a numeric, b int, c int8) PARTITION BY list (a);
CREATE TABLE sub_parted PARTITION OF list_parted for VALUES in (1) PARTITION BY list (b);

CREATE TABLE sub_part1(b int, c int8, a numeric);
alter table sub_part1 set distributed by (a);
ALTER TABLE sub_parted ATTACH PARTITION sub_part1 for VALUES in (1);
CREATE TABLE sub_part2(b int, c int8, a numeric);
alter table sub_part2 set distributed by (a);
ALTER TABLE sub_parted ATTACH PARTITION sub_part2 for VALUES in (2);

CREATE TABLE list_part1(a numeric, b int, c int8);
ALTER TABLE list_parted ATTACH PARTITION list_part1 for VALUES in (2,3);
-- make fastseq bigger than 1
INSERT into list_parted VALUES (2,5,50);
INSERT into list_parted VALUES (2,5,50);
INSERT into list_parted VALUES (2,5,50);
INSERT into list_parted VALUES (2,5,50);
INSERT into list_parted VALUES (2,5,50);
INSERT into list_parted VALUES (2,5,50);
INSERT into list_parted VALUES (3,6,60);
INSERT into list_parted VALUES (3,6,60);
INSERT into list_parted VALUES (3,6,60);
INSERT into list_parted VALUES (3,6,60);
INSERT into list_parted VALUES (3,6,60);
INSERT into list_parted VALUES (3,6,60);
INSERT into sub_parted VALUES (1,1,60);
INSERT into sub_parted VALUES (1,2,10);

CREATE TABLE non_parted (id int);
INSERT into non_parted VALUES (1), (1), (1), (2), (2), (2), (3), (3), (3);
select gp_segment_id,seq from gp_dist_random('pg_ext_aux.pg_pax_fastsequence') where 
    objid = 'list_part1'::regclass::oid or objid = 'sub_part1'::regclass::oid or objid = 'sub_part2'::regclass::oid;
ALTER TABLE list_parted SET DISTRIBUTED BY (c);
select gp_segment_id,seq from gp_dist_random('pg_ext_aux.pg_pax_fastsequence') where 
    objid = 'list_part1'::regclass::oid or objid = 'sub_part1'::regclass::oid or objid = 'sub_part2'::regclass::oid;

UPDATE list_parted t1 set a = 2 FROM non_parted t2 WHERE t1.a = t2.id and a = 1;

drop table list_parted;
drop table non_parted;

CREATE TABLE list_parted (a numeric, b int, c int8) PARTITION BY list (a);
CREATE TABLE sub_parted PARTITION OF list_parted for VALUES in (1) PARTITION BY list (b);

CREATE TABLE sub_part1(b int, c int8, a numeric);
alter table sub_part1 set distributed by (a);
ALTER TABLE sub_parted ATTACH PARTITION sub_part1 for VALUES in (1);
CREATE TABLE sub_part2(b int, c int8, a numeric);
alter table sub_part2 set distributed by (a);
ALTER TABLE sub_parted ATTACH PARTITION sub_part2 for VALUES in (2);

CREATE TABLE list_part1(a numeric, b int, c int8);
ALTER TABLE list_parted ATTACH PARTITION list_part1 for VALUES in (2,3);

INSERT into list_parted VALUES (2,5,50);
INSERT into list_parted VALUES (3,6,60);
INSERT into sub_parted VALUES (1,1,60);
INSERT into sub_parted VALUES (1,2,10);
select gp_segment_id,seq from gp_dist_random('pg_ext_aux.pg_pax_fastsequence') where 
    objid = 'list_part1'::regclass::oid or objid = 'sub_part1'::regclass::oid or objid = 'sub_part2'::regclass::oid;
select gp_inject_fault('fts_probe', 'skip', dbid) FROM gp_segment_configuration WHERE role = 'p' AND content > -1;
SELECT gp_inject_fault('pax_finish_swap_fast_fastsequence', 'panic', dbid) FROM gp_segment_configuration WHERE role = 'p' AND content > -1;
ALTER TABLE list_parted SET DISTRIBUTED BY (c);
select gp_segment_id,seq from gp_dist_random('pg_ext_aux.pg_pax_fastsequence') where 
    objid = 'list_part1'::regclass::oid or objid = 'sub_part1'::regclass::oid or objid = 'sub_part2'::regclass::oid;
-- start_ignore
SELECT gp_inject_fault('pax_finish_swap_fast_fastsequence', 'reset', dbid) FROM gp_segment_configuration WHERE role = 'p' AND content > -1;
-- end_ignore
SELECT gp_inject_fault('finish_prepared_after_record_commit_prepared', 'reset', dbid) FROM gp_segment_configuration WHERE role = 'p' AND content > -1;
select gp_inject_fault('fts_probe', 'reset', dbid) FROM gp_segment_configuration WHERE role = 'p' AND content > -1;

drop table list_parted;

