create table t_insert(a int);
select gp_inject_fault_infinite('fts_probe','skip',dbid) FROM gp_segment_configuration WHERE role = 'p' AND content = -1;
select gp_request_fts_probe_scan();
select gp_inject_fault('orc_writer_write_tuple','panic',dbid) FROM gp_segment_configuration WHERE role = 'p' AND content > -1;
-- failed because of fault injection
insert into t_insert select generate_series(1,10);

-- start_ignore
-- clear the fault inject, so the next insert will success.
-- put the reset operation in ignore range
select gp_inject_fault('orc_writer_write_tuple','reset',dbid) FROM gp_segment_configuration WHERE role = 'p' AND content > -1;
select gp_inject_fault('fts_probe','reset',dbid) FROM gp_segment_configuration WHERE role = 'p' AND content = -1;
-- end_ignore

-- success 
insert into t_insert select generate_series(1,10);
drop table t_insert;
