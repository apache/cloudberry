drop table if exists _tmp_table;
create table _tmp_table (i1 int, i2 int, i3 int, i4 int);
insert into _tmp_table select i, i % 100, i % 10000, i % 75 from generate_series(0,99999) i;

-- make sort to spill
set statement_mem="2MB";
set gp_cte_sharing=on;

select gp_inject_fault('execsort_sort_mergeruns', 'reset', 1);
-- set QueryFinishPending=true in sort mergeruns. This will stop sort and set result_tape to NULL
select gp_inject_fault('execsort_sort_mergeruns', 'finish_pending', 1);

-- return results although sort will be interrupted in one of the segments 
select DISTINCT S from (select row_number() over(partition by i1 order by i2) AS T, count(*) over (partition by i1) AS S from _tmp_table) AS TMP;

-- In single node, merge run won't be triggered.
-- start_ignore
select gp_inject_fault('execsort_sort_mergeruns', 'status', 1);
-- end_ignore

select gp_inject_fault('execsort_dumptuples', 'reset', 1);
-- set QueryFinishPending=true in sort dumptuples. This will stop sort.
select gp_inject_fault('execsort_dumptuples', 'finish_pending', 1);

-- return results although sort will be interrupted in one of the segments 
select DISTINCT S from (select row_number() over(partition by i1 order by i2) AS T, count(*) over (partition by i1) AS S from _tmp_table) AS TMP;

-- In single node, merge run won't be triggered.
-- start_ignore
select gp_inject_fault('execsort_dumptuples', 'status', 1);
-- end_ignore

-- ORCA does not trigger sort_bounded_heap() in following queries
set optimizer=off;

select gp_inject_fault('execsort_sort_bounded_heap', 'reset', 1);
-- set QueryFinishPending=true in sort_bounded_heap. This will stop sort.
select gp_inject_fault('execsort_sort_bounded_heap', 'finish_pending', 1);

-- In cbdb single node, if we inject fault in the single node, no data should return. 
-- start_ignore
-- return results although sort will be interrupted in one of the segments 
select i1 from _tmp_table order by i2 limit 3;
-- end_ignore

select gp_inject_fault('execsort_sort_bounded_heap', 'status', 1);

-- test if Share Input Scan deletes memory correctly when QueryFinishPending and its child has been eagerly freed,
-- where the child is a Sort node
drop table if exists testsisc;
create table testsisc (i1 int, i2 int, i3 int, i4 int); 
insert into testsisc select i, i % 1000, i % 100000, i % 75 from
(select generate_series(1, nsegments * 50000) as i from 
	(select count(*) as nsegments from gp_segment_configuration where role='p' and content >= -1) foo) bar; 

set gp_resqueue_print_operator_memory_limits=on;
set statement_mem='2MB';
-- ORCA does not generate ShareInputScan with a Sort node underneath it. For
-- the following query, ORCA disregards the order by inside the cte definition;
-- planner on the other hand does not.
set optimizer=off;
select gp_inject_fault('execshare_input_next', 'reset', 1);
-- Set QueryFinishPending to true after ShareInputScan has retrieved the first tuple. 
-- This will eagerly free the memory context of Share Input Scan's child node.  
select gp_inject_fault('execshare_input_next', 'finish_pending', 1);

-- In cbdb single node, if we inject fault in the single node, no data should return. 
-- start_ignore
with cte as (select i2 from testsisc order by i2)
select * from cte c1, cte c2 limit 2;
-- end_ignore
select gp_inject_fault('execshare_input_next', 'status', 1);

-- test if Share Input Scan deletes memory correctly when QueryFinishPending and its child has been eagerly freed,
-- where the child is a Sort node and sort_mk algorithm is used


select gp_inject_fault('execshare_input_next', 'reset', 1);
-- Set QueryFinishPending to true after ShareInputScan has retrieved the first tuple. 
-- This will eagerly free the memory context of Share Input Scan's child node.  
select gp_inject_fault('execshare_input_next', 'finish_pending', 1);

-- In cbdb single node, if we inject fault in the single node, no data should return. 
-- start_ignore
with cte as (select i2 from testsisc order by i2)
select * from cte c1, cte c2 limit 2;
-- end_ignore

select gp_inject_fault('execshare_input_next', 'status', 1);

-- Disable faultinjectors
select gp_inject_fault('execsort_sort_mergeruns', 'reset', 1);
select gp_inject_fault('execsort_dumptuples', 'reset', 1);
select gp_inject_fault('execsort_sort_bounded_heap', 'reset', 1);
select gp_inject_fault('execshare_input_next', 'reset', 1);

drop table testsisc;
