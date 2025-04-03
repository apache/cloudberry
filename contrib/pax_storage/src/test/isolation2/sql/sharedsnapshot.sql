2: CREATE OR REPLACE PROCEDURE test(bdate text, edate text) LANGUAGE PLPGSQL AS $$/*in func*/
   BEGIN /*in func*/
     EXECUTE format('ALTER TABLE dummy2 ADD PARTITION START (date ''%s'') INCLUSIVE END (date ''%s'') EXCLUSIVE', bdate, edate);/*in func*/
     EXCEPTION/*in func*/
   	WHEN NO_DATA_FOUND THEN/*in func*/
   		RAISE EXCEPTION 'exception';/*in func*/
   END;/*in func*/
   $$;

vacuum full pg_class;

2: CREATE TABLE dummy2 (id int, date date, amt decimal(10,2))
DISTRIBUTED BY (id)
PARTITION BY RANGE (date)
(START (date '2017-01-31') INCLUSIVE
END (date '2017-02-01') EXCLUSIVE EVERY (INTERVAL '1 day') );

2: begin;
2: savepoint a;
2: call test('2020-01-01', '2020-01-02');

1: create table dummy1 as select sum(a.relnatts) from pg_class as a full join pg_class as b on a.relnatts = b.relnatts; 
1: drop table dummy1;
1: create table dummy1 as select sum(a.relnatts) from pg_class as a full join pg_class as b on a.relnatts = b.relnatts; 

2: commit;
2: call test('2020-01-02', '2020-01-03');
2: select count(1) from pg_inherits where inhparent = 'dummy2'::regclass;
2: drop table dummy2, dummy1;
