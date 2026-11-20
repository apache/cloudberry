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

-- Cursor readers must retain the writer's in-progress subtransaction IDs.
8: CREATE SCHEMA sharedsnapshot_cursor;
8: CREATE TABLE sharedsnapshot_cursor.anchor(v oid) DISTRIBUTED BY (v);
8: INSERT INTO sharedsnapshot_cursor.anchor VALUES ('pg_class'::regclass::oid);
8: CREATE TABLE sharedsnapshot_cursor.p(a int) USING ao_column DISTRIBUTED BY (a) PARTITION BY LIST (a);
8: CREATE TABLE sharedsnapshot_cursor.p0 PARTITION OF sharedsnapshot_cursor.p FOR VALUES IN (0);
8: CREATE FUNCTION sharedsnapshot_cursor.add_part(child text, partvalue int) RETURNS void LANGUAGE plpgsql AS $$
   BEGIN /*in func*/
     EXECUTE format('CREATE TABLE sharedsnapshot_cursor.%I PARTITION OF sharedsnapshot_cursor.p FOR VALUES IN (%s)', child, partvalue); /*in func*/
   EXCEPTION WHEN OTHERS THEN /*in func*/
     RAISE; /*in func*/
   END; /*in func*/
   $$;

4: SET optimizer = off;
4: SET enable_indexscan = off;
4: SET enable_indexonlyscan = off;
4: SET enable_bitmapscan = off;

-- Keep the snapshot horizon below the DDL subtransaction.
7: BEGIN;
7: DO $$ BEGIN PERFORM txid_current(); END $$;

-- Reuse the entry DB reader with an older local snapshot.
4: BEGIN;
4: DECLARE warm_class CURSOR FOR SELECT c.oid FROM pg_class c JOIN sharedsnapshot_cursor.anchor a ON c.oid = a.v;
4: FETCH ALL FROM warm_class;
4: COMMIT;
4: BEGIN;
4: DECLARE warm_type CURSOR FOR SELECT c.oid FROM pg_type c JOIN sharedsnapshot_cursor.anchor a ON c.oid = a.v;
4: FETCH ALL FROM warm_type;
4: COMMIT;

5: BEGIN;
5: DO $$ BEGIN PERFORM txid_current(); END $$;
5: SELECT sharedsnapshot_cursor.add_part('p1', 1);

-- A newer completed XID makes the subtransaction fall below snapshot xmax.
6: DO $$ BEGIN PERFORM txid_current(); END $$;

-- Scan the uncommitted catalog tuples without marking their xmin invalid.
4: BEGIN;
4: DECLARE probe_class CURSOR FOR SELECT c.oid FROM pg_class c JOIN sharedsnapshot_cursor.anchor a ON c.oid = a.v;
4: FETCH ALL FROM probe_class;
4: COMMIT;
4: BEGIN;
4: DECLARE probe_type CURSOR FOR SELECT c.oid FROM pg_type c JOIN sharedsnapshot_cursor.anchor a ON c.oid = a.v;
4: FETCH ALL FROM probe_type;
4: COMMIT;
5: COMMIT;

-- A fresh session must see the committed catalogs and build the partition tree.
9: SELECT count(*) FROM pg_class WHERE relnamespace = 'sharedsnapshot_cursor'::regnamespace AND relname = 'p1';
9: SELECT count(*) FROM pg_type WHERE typnamespace = 'sharedsnapshot_cursor'::regnamespace AND typname IN ('p1', '_p1');
9: SELECT count(*) FROM sharedsnapshot_cursor.p;
9: SELECT sharedsnapshot_cursor.add_part('p2', 2);
9: SELECT count(*) FROM pg_inherits WHERE inhparent = 'sharedsnapshot_cursor.p'::regclass;
7: ROLLBACK;
9: DROP SCHEMA sharedsnapshot_cursor CASCADE;
