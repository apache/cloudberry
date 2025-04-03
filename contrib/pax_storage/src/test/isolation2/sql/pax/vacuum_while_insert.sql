-- @Description Ensures that a vacuum during insert operations is ok
-- 
DROP TABLE IF EXISTS pax_tbl;
CREATE TABLE pax_tbl (a INT);
insert into pax_tbl select generate_series(1,1000);
insert into pax_tbl select generate_series(1,1000);
insert into pax_tbl select generate_series(1,1000);
insert into pax_tbl select generate_series(1,1000);
insert into pax_tbl select generate_series(1,1000);
insert into pax_tbl select generate_series(1,1000);
insert into pax_tbl select generate_series(1,1000);
insert into pax_tbl select generate_series(1,1000);
insert into pax_tbl select generate_series(1,1000);
insert into pax_tbl select generate_series(1,1000);
insert into pax_tbl select generate_series(1,1000);
insert into pax_tbl select generate_series(1,1000);
insert into pax_tbl select generate_series(1,1000);
insert into pax_tbl select generate_series(1,1000);
insert into pax_tbl select generate_series(1,1000);
insert into pax_tbl select generate_series(1,1000);
insert into pax_tbl select generate_series(1,1000);
insert into pax_tbl select generate_series(1,1000);
insert into pax_tbl select generate_series(1,1000);
insert into pax_tbl select generate_series(1,1000);
insert into pax_tbl select generate_series(1,1000);

DELETE FROM pax_tbl WHERE a < 128;
1: BEGIN;
1>: insert into pax_tbl select generate_series(1001,2000);insert into pax_tbl select generate_series(1001,2000)
    ;insert into pax_tbl select generate_series(1001,2000);insert into pax_tbl select generate_series(1001,2000)
    ;insert into pax_tbl select generate_series(1001,2000);insert into pax_tbl select generate_series(1001,2000)
    ;insert into pax_tbl select generate_series(1001,2000);insert into pax_tbl select generate_series(1001,2000)
    ;insert into pax_tbl select generate_series(1001,2000);insert into pax_tbl select generate_series(1001,2000)
    ;insert into pax_tbl select generate_series(1001,2000);insert into pax_tbl select generate_series(1001,2000)
    ;insert into pax_tbl select generate_series(1001,2000);insert into pax_tbl select generate_series(1001,2000)
    ;insert into pax_tbl select generate_series(1001,2000);insert into pax_tbl select generate_series(1001,2000)
    ;insert into pax_tbl select generate_series(1001,2000);insert into pax_tbl select generate_series(1001,2000)
    ;insert into pax_tbl select generate_series(1001,2000);insert into pax_tbl select generate_series(1001,2000)
    ;COMMIT;
4: BEGIN;
4>: insert into pax_tbl select generate_series(1001,2000);insert into pax_tbl select generate_series(1001,2000)
    ;insert into pax_tbl select generate_series(1001,2000);insert into pax_tbl select generate_series(1001,2000)
    ;insert into pax_tbl select generate_series(1001,2000);insert into pax_tbl select generate_series(1001,2000)
    ;insert into pax_tbl select generate_series(1001,2000);insert into pax_tbl select generate_series(1001,2000)
    ;insert into pax_tbl select generate_series(1001,2000);insert into pax_tbl select generate_series(1001,2000)
    ;insert into pax_tbl select generate_series(1001,2000);insert into pax_tbl select generate_series(1001,2000)
    ;insert into pax_tbl select generate_series(1001,2000);insert into pax_tbl select generate_series(1001,2000)
    ;insert into pax_tbl select generate_series(1001,2000);insert into pax_tbl select generate_series(1001,2000)
    ;insert into pax_tbl select generate_series(1001,2000);insert into pax_tbl select generate_series(1001,2000)
    ;insert into pax_tbl select generate_series(1001,2000);insert into pax_tbl select generate_series(1001,2000)
    ;COMMIT;
2: VACUUM pax_tbl;
1<:
4<:
3: SELECT COUNT(*) FROM pax_tbl WHERE a = 1500;
3: INSERT INTO pax_tbl VALUES (0);
