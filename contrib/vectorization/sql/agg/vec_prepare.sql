SET vector.enable_vectorization = off;

DROP TABLE IF EXISTS trow;
CREATE TABLE IF NOT EXISTS trow(c1 INT, c2 INT);
INSERT INTO trow SELECT generate_series(1, 10), generate_series(11, 20);
INSERT INTO trow SELECT generate_series(1, 10), generate_series(11, 20);

DROP TABLE IF EXISTS tagg;
CREATE TABLE tagg(cint4 int, cint8 bigint, cint2 smallint, cdate date, cts timestamp, ctext text, grpkey int) with(appendonly=true, orientation=column);
INSERT INTO tagg VALUES (1, 11, 21, '2016-3-1',  '2016-1-1 1:12:31', 'abc', 1);
INSERT INTO tagg VALUES (2, 12, 22, '2015-4-1',  '2015-2-1 2:22:35', 'def', 1);
INSERT INTO tagg VALUES (3, 13, 23, '2014-5-1',  '2014-3-1 3:13:39', 'ghi', 2);
INSERT INTO tagg VALUES (4, 14, 24, '2013-6-1',  '2013-4-1 4:21:32', 'jkl', 2);
INSERT INTO tagg VALUES (5, 15, 25, '2012-7-1',  '2012-5-1 5:20:53',  null, 3);
INSERT INTO tagg VALUES (6, 16, 26, '2011-8-1',  '2011-6-1 6:19:43', 'stu', 3);
INSERT INTO tagg VALUES (7, 17, 27, '2010-9-1',  '2010-7-1 7:16:33',  null, 4);
INSERT INTO tagg VALUES (8, 18, 28, '2009-10-1', '2009-8-1 8:08:23', 'mno', null);
INSERT INTO tagg VALUES (9, 19, 29, '2008-11-1', '2008-9-1 9:05:13', 'pqr', null);
INSERT INTO tagg SELECT * FROM tagg;
INSERT INTO tagg VALUES (null, null, null, null, null, null, 1);
INSERT INTO tagg VALUES (null, null, null, null, null, null, 1);
INSERT INTO tagg VALUES (null, null, null, null, null, null, 2);
INSERT INTO tagg VALUES (null, null, null, null, null, null, 2);
INSERT INTO tagg VALUES (null, null, null, null, null, null, null);
INSERT INTO tagg VALUES (null, null, null, null, null, null, null);
INSERT INTO tagg VALUES (null, null, null, null, null, null, 5);
INSERT INTO tagg VALUES (null, null, null, null, null, null, 5);

DROP TABLE IF EXISTS tnulls;
CREATE TABLE tnulls(cint4 int, cint8 bigint, cint2 smallint, cdate date, cts timestamp, ctext text, grpkey int) with(appendonly=true, orientation=column);
INSERT INTO tnulls VALUES (null, null, null, null, null, null, null);
INSERT INTO tnulls VALUES (null, null, null, null, null, null, null);

DROP TABLE IF EXISTS tempty;
CREATE TABLE tempty(cint4 int, cint8 bigint, cint2 smallint, cdate date, cts timestamp, ctext text, grpkey int) with(appendonly=true, orientation=column);