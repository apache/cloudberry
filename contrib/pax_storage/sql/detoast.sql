set default_table_access_method = pax;
CREATE TABLE toasttest_external(f1 text);
-- The storage `EXTERNAL` allows out-of-line storage but not compression.
alter table toasttest_external alter column f1 set storage external;
-- These tests are sensitive to block size. In CBDB, the block
-- size is 32 kB, whereas in PostgreSQL it's 8kB. Therefore make
-- the data 4x larger here.
INSERT INTO toasttest_external values (repeat('1234567890',300*4));
INSERT INTO toasttest_external values (repeat('1234567890',300*4));
INSERT INTO toasttest_external values (repeat('1234567890',300*4));
INSERT INTO toasttest_external values (repeat('1234567890',300*4));
SELECT reltoastrelid = 0 AS is_empty
  FROM pg_class where relname = 'toasttest_external';

create table toasttest_external_pax(f1 text) using pax;
insert into toasttest_external_pax select * from toasttest_external;
drop table toasttest_external;
-- If pax insert toast here, Then after drop toasttest_external, toast 
-- will not get the source data.
select length(f1) from toasttest_external_pax;
drop table toasttest_external_pax;


CREATE TABLE toasttest_compress(f1 text);
-- The storage `MAIN` allows compression but not out-of-line storage. 
alter table toasttest_compress alter column f1 set storage main;
-- about 1M
INSERT INTO toasttest_compress values (repeat('1234567890123456',1024 * 64));
-- should be true, becase it's not store in toast table
SELECT reltoastrelid = 0 AS is_empty FROM pg_class where relname = 'toasttest_compress';

create table toasttest_compress_pax(f1 text) using pax;
insert into toasttest_compress_pax select * from toasttest_compress;
drop table toasttest_compress;
select length(f1) from toasttest_compress_pax;
drop table toasttest_compress_pax;

CREATE TABLE toasttest_extended(f1 text);
-- The storage `EXTENDED` allows both compression and out-of-line storage. 
alter table toasttest_extended alter column f1 set storage EXTENDED;
-- about 1M, will use out-of-line storage
INSERT INTO toasttest_extended values (repeat('1234567890123456',1024 * 64));
-- about 80k , will use compression storage
INSERT INTO toasttest_extended values (repeat('1234567890123456',1024 * 5));
SELECT reltoastrelid = 0 AS is_empty FROM pg_class where relname = 'toasttest_extended';

create table toasttest_extended_pax(f1 text) using pax;
insert into toasttest_extended_pax select * from toasttest_extended;
drop table toasttest_extended;
select length(f1) from toasttest_extended_pax;
drop table toasttest_extended_pax;