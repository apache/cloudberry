set default_table_access_method = pax;
CREATE TABLE pax_test.all_typbyval_pg_types (
  id int,
  bool_col bool,
  char_col char,
  int2_col int2,
  cid_col cid,
  
  float4_col float4,
  int4_col int4,
  date_col date,
  
  oid_col oid,
  -- åxid_col xid,
  time_stamp_col timestamp,
  int8_col int8,
  -- xid8_col xid8,
  float8_col float8,
  money_col money,
  time_col time,
  timestamptz_col timestamptz,
  pg_lsn_col pg_lsn
) USING pax distributed by (id);

insert into pax_test.all_typbyval_pg_types values(1, true,'c',2,'cid',4.2,5,'2023-05-17 17:56:49',7,'2023-05-17 17:56:49',10,11.1111,12,'2023-05-17 17:56:49','2023-05-17 17:56:49', '16/0'),
(1, true,'c',2,'cid',4.2,5,'2023-05-17 17:56:49',7,'2023-05-17 17:56:49',10,11.1111,12,'2023-05-17 17:56:49','2023-05-17 17:56:49', '16/0'),
(1, true,'c',2,'cid',4.2,5,'2023-05-17 17:56:49',7,'2023-05-17 17:56:49',10,11.1111,12,'2023-05-17 17:56:49','2023-05-17 17:56:49', '16/0');
select * from pax_test.all_typbyval_pg_types;

create table pax_test.all_typlen_lt_0_pg_type (
  id int,
  name_col name,
  numeric_col numeric,
  text_col text,
  varchar_col varchar(128),
  point_col point
) USING pax distributed by (id);

insert into pax_test.all_typlen_lt_0_pg_type values(1,'hello', 1.23, 'text', 'varchar', point(1,2));
select * from pax_test.all_typlen_lt_0_pg_type;

drop table pax_test.all_typbyval_pg_types;
drop table pax_test.all_typlen_lt_0_pg_type;
