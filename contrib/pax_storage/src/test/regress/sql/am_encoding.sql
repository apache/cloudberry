

-- test am encoding 

drop table if exists t1_encoding_heap;
CREATE TABLE t1_encoding_heap (c1 int ENCODING (compresstype=zstd),
                  c2 char ENCODING (compresstype=zlib),
                  c3 char) using heap;

drop table if exists t1_encoding_aoco;
CREATE TABLE t1_encoding_aoco (c1 int ENCODING (compresstype=zstd),
                  c2 char ENCODING (compresstype=zlib),
                  c3 char) using ao_column;
\d+ t1_encoding_aoco
select attnum, attoptions from pg_attribute_encoding where attrelid='t1_encoding_aoco'::regclass::oid order by attnum ;


drop table if exists t2_encoding_heap;
CREATE TABLE t2_encoding_heap (c1 int ENCODING (compresstype=zlib),
                  c2 char ENCODING (compresstype=zstd),
                  c3 char,
                  COLUMN c3 ENCODING (compresstype=RLE_TYPE)
                  ) using heap;

drop table if exists t2_encoding_aoco;
CREATE TABLE t2_encoding_aoco (c1 int ENCODING (compresstype=zlib),
                  c2 char ENCODING (compresstype=zstd),
                  c3 char,
                  COLUMN c3 ENCODING (compresstype=RLE_TYPE)
                  ) using ao_column;
\d+ t2_encoding_aoco
select attnum, attoptions from pg_attribute_encoding where attrelid='t2_encoding_aoco'::regclass::oid order by attnum ;

drop table if exists t3_encoding_heap;
CREATE TABLE t3_encoding_heap (c1 int ENCODING (compresstype=zlib),
                  c2 char,
                  c3 text,
                  c4 smallint ENCODING (compresstype=none),
                  DEFAULT COLUMN ENCODING (compresstype=zstd),
                  COLUMN c3 ENCODING (compresstype=RLE_TYPE)
                  ) using heap;

drop table if exists t3_encoding_aoco;
CREATE TABLE t3_encoding_aoco (c1 int ENCODING (compresstype=zlib),
                  c2 char,
                  c3 text,
                  c4 smallint ENCODING (compresstype=none),
                  DEFAULT COLUMN ENCODING (compresstype=zstd),
                  COLUMN c3 ENCODING (compresstype=RLE_TYPE)
                  ) using ao_column;
\d+ t3_encoding_aoco
select attnum, attoptions from pg_attribute_encoding where attrelid='t3_encoding_aoco'::regclass::oid order by attnum ;

drop table if exists t4_encoding_heap;
CREATE TABLE t4_encoding_heap (c1 int,
                  c2 char ENCODING (compresstype=RLE_TYPE),
                  c3 char ENCODING (compresstype=RLE_TYPE, compresslevel=1)) 
                  using heap
                  with(COMPRESSTYPE=zstd, compresslevel=5);

drop table if exists t4_encoding_aoco;
CREATE TABLE t4_encoding_aoco (c1 int,
                  c2 char ENCODING (compresstype=RLE_TYPE),
                  c3 char ENCODING (compresstype=RLE_TYPE, compresslevel=1)) 
                  using ao_column
                  with(COMPRESSTYPE=zstd, compresslevel=5);
\d+ t4_encoding_aoco
select attnum, attoptions from pg_attribute_encoding where attrelid='t4_encoding_aoco'::regclass::oid order by attnum ;

drop table if exists t5_encoding_heap;
CREATE TABLE t5_encoding_heap (c1 int,
                  c2 char ENCODING (compresstype=RLE_TYPE),
                  c3 char, COLUMN c3 ENCODING (compresstype=RLE_TYPE, compresslevel=1)) 
                  using heap
                  with(COMPRESSTYPE=zstd, compresslevel=5);

drop table if exists t5_encoding_aoco;
CREATE TABLE t5_encoding_aoco (c1 int,
                  c2 char ENCODING (compresstype=RLE_TYPE),
                  c3 char, COLUMN c3 ENCODING (compresstype=RLE_TYPE, compresslevel=1)) 
                  using ao_column
                  with(COMPRESSTYPE=zstd, compresslevel=5);
\d+ t5_encoding_aoco
select attnum, attoptions from pg_attribute_encoding where attrelid='t5_encoding_aoco'::regclass::oid order by attnum ;

drop table if exists t6_encoding_aoco;
CREATE TABLE t6_encoding_aoco (c1 int,
                  c2 char,
                  c3 char) 
                  using ao_column
                  with(COMPRESSTYPE=zstd, compresslevel=5);
\d+ t6_encoding_aoco
select attnum, attoptions from pg_attribute_encoding where attrelid='t6_encoding_aoco'::regclass::oid order by attnum ;

drop table if exists t7_encoding_aoco;
CREATE TABLE t7_encoding_aoco (c1 int,
                  c2 char,
                  c3 char) 
                  using ao_column;
\d+ t7_encoding_aoco
select attnum, attoptions from pg_attribute_encoding where attrelid='t7_encoding_aoco'::regclass::oid order by attnum ;

drop table t1_encoding_aoco;
drop table t2_encoding_aoco;
drop table t3_encoding_aoco;
drop table t4_encoding_aoco;
drop table t5_encoding_aoco;
drop table t6_encoding_aoco;
drop table t7_encoding_aoco;

-- test am encoding with gp partition
drop table if exists t1_part_encoding_heap;
CREATE TABLE t1_part_encoding_heap (c1 int ENCODING (compresstype=zlib),
                  c2 char ENCODING (compresstype=zstd, blocksize=65536),
                  c3 text, COLUMN c3 ENCODING (compresstype=RLE_TYPE) )
    using heap
    PARTITION BY RANGE (c3) (START ('1900-01-01'::DATE)          
                             END ('2100-12-31'::DATE),
                             COLUMN c3 ENCODING (compresstype=zlib));

drop table if exists t1_part_encoding_aoco;
CREATE TABLE t1_part_encoding_aoco (c1 int ENCODING (compresstype=zlib),
                  c2 char ENCODING (compresstype=zstd, blocksize=65536),
                  c3 text, COLUMN c3 ENCODING (compresstype=RLE_TYPE) )
    using ao_column
    PARTITION BY RANGE (c3) (START ('1900-01-01'::DATE)          
                             END ('2100-12-31'::DATE),
                             COLUMN c3 ENCODING (compresstype=zlib));

\d+ t1_part_encoding_aoco_1_prt_1
select attnum, attoptions from pg_attribute_encoding where attrelid='t1_part_encoding_aoco_1_prt_1'::regclass::oid order by attnum;


drop table if exists t2_part_encoding_heap;
CREATE TABLE t2_part_encoding_heap (c1 int ENCODING (compresstype=zlib),
                  c2 char ENCODING (compresstype=zstd, blocksize=65536),
                  c3 text, COLUMN c3 ENCODING (compresstype=RLE_TYPE) )
    using heap
    PARTITION BY RANGE (c3) (START ('1900-01-01'::DATE)          
                             END ('2100-12-31'::DATE));

drop table if exists t2_part_encoding_aoco;
CREATE TABLE t2_part_encoding_aoco (c1 int ENCODING (compresstype=zlib),
                  c2 char ENCODING (compresstype=zstd, blocksize=65536),
                  c3 text, COLUMN c3 ENCODING (compresstype=zstd) )
    using ao_column
    PARTITION BY RANGE (c3) (START ('1900-01-01'::DATE)          
                             END ('2100-12-31'::DATE));
\d+ t2_part_encoding_aoco_1_prt_1
select attnum, attoptions from pg_attribute_encoding where attrelid='t2_part_encoding_aoco_1_prt_1'::regclass::oid order by attnum;

drop table if exists t3_part_encoding_heap;
CREATE TABLE t3_part_encoding_heap (c1 int,
                  c2 char,
                  c3 text)
    using heap
    PARTITION BY RANGE (c3) (START ('1900-01-01'::DATE)
                             END ('2100-12-31'::DATE),
                             COLUMN c3 ENCODING (compresstype=zlib));

drop table if exists t3_part_encoding_aoco;
CREATE TABLE t3_part_encoding_aoco (c1 int,
                  c2 char,
                  c3 text)
    using ao_column
    PARTITION BY RANGE (c3) (START ('1900-01-01'::DATE)
                             END ('2100-12-31'::DATE),
                             COLUMN c3 ENCODING (compresstype=zlib));
\d+ t3_part_encoding_aoco_1_prt_1
select attnum, attoptions from pg_attribute_encoding where attrelid='t3_part_encoding_aoco_1_prt_1'::regclass::oid order by attnum;

drop table if exists t4_part_encoding_heap;
CREATE TABLE t4_part_encoding_heap (i int, j int, k int, l int) 
    using heap
    PARTITION BY range(i) SUBPARTITION BY range(j)
    (
       partition p1 start(1) end(2)
       ( subpartition sp1 start(1) end(2) 
         column i encoding(compresstype=zlib)
       ), 
       partition p2 start(2) end(3)
       ( subpartition sp1 start(1) end(2)
           column i encoding(compresstype=rle)
           column k encoding(compresstype=zstd)
       )
    );

drop table if exists t4_part_encoding_aoco;
CREATE TABLE t4_part_encoding_aoco (i int, j int, k int, l int) 
    using ao_column
    PARTITION BY range(i) SUBPARTITION BY range(j)
    (
       partition p1 start(1) end(2)
       ( subpartition sp1 start(1) end(2) 
         column i encoding(compresstype=zlib)
       ), 
       partition p2 start(2) end(3)
       ( subpartition sp1 start(1) end(2)
           column i encoding(compresstype=zlib)
           column k encoding(compresstype=zstd)
       )
    );
\d+ t4_part_encoding_aoco_1_prt_p1_2_prt_sp1
\d+ t4_part_encoding_aoco_1_prt_p2_2_prt_sp1
select attnum, attoptions from pg_attribute_encoding where attrelid='t4_part_encoding_aoco_1_prt_p1_2_prt_sp1'::regclass::oid order by attnum;
select attnum, attoptions from pg_attribute_encoding where attrelid='t4_part_encoding_aoco_1_prt_p2_2_prt_sp1'::regclass::oid order by attnum;

drop table if exists t5_part_encoding_heap;
CREATE TABLE t5_part_encoding_heap (i int ENCODING (compresstype=zlib), j int, k int, l int, column l encoding(compresstype=zstd)) 
    using heap
    PARTITION BY range(i) SUBPARTITION BY range(j)
    (
       partition p1 start(1) end(2)
       ( subpartition sp1 start(1) end(2) 
         column i encoding(compresstype=zlib)
       ), 
       partition p2 start(2) end(3)
       ( subpartition sp1 start(1) end(2)
           column i encoding(compresstype=zlib)
           column k encoding(compresstype=zstd)
       )
    );

drop table if exists t5_part_encoding_aoco;
CREATE TABLE t5_part_encoding_aoco (i int ENCODING (compresstype=RLE_TYPE), j int, k int, l int, column l encoding(compresstype=zstd)) 
    using ao_column
    PARTITION BY range(i) SUBPARTITION BY range(j)
    (
       partition p1 start(1) end(2)
       ( subpartition sp1 start(1) end(2) 
         column i encoding(compresstype=zlib)
       ), 
       partition p2 start(2) end(3)
       ( subpartition sp1 start(1) end(2)
           column j encoding(compresstype=zlib)
           column k encoding(compresstype=zstd)
       )
    );

\d+ t5_part_encoding_aoco_1_prt_p1_2_prt_sp1
\d+ t5_part_encoding_aoco_1_prt_p2_2_prt_sp1
select attnum, attoptions from pg_attribute_encoding where attrelid='t5_part_encoding_aoco_1_prt_p1_2_prt_sp1'::regclass::oid order by attnum;
select attnum, attoptions from pg_attribute_encoding where attrelid='t5_part_encoding_aoco_1_prt_p2_2_prt_sp1'::regclass::oid order by attnum;

drop table t1_part_encoding_aoco;
drop table t2_part_encoding_aoco;
drop table t3_part_encoding_aoco;
drop table t4_part_encoding_aoco;
drop table t5_part_encoding_aoco;

-- test create type with encoding clause

CREATE FUNCTION int33_in(cstring) RETURNS int33
  STRICT IMMUTABLE LANGUAGE internal AS 'int4in';
CREATE FUNCTION int33_out(int33) RETURNS cstring
  STRICT IMMUTABLE LANGUAGE internal AS 'int4out';

CREATE TYPE int33 (
   internallength = 4,
   input = int33_in,
   output = int33_out,
   alignment = int4,
   default = 123,
   passedbyvalue,
   compresstype="zlib",
   blocksize=65536,
   compresslevel=1
);

drop table if exists t1_type_int33_heap;
create table t1_type_int33_heap (c1 int33) using heap;
\d+ t1_type_int33_heap
select attnum, attoptions from pg_attribute_encoding where attrelid='t1_type_int33_heap'::regclass::oid order by attnum;

drop table if exists t1_type_int33_aoco;
create table t1_type_int33_aoco (c1 int33) using ao_column;
\d+ t1_type_int33_aoco
select attnum, attoptions from pg_attribute_encoding where attrelid='t1_type_int33_aoco'::regclass::oid order by attnum;

drop table t1_type_int33_heap;
drop table t1_type_int33_aoco;

-- test no implement am encoding callback table still can use relation WITH option

CREATE TABLE t1_heap (a int) WITH (autovacuum_enabled=true, autovacuum_analyze_scale_factor=0.3, fillfactor=32);
CREATE TABLE t2_heap (a int) WITH (autovacuum_enabled=true, autovacuum_analyze_scale_factor=0.3, fillfactor=32);

drop table t1_heap;
drop table t2_heap;
