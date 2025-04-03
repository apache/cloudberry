--
-- Test different combinations of client and server encodings with COPY.
--
CREATE DATABASE utf8db ENCODING 'utf8' TEMPLATE=template0 LC_COLLATE='C' LC_CTYPE='C';
CREATE DATABASE latin1db ENCODING 'latin1' TEMPLATE=template0 LC_COLLATE='C' LC_CTYPE='C';

-- First, connect to the UTF-8 database, and use COPY TO with non-ASCII data.
-- Use both explicit ENCODING, and client_encoding, to specify the output
-- encoding.
\c utf8db
set client_encoding='utf8';
CREATE TABLE enctest (t text);
insert into enctest values (chr(196)); -- Latin Capital Letter a with Diaeresis

-- with UTF-8 as the server encoding, it should be stored as two bytes.
select octet_length(t) from enctest;

copy enctest to '/tmp/enctest_utf_to_latin1-1' encoding 'latin1';

set client_encoding='latin1';
copy enctest to stdout;
copy enctest to '/tmp/enctest_utf_to_latin1-2';

-- Connect to 'latin1' database, and load back the files we just created.
-- This is to check that they were created correctly, and that the ENCODING
-- option works correctly also in COPY FROM.
\c latin1db
CREATE TABLE enctest (t text);

set client_encoding='latin1';
copy enctest from '/tmp/enctest_utf_to_latin1-1';
copy enctest from '/tmp/enctest_utf_to_latin1-2';

set client_encoding='utf8';

copy enctest from '/tmp/enctest_utf_to_latin1-1' encoding 'latin1';
copy enctest from '/tmp/enctest_utf_to_latin1-2' encoding 'latin1';

-- with latin1 as the server encoding, the character we used in the tests should be
-- stored as one byte.
select octet_length(t) from enctest;

select * from enctest;
copy enctest to stdout;

\c regression
drop database utf8db;
drop database latin1db;
