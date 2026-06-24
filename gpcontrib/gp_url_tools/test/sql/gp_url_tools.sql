-- start_ignore
CREATE EXTENSION IF NOT EXISTS gp_url_tools;
-- end_ignore
SET client_encoding TO UTF8;

-- Basic encode/decode with ASCII and %XX escaping.
SELECT url_tools_schema.encode_url('Hello World');
SELECT url_tools_schema.decode_url('Hello%20World');

-- encode_url() should escape reserved URL characters like ':'.
SELECT url_tools_schema.encode_url(unnest) from unnest(string_to_array('http://hu.wikipedia.org/wiki/São_Paulo','/'));

-- encode_uri() keeps URI delimiters, decode_uri() reverses UTF-8 %XX escaping.
SELECT url_tools_schema.encode_uri('http://hu.wikipedia.org/wiki/São_Paulo');
SELECT md5(url_tools_schema.decode_uri('http://hu.wikipedia.org/wiki/S%C3%A3o_Paulo'));

-- Legacy UTF-16 %uXXXX decoding for BMP characters.
SELECT md5(url_tools_schema.decode_url('%u6D6A%u82B1%u4E00%u6735%u6735%20%u7B2C8%u96C6%20-%20%u89C6%u9891%u5728%u7EBF%u89C2%u770B%20-%20%u6D6A%u82B1%u4E00%u6735%u6735%20-%20%u8292%u679CTV'));

-- Single UTF-16 surrogate pair should decode to one Unicode character.
SELECT url_tools_schema.decode_url('%uD83D%uDE00');

-- Surrogate pair should also decode correctly in the middle of a string.
SELECT url_tools_schema.decode_url('hello%uD83D%uDE00world');

-- Mixed input: ASCII, UTF-8 %XX, UTF-16 BMP, and UTF-16 surrogate pair.
SELECT url_tools_schema.decode_url('A%20%C3%A3%20%u6D6A%20%uD83D%uDE00');

-- Truncated surrogate pair should raise an error.
SELECT url_tools_schema.decode_url('%uD83D');

-- High surrogate followed by a non-low-surrogate code unit should fail.
SELECT url_tools_schema.decode_url('%uD83D%u0041');

DROP EXTENSION gp_url_tools;
