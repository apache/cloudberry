CREATE EXTENSION gp_url_tools;

SELECT url_tools_schema.encode_url('Hello World');
SELECT url_tools_schema.decode_url('Hello%20World');

SELECT url_tools_schema.encode_url(unnest) from unnest(string_to_array('http://hu.wikipedia.org/wiki/São_Paulo','/'));

SELECT url_tools_schema.encode_uri('http://hu.wikipedia.org/wiki/São_Paulo');
SELECT md5(url_tools_schema.decode_uri('http://hu.wikipedia.org/wiki/S%C3%A3o_Paulo'));

SELECT md5(url_tools_schema.decode_url('%u6D6A%u82B1%u4E00%u6735%u6735%20%u7B2C8%u96C6%20-%20%u89C6%u9891%u5728%u7EBF%u89C2%u770B%20-%20%u6D6A%u82B1%u4E00%u6735%u6735%20-%20%u8292%u679CTV'));


