\getenv abs_srcdir PG_ABS_SRCDIR
\set copy_converse_varify_error_file :abs_srcdir '/data/copy_converse_varify_error.data'
CREATE TABLE copy_converse_varify_error(a int, b text);
COPY copy_converse_varify_error FROM :'copy_converse_varify_error_file'
WITH(FORMAT text, delimiter '|', "null" E'\\N', newline 'LF', escape 'OFF')
LOG ERRORS SEGMENT REJECT LIMIT 10 ROWS;
SELECT * FROM copy_converse_varify_error;
DROP TABLE copy_converse_varify_error;

\set eol_on_next_raw_page_file :abs_srcdir '/data/eol_on_next_raw_page.data'
CREATE TABLE copy_eol_on_nextrawpage(b text);
COPY copy_eol_on_nextrawpage FROM :'eol_on_next_raw_page_file'
WITH(FORMAT text, delimiter '|', "null" E'\\N', newline 'LF', escape 'OFF')
LOG ERRORS SEGMENT REJECT LIMIT 10 ROWS;
SELECT count(*) FROM copy_eol_on_nextrawpage;
DROP TABLE copy_eol_on_nextrawpage;
