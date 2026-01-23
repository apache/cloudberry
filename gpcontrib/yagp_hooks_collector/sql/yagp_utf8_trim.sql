CREATE EXTENSION IF NOT EXISTS yagp_hooks_collector;

CREATE OR REPLACE FUNCTION get_marked_query(marker TEXT)
RETURNS TEXT AS $$
    SELECT query_text
    FROM yagpcc.log
    WHERE query_text LIKE '%' || marker || '%'
    ORDER BY datetime DESC
    LIMIT 1
$$ LANGUAGE sql VOLATILE;

SET yagpcc.ignored_users_list TO '';
SET yagpcc.enable TO TRUE;

-- Test 1: 1 byte chars
SET yagpcc.max_text_size to 19;
SET yagpcc.logging_mode to 'TBL';
SELECT /*test1*/ 'HelloWorld';
RESET yagpcc.logging_mode;
SELECT octet_length(get_marked_query('test1')) = 19 AS correct_length;

-- Test 2: 2 byte chars
SET yagpcc.max_text_size to 19;
SET yagpcc.logging_mode to 'TBL';
SELECT /*test2*/ 'РУССКИЙЯЗЫК';
RESET yagpcc.logging_mode;
-- Character 'Р' has two bytes and cut in the middle => not included.
SELECT octet_length(get_marked_query('test2')) = 18 AS correct_length;

-- Test 3: 4 byte chars
SET yagpcc.max_text_size to 21;
SET yagpcc.logging_mode to 'TBL';
SELECT /*test3*/ '😀';
RESET yagpcc.logging_mode;
-- Emoji has 4 bytes and cut before the last byte => not included.
SELECT octet_length(get_marked_query('test3')) = 18 AS correct_length;

-- Cleanup
DROP FUNCTION get_marked_query(TEXT);
RESET yagpcc.max_text_size;
RESET yagpcc.logging_mode;
RESET yagpcc.enable;
RESET yagpcc.ignored_users_list;

DROP EXTENSION yagp_hooks_collector;
