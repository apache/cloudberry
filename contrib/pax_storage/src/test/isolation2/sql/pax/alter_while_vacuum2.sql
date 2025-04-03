-- @Description Ensures that an alter table while a vacuum operation is ok
-- 
CREATE TABLE alter_while_vacuum2_pax_column (a INT, b INT);
INSERT INTO alter_while_vacuum2_pax_column SELECT i as a, i as b FROM generate_series(1, 100000) AS i;
INSERT INTO alter_while_vacuum2_pax_column SELECT i as a, i as b FROM generate_series(1, 100000) AS i;
INSERT INTO alter_while_vacuum2_pax_column SELECT i as a, i as b FROM generate_series(1, 100000) AS i;
INSERT INTO alter_while_vacuum2_pax_column SELECT i as a, i as b FROM generate_series(1, 100000) AS i;
INSERT INTO alter_while_vacuum2_pax_column SELECT i as a, i as b FROM generate_series(1, 100000) AS i;
INSERT INTO alter_while_vacuum2_pax_column SELECT i as a, i as b FROM generate_series(1, 100000) AS i;

DELETE FROM alter_while_vacuum2_pax_column WHERE a < 12000;
1: SELECT COUNT(*) FROM alter_while_vacuum2_pax_column;
2>: VACUUM alter_while_vacuum2_pax_column;
1: Alter table alter_while_vacuum2_pax_column set distributed randomly;
2<:
1: SELECT COUNT(*) FROM alter_while_vacuum2_pax_column WHERE a < 12010;
