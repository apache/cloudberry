-- __list_pipes() returns SETOF record; called without a column definition
-- list it must raise an error, not crash the backend.
SELECT dbms_pipe.__list_pipes();
SELECT count(*) >= 0 AS ok FROM dbms_pipe.db_pipes;
