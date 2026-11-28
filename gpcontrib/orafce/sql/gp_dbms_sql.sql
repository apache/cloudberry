-- A portal opened in a subtransaction is dropped when the subtransaction
-- aborts. The cursor must not use it afterwards.
DO $$
DECLARE c int; v int; n int;
BEGIN
  c := dbms_sql.open_cursor();
  CALL dbms_sql.parse(c, 'SELECT i FROM generate_series(1, 3) g(i)');
  CALL dbms_sql.define_column(c, 1, v);
  BEGIN
    PERFORM dbms_sql.execute(c);
    RAISE EXCEPTION 'roll back the subtransaction';
  EXCEPTION WHEN raise_exception THEN
    NULL;
  END;
  BEGIN
    n := dbms_sql.fetch_rows(c);
  EXCEPTION WHEN others THEN
    RAISE NOTICE 'fetch_rows: %', sqlerrm;
  END;
  -- the cursor can be executed again
  PERFORM dbms_sql.execute(c);
  WHILE dbms_sql.fetch_rows(c) > 0 LOOP
    CALL dbms_sql.column_value(c, 1, v);
    RAISE NOTICE 'v = %', v;
  END LOOP;
  CALL dbms_sql.close_cursor(c);
END;
$$;

-- close_cursor() after the portal is gone
DO $$
DECLARE c int; v int;
BEGIN
  c := dbms_sql.open_cursor();
  CALL dbms_sql.parse(c, 'SELECT 1');
  CALL dbms_sql.define_column(c, 1, v);
  BEGIN
    PERFORM dbms_sql.execute(c);
    RAISE EXCEPTION 'roll back the subtransaction';
  EXCEPTION WHEN raise_exception THEN
    NULL;
  END;
  CALL dbms_sql.close_cursor(c);
  RAISE NOTICE 'closed';
END;
$$;
