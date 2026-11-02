--
-- Btree deduplication on append-optimized tables.
--
-- Bottom-up index deletion isn't supported on these tables, but
-- deduplication is, and neither must get in the way of inserting into a
-- full leaf page.
--
CREATE FUNCTION ao_btree_dedup_test(am text) RETURNS TABLE (check_name text, ok boolean)
LANGUAGE plpgsql AS $$
BEGIN
    EXECUTE format('CREATE TABLE dedup_t (a int, b int) USING %I DISTRIBUTED BY (b)', am);
    CREATE INDEX dedup_t_on ON dedup_t (a) WITH (deduplicate_items = on);
    CREATE INDEX dedup_t_off ON dedup_t (a) WITH (deduplicate_items = off);

    -- Fill the indexes with duplicates, after they have been built.
    INSERT INTO dedup_t SELECT i % 10, i FROM generate_series(1, 60000) i;
    check_name := 'deduplication makes the index smaller';
    ok := pg_relation_size('dedup_t_on') * 2 < pg_relation_size('dedup_t_off');
    RETURN NEXT;

    -- Posting list tuples must return every TID, also after a delete and a
    -- vacuum-free insert into a posting list.
    SET LOCAL enable_seqscan = off;
    SET LOCAL optimizer_enable_tablescan = off;
    DELETE FROM dedup_t WHERE a = 3 AND b % 2 = 1;
    INSERT INTO dedup_t SELECT 3, i FROM generate_series(1, 1000) i;
    check_name := 'index returns the right rows';
    ok := (SELECT count(*) FROM dedup_t WHERE a = 3) =
          (SELECT count(*) FROM dedup_t WHERE a + 0 = 3);
    RETURN NEXT;

    DROP TABLE dedup_t;

    -- An UPDATE that doesn't change the indexed column passes the
    -- indexUnchanged hint, which asks for bottom-up deletion on a full page.
    EXECUTE format('CREATE TABLE dedup_t (a int, b int) USING %I DISTRIBUTED BY (a)', am);
    CREATE INDEX dedup_t_a ON dedup_t (a);
    INSERT INTO dedup_t SELECT 1, i FROM generate_series(1, 2000) i;
    FOR i IN 1..10 LOOP
        UPDATE dedup_t SET b = b + 1;
    END LOOP;
    check_name := 'update with an unchanged index key';
    ok := (SELECT count(*) FROM dedup_t WHERE a = 1) = 2000;
    RETURN NEXT;
    DROP TABLE dedup_t;

    -- Churn on a unique index also asks for it, as it finds physical
    -- duplicates of the key.
    EXECUTE format('CREATE TABLE dedup_t (a int) USING %I DISTRIBUTED BY (a)', am);
    CREATE UNIQUE INDEX dedup_t_u ON dedup_t (a);
    FOR i IN 1..500 LOOP
        DELETE FROM dedup_t WHERE a = 1;
        INSERT INTO dedup_t VALUES (1);
    END LOOP;
    check_name := 'delete and reinsert on a unique index';
    ok := (SELECT count(*) FROM dedup_t WHERE a = 1) = 1;
    RETURN NEXT;

    -- The unique check must still see through posting lists.
    BEGIN
        INSERT INTO dedup_t VALUES (1);
        ok := false;
    EXCEPTION WHEN unique_violation THEN
        ok := true;
    END;
    check_name := 'unique violation is detected';
    RETURN NEXT;
    DROP TABLE dedup_t;
END $$;

SELECT * FROM ao_btree_dedup_test('ao_row');
SELECT * FROM ao_btree_dedup_test('ao_column');

DROP FUNCTION ao_btree_dedup_test(text);
