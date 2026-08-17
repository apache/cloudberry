# TRY_CONVERT

TRY_CONVERT is Greenplum/Cloudberry extension, which adds function for error-safe type cast like [TRY_CAST from SQL-Server](https://learn.microsoft.com/ru-ru/sql/t-sql/functions/try-cast-transact-sql?view=sql-server-ver16)

## Usage

```
TRY_CONVERT(SOURCE_VALUE, DEFAULT_VALUE::TARGET_TYPE) 
    returns (VALUE_IN_TARGET_TYPE or DEFAULT_VALUE)
```

```
TRY_CONVERT('42'::text, NULL::int2) -- returns 42::int2
TRY_CONVERT('42d'::text, NULL::int2) -- returns NULL::int2
TRY_CONVERT('42d'::text, 1234::int2) -- returns 1234::int2
```

### Extension's type casts

Casting from extensions types is able only for extensions:

- hstore
- citext

To enable casting from hstore and citext types use, `add_type_for_try_convert(regtype)` function

## Error handling

The cast is executed inside a `PG_TRY()` block: when the cast function reports
a failure, the error is discarded and the default value is returned instead.
Query cancellation and assertion failures are never swallowed, they are
re-thrown, the same way plpgsql handles `EXCEPTION WHEN others`.

The long-term plan is to replace the `PG_TRY()` block by the "soft" error
handling concept introduced in Postgres 17 (https://github.com/postgres/postgres/commit/ccff2d20ed9622815df2a7deffce8a7b14830965),
which lets a datatype input function report a conversion failure without
throwing. That concept was spread on data types in [21be368
Preview](https://github.com/open-gpdb/gpdb/commit/21be3688729ec4468ffd083da197721860fa2cbd) and [d31f362
](https://github.com/open-gpdb/gpdb/commit/d31f362250105e456961c2c9249693e42e67eca9) commits.
It requires converting the datatype input functions of Cloudberry first.

## Why signature is so strange?

Greenplum/Cloudberry function polymorphism accept to have polymorphic functions only one any type in signature.  

## Supported casts

    ✅ Values Cast
    ✅ Types with typemod
    ❌ Array-Array Cast
    ❌ To Domain type cast

An unsupported or non-existing cast is a query error, it is not turned into the
default value: only failures caused by the converted data are.

## Tests

The regression test is generated out of the catalog files and of the sample
values in `data/`, so it is not stored in the repository. `make installcheck`
generates it into `input/` and `output/` and then runs it:

```
make -C contrib/try_convert installcheck
```

`make -C contrib/try_convert generate-tests` generates it without running it.

## Benchmark results by pgbench


|     | without errors | with errors |
| --- | --- | --- |
| cast | 299.346 | ❌ fails |
| try_convert | 984.280 | 1004.524 |
| sql | 1384.784 | 5787.115 |
| sql execute | 5843.220 | 5898.813 |


SQL version:
```
CREATE OR REPLACE FUNCTION try_convert_into_int(_in text, d int2) RETURNS int2
  LANGUAGE plpgsql AS
$func$
    BEGIN
        RETURN CAST(_in AS int2);
        EXCEPTION WHEN others THEN
        RETURN d;
    END
$func$;
```


SQL with execute version:
```
CREATE OR REPLACE FUNCTION try_convert_by_sql(_in text, INOUT _out ANYELEMENT)
  LANGUAGE plpgsql AS
$func$
BEGIN
   EXECUTE format('SELECT %L::%s', $1, pg_typeof(_out))
   INTO  _out;
EXCEPTION WHEN others THEN
   -- do nothing: _out already carries default
END
$func$;
```

Data:
```
drop table if exists text_ints; create table text_ints (n text);
Insert into text_ints(n) select (random()*1000)::int4::text from generate_series(1,1000000);

drop table if exists text_error_ints; create table text_error_ints (n text);
Insert into text_error_ints(n) select (random()*1000000)::int8::text from generate_series(1,1000000);
```


