-- to make sure that the schema 'notfoundns' is really not found
select nspname from pg_namespace where nspname = 'notfoundns';
select diskquota.set_schema_quota('notfoundns', '1 MB');
