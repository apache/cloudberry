--
-- Test cases for COPY (INSERT/UPDATE/DELETE) TO
--
create table copydml_test (id serial, t text);
insert into copydml_test (t) values ('a');
insert into copydml_test (t) values ('b');
insert into copydml_test (t) values ('c');
insert into copydml_test (t) values ('d');
insert into copydml_test (t) values ('e');

--
-- Test COPY (insert/update/delete ...)
--
copy (insert into copydml_test (t) values ('f') returning id) to stdout;
copy (update copydml_test set t = 'g' where t = 'f' returning id) to stdout;

-- Pax not support tuple_fetch_row_version
-- copy (delete from copydml_test where t = 'g' returning id) to stdout;

--
-- Test \copy (insert/update/delete ...)
--
\copy (insert into copydml_test (t) values ('f') returning id) to stdout;
\copy (update copydml_test set t = 'g' where t = 'f' returning id) to stdout;

-- Pax not support tuple_fetch_row_version
-- \copy (delete from copydml_test where t = 'g' returning id) to stdout;

-- Error cases
copy (insert into copydml_test default values) to stdout;
copy (update copydml_test set t = 'g') to stdout;
copy (delete from copydml_test) to stdout;

create rule qqq as on insert to copydml_test do instead nothing;
copy (insert into copydml_test default values) to stdout;
drop rule qqq on copydml_test;
create rule qqq as on insert to copydml_test do also delete from copydml_test;
copy (insert into copydml_test default values) to stdout;
drop rule qqq on copydml_test;
create rule qqq as on insert to copydml_test do instead (delete from copydml_test; delete from copydml_test);
copy (insert into copydml_test default values) to stdout;
drop rule qqq on copydml_test;
create rule qqq as on insert to copydml_test where new.t <> 'f' do instead delete from copydml_test;
copy (insert into copydml_test default values) to stdout;
drop rule qqq on copydml_test;

create rule qqq as on update to copydml_test do instead nothing;
copy (update copydml_test set t = 'f') to stdout;
drop rule qqq on copydml_test;
create rule qqq as on update to copydml_test do also delete from copydml_test;
copy (update copydml_test set t = 'f') to stdout;
drop rule qqq on copydml_test;
create rule qqq as on update to copydml_test do instead (delete from copydml_test; delete from copydml_test);
copy (update copydml_test set t = 'f') to stdout;
drop rule qqq on copydml_test;
create rule qqq as on update to copydml_test where new.t <> 'f' do instead delete from copydml_test;
copy (update copydml_test set t = 'f') to stdout;
drop rule qqq on copydml_test;

create rule qqq as on delete to copydml_test do instead nothing;
copy (delete from copydml_test) to stdout;
drop rule qqq on copydml_test;
create rule qqq as on delete to copydml_test do also insert into copydml_test default values;
copy (delete from copydml_test) to stdout;
drop rule qqq on copydml_test;
create rule qqq as on delete to copydml_test do instead (insert into copydml_test default values; insert into copydml_test default values);
copy (delete from copydml_test) to stdout;
drop rule qqq on copydml_test;
create rule qqq as on delete to copydml_test where old.t <> 'f' do instead insert into copydml_test default values;
copy (delete from copydml_test) to stdout;
drop rule qqq on copydml_test;
drop table copydml_test;
