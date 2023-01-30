-- need run 2.1_set_quota before run this test
-- FIXME add version check here

\! sleep 5

insert into s1.a select generate_series(1, 10000000); -- fail.
insert into srole.b select generate_series(1, 100000); -- fail.

insert into rolespcrole.b select generate_series(1, 100000); -- fail.
insert into spcs1.a select generate_series(1, 100000); -- fail.

drop table s1.a, srole.b, spcs1.a, rolespcrole.b;
drop schema s1, srole, spcs1, rolespcrole;
drop tablespace rolespc;
drop tablespace schemaspc;
drop role u1, rolespcu1;
