-- Given I have an append-only table
create table users(
  first_name tsvector
) with (appendonly=true);

-- And I have a large amount of data in the table
insert into users
  select to_tsvector( md5(random()::text))
  from generate_series(1, 60000) i;

insert into users values (to_tsvector('John'));

-- When I create a GIN index on users
CREATE INDEX users_search_idx ON users USING gin (first_name);

-- Orca performs seq scan in this case, so disable Orca.
set optimizer = 0;

-- Then I should be able to query the table
select * from users where first_name @@ to_tsquery('John');
explain (costs off) select * from users where first_name @@ to_tsquery('John');

drop table users;
reset optimizer;
