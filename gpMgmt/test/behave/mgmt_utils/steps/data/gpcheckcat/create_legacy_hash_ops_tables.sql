set gp_use_legacy_hashops = 1;

create table t_old(a int, b int, c int) distributed by (a, b);
create table t1_old(a int, b int, c int) distributed by (a, b);
create table t_replicate_old(a int , b int) distributed replicated;
create table t_random_old(a int , b int) distributed randomly;

CREATE TABLE rank_old (id int, rank int, year int, gender
        char(1), count int)
DISTRIBUTED BY (id)
PARTITION BY RANGE (year)
( START (2006) END (2016) EVERY (1),
          DEFAULT PARTITION extra );


CREATE OR REPLACE FUNCTION random_between(low INT ,high INT)
   RETURNS INT AS
$$
BEGIN
           RETURN floor(random()* (high-low + 1) + low);
END;
$$ language 'plpgsql' STRICT;

insert into rank_old
select i, i, random_between(2005, 2017), 'g', i
from generate_series(1, 100000)i;

-- some special characters in column names
create table t_space("a col" int);
create table t_dot("a.col" int);
create table t_dash("a-col" int);
create table t_multispecial("a col" int, "b.col" int, "c-col" int) distributed by ("a col", "b.col", "c-col");

