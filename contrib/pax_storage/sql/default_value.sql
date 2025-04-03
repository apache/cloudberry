--
create table t_default_value1(a int, i int default 11,
    b bool default 't', 
    bp char(17) default '  _X_  ',
    s text default '  _T_  ',
    n numeric(35, 15)) using pax with(storage_format=porc);

insert into t_default_value1
select 1,
case when i%2=0 or i%3=0 then i else null end case, -- i
case when i%3=0 then 't'::bool
     when i%3=1 then 'f'::bool
     else null
     end case, -- b
case when i%4=0 then ''
     when i%4=1 then '  abc  '
     else null
     end case, -- bp
case when i%3=0 then ' ksdf '
     when i%3=1 then ''
     else null
     end case, -- s
case when i%3=0 then null
     when i%3=1 then 3.141592653
     else 11.11
     end case -- n
from generate_series(1,40)i;

explain select *, '|' || bp || '|' from t_default_value1;
select *, '|' || bp || '|' from t_default_value1;

alter table t_default_value1 add b2 bool default 'f';
alter table t_default_value1 add bp2 char(16) default '  _PAD_  ';
alter table t_default_value1 add s2 text default '  _DEF_TEXT_  ';
alter table t_default_value1 add n2 numeric(22, 10) default 6.283185307179586;


explain select *, '|' || bp2 || '|' from t_default_value1;
select *, '|' || bp2 || '|' from t_default_value1;

-- precision > 35 will fallback to pg executor
alter table t_default_value1 add n3 numeric(36, 10) default 1.234;
explain select *, '|' || bp2 || '|' from t_default_value1;
select *, '|' || bp2 || '|' from t_default_value1;

explain select *, '|' || bp2 || '|' from t_default_value1;
select *, '|' || bp2 || '|' from t_default_value1;

drop table t_default_value1;

-- tests for non-vec storage
create table t_default_value1(a int, i int default 11,
    b bool default 't', 
    bp char(17) default '  _X_  ',
    s text default '  _T_  ',
    n numeric(35, 15)) using pax with(storage_format=porc_vec);

insert into t_default_value1
select 1,
case when i%2=0 or i%3=0 then i else null end case, -- i
case when i%3=0 then 't'::bool
     when i%3=1 then 'f'::bool
     else null
     end case, -- b
case when i%4=0 then ''
     when i%4=1 then '  abc  '
     else null
     end case, -- bp
case when i%3=0 then ' ksdf '
     when i%3=1 then ''
     else null
     end case, -- s
case when i%3=0 then null
     when i%3=1 then 3.141592653
     else 11.11
     end case -- n
from generate_series(1,40)i;

explain select *, '|' || bp || '|' from t_default_value1;
select *, '|' || bp || '|' from t_default_value1;

alter table t_default_value1 add b2 bool default 'f';
alter table t_default_value1 add bp2 char(16) default '  _PAD_  ';
alter table t_default_value1 add s2 text default '  _DEF_TEXT_  ';
alter table t_default_value1 add n2 numeric(22, 10) default 6.283185307179586;

explain select *, '|' || bp2 || '|' from t_default_value1;
select *, '|' || bp2 || '|' from t_default_value1;

-- fail: porc_vec disallow to use numeric with precision > 35
alter table t_default_value1 add n3 numeric(36, 10) default 1.234;

explain select *, '|' || bp2 || '|' from t_default_value1;
select *, '|' || bp2 || '|' from t_default_value1;

drop table t_default_value1;
