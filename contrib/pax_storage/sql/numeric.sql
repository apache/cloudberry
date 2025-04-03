-- create failed, cause out of range
create table pax_vec_numeric_tbl1 (v numeric) using pax with(storage_format=porc_vec);
create table pax_vec_numeric_tbl2 (v numeric(100)) using pax with(storage_format=porc_vec);
create table pax_vec_numeric_tbl3 (v numeric(36,36)) using pax with(storage_format=porc_vec);

create table pax_vec_numeric_tbl (v numeric(35,0)) using pax with(storage_format=porc_vec);
insert into pax_vec_numeric_tbl values(1);
insert into pax_vec_numeric_tbl values(1.00);
insert into pax_vec_numeric_tbl values(0.11);
select v from pax_vec_numeric_tbl;


insert into pax_vec_numeric_tbl values(repeat('1',35)::numeric);
insert into pax_vec_numeric_tbl values(repeat('9',35)::numeric);

insert into pax_vec_numeric_tbl values(-repeat('1',35)::numeric);
insert into pax_vec_numeric_tbl values(-repeat('9',35)::numeric);

insert into pax_vec_numeric_tbl values(10000000000000000000000000000000000);
insert into pax_vec_numeric_tbl values(90000000000000000000000000000000000);
select v from pax_vec_numeric_tbl;

-- failed, numeric field overflow
insert into pax_vec_numeric_tbl values(100000000000000000000000000000000000); -- 36 precision
insert into pax_vec_numeric_tbl values(repeat('9',36)::numeric);


drop table pax_vec_numeric_tbl;

create table pax_vec_numeric_tbl (v numeric(35,35)) using pax with(storage_format=porc_vec);
insert into pax_vec_numeric_tbl values(0.11111111111111111111111111111111111);
insert into pax_vec_numeric_tbl values(0.99999999999999999999999999999999999);

insert into pax_vec_numeric_tbl values(-0.11111111111111111111111111111111111);
insert into pax_vec_numeric_tbl values(-0.99999999999999999999999999999999999);

insert into pax_vec_numeric_tbl values(0.00000000000000000000000000000000001);
insert into pax_vec_numeric_tbl values(-0.00000000000000000000000000000000001);
select v from pax_vec_numeric_tbl;

-- won't failed, but will be 0
insert into pax_vec_numeric_tbl values(0.000000000000000000000000000000000001);
insert into pax_vec_numeric_tbl values(-0.000000000000000000000000000000000001);
select v from pax_vec_numeric_tbl;

-- failed, numeric field overflow
insert into pax_vec_numeric_tbl values(1.0);
insert into pax_vec_numeric_tbl values(-100000.000000000000000000000000000000000001);

drop table pax_vec_numeric_tbl;

create table pax_vec_numeric_tbl (v numeric(35,17)) using pax with(storage_format=porc_vec);

insert into pax_vec_numeric_tbl values(1234567890.0123456789);


insert into pax_vec_numeric_tbl values(999999999999999999.99999999999999999); -- 18 nweight, 17 scale
insert into pax_vec_numeric_tbl values(-999999999999999999.99999999999999999); -- negative, 18 nweight, 17 scale
insert into pax_vec_numeric_tbl values(99999999999999999.999999999999999999); -- negative, 18 nweight, 17 scale
insert into pax_vec_numeric_tbl values(999999999999999999.999999999999999990); -- 18 nweight, 18 scale with zero
insert into pax_vec_numeric_tbl values(999999999999999999.9999999999999999900000);
select v from pax_vec_numeric_tbl;

insert into pax_vec_numeric_tbl values(0.00000000000000001);  -- 17 scale
insert into pax_vec_numeric_tbl values(0.00000000000000009);  -- 17 scale
insert into pax_vec_numeric_tbl values(0.000000000000000001); -- 18 scale
insert into pax_vec_numeric_tbl values(0.000000000000000009); -- 18 scale
select v from pax_vec_numeric_tbl;

insert into pax_vec_numeric_tbl values(-0.00000000000000001);  -- negative, 17 scale
insert into pax_vec_numeric_tbl values(-0.00000000000000009);  -- negative, 17 scale
insert into pax_vec_numeric_tbl values(-0.000000000000000001); -- negative, 18 scale, last scale < 5
insert into pax_vec_numeric_tbl values(-0.000000000000000009); -- negative, 18 scale, last scale > 5
select v from pax_vec_numeric_tbl;


truncate pax_vec_numeric_tbl;
insert into pax_vec_numeric_tbl values('NaN');
select v from pax_vec_numeric_tbl;

-- failed, numeric field overflow
insert into pax_vec_numeric_tbl values('+Inf');
insert into pax_vec_numeric_tbl values('Inf');
insert into pax_vec_numeric_tbl values('-Inf');

insert into pax_vec_numeric_tbl values(999999999999999999.999999999999999999); -- 18 nweight, 18 scale, last scale > 5
insert into pax_vec_numeric_tbl values(9999999999999999999.9999999999999999);  -- 19 nweight, 17 scale

-- test with null datum
truncate pax_vec_numeric_tbl;
insert into pax_vec_numeric_tbl values(1), (1.00), (2.22), (NULL), (NULL);
insert into pax_vec_numeric_tbl values(NULL), (NULL);
insert into pax_vec_numeric_tbl values(NULL), (3.33), (44.44);
insert into pax_vec_numeric_tbl values(6.667), (NULL), (77.77), (NULL), (88.888);
select v from pax_vec_numeric_tbl;

drop table pax_vec_numeric_tbl;


-- without option `storage_format=porc_vec`, pax should store numeric as non fixed column

create table pax_numeric_tbl1 (v numeric) using pax with(storage_format=porc);
insert into pax_numeric_tbl1 values(repeat('1',100)::numeric);
insert into pax_numeric_tbl1 values(0.9999999999999999999999999999999999999999999999999999); -- 52 scale

select v from pax_numeric_tbl1;

create table pax_numeric_tbl2 (v numeric) using pax;
insert into pax_numeric_tbl2 select v from pax_numeric_tbl1;
select v from pax_numeric_tbl2;

drop table pax_numeric_tbl1;
drop table pax_numeric_tbl2;


-- without option `storage_format=porc_vec`， should pass the same test cases

create table pax_numeric_tbl (v numeric(35,0)) using pax with(storage_format=porc);
insert into pax_numeric_tbl values(1);
insert into pax_numeric_tbl values(1.00);
insert into pax_numeric_tbl values(0.11);
select v from pax_numeric_tbl;


insert into pax_numeric_tbl values(repeat('1',35)::numeric);
insert into pax_numeric_tbl values(repeat('9',35)::numeric);

insert into pax_numeric_tbl values(-repeat('1',35)::numeric);
insert into pax_numeric_tbl values(-repeat('9',35)::numeric);

insert into pax_numeric_tbl values(10000000000000000000000000000000000);
insert into pax_numeric_tbl values(90000000000000000000000000000000000);
select v from pax_numeric_tbl;

-- failed, numeric field overflow
insert into pax_numeric_tbl values(100000000000000000000000000000000000); -- 36 precision
insert into pax_numeric_tbl values(repeat('9',36)::numeric);

drop table pax_numeric_tbl;

create table pax_numeric_tbl (v numeric(35,35)) using pax with(storage_format=porc);
insert into pax_numeric_tbl values(0.11111111111111111111111111111111111);
insert into pax_numeric_tbl values(0.99999999999999999999999999999999999);

insert into pax_numeric_tbl values(-0.11111111111111111111111111111111111);
insert into pax_numeric_tbl values(-0.99999999999999999999999999999999999);

insert into pax_numeric_tbl values(0.00000000000000000000000000000000001);
insert into pax_numeric_tbl values(-0.00000000000000000000000000000000001);
select v from pax_numeric_tbl;

-- won't failed, but will be 0
insert into pax_numeric_tbl values(0.000000000000000000000000000000000001);
insert into pax_numeric_tbl values(-0.000000000000000000000000000000000001);
select v from pax_numeric_tbl;

insert into pax_numeric_tbl values(1.0);
insert into pax_numeric_tbl values(-100000.000000000000000000000000000000000001);

drop table pax_numeric_tbl;

create table pax_numeric_tbl (v numeric(35,17)) using pax with(storage_format=porc);

insert into pax_numeric_tbl values(1234567890.0123456789);


insert into pax_numeric_tbl values(999999999999999999.99999999999999999); -- 18 nweight, 17 scale
insert into pax_numeric_tbl values(-999999999999999999.99999999999999999); -- negative, 18 nweight, 17 scale
insert into pax_numeric_tbl values(99999999999999999.999999999999999999); -- negative, 18 nweight, 17 scale
insert into pax_numeric_tbl values(999999999999999999.999999999999999990); -- 18 nweight, 18 scale with zero
insert into pax_numeric_tbl values(999999999999999999.9999999999999999900000);
select v from pax_numeric_tbl;

insert into pax_numeric_tbl values(0.00000000000000001);  -- 17 scale
insert into pax_numeric_tbl values(0.00000000000000009);  -- 17 scale
insert into pax_numeric_tbl values(0.000000000000000001); -- 18 scale
insert into pax_numeric_tbl values(0.000000000000000009); -- 18 scale
select v from pax_numeric_tbl;

insert into pax_numeric_tbl values(-0.00000000000000001);  -- negative, 17 scale
insert into pax_numeric_tbl values(-0.00000000000000009);  -- negative, 17 scale
insert into pax_numeric_tbl values(-0.000000000000000001); -- negative, 18 scale, last scale < 5
insert into pax_numeric_tbl values(-0.000000000000000009); -- negative, 18 scale, last scale > 5
select v from pax_numeric_tbl;


truncate pax_numeric_tbl;
insert into pax_numeric_tbl values('NaN');
select v from pax_numeric_tbl;

-- failed, numeric field overflow
insert into pax_numeric_tbl values('+Inf');
insert into pax_numeric_tbl values('Inf');
insert into pax_numeric_tbl values('-Inf');

insert into pax_numeric_tbl values(999999999999999999.999999999999999999); -- 18 nweight, 18 scale, last scale > 5
insert into pax_numeric_tbl values(9999999999999999999.9999999999999999);  -- 19 nweight, 17 scale

-- test with null datum
truncate pax_numeric_tbl;
insert into pax_numeric_tbl values(1), (1.00), (2.22), (NULL), (NULL);
insert into pax_numeric_tbl values(NULL), (NULL);
insert into pax_numeric_tbl values(NULL), (3.33), (44.44);
insert into pax_numeric_tbl values(6.667), (NULL), (77.77), (NULL), (88.888);
select v from pax_numeric_tbl;

drop table pax_numeric_tbl;
