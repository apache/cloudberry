set TimeZone to 'US/Pacific';

-- Tests for Time Series features of 5.0+
-- Some of these tests could live elsewhere, but it seems good to collect them.

-- Interval-Interval division and remainder and associated overloads

select
    interval_interval_div(interval '365 days', interval '1 month') as "12 1/6",
    interval '365 days'/interval '1 month' as "12 1/6",
    interval_interval_mod(interval '365 days', interval '1 month') as "5*24",
    interval '365 days'%interval '1 month' as "5*24";

select
    interval_interval_div(interval '1 day', interval '8 hours') as "3",
    interval '1 day'/interval '8 hours' as "3",
    interval_interval_mod(interval '1 day', interval '8 hours') as "0",
    interval '1 day'%interval '8 hours' as "0";

select
    interval_interval_div(interval '2 hours', interval '-100 minutes') as "-1 1/5",
    interval '2 hours'/interval '-100 minutes' as "-1 1/5",
    interval_interval_mod(interval '2 hours', interval '-100 minutes') as "20",
    interval '2 hours'%interval '-100 minutes' as "20";

-- generate_series with timestamp

select *
from generate_series(
	timestamp '2011-01-01 12:00:00',
	timestamp '2011-01-01 12:00:00',
	interval '1 year');

select *
from generate_series(
	timestamp '2011-01-01 12:00:00',
	timestamp '2012-01-01 12:00:00',
	interval '1 month');

select *
from generate_series(
	timestamp '2012-01-01 12:00:00',
	timestamp '2013-01-01 12:00:00',
	interval '1 month');

select *
from generate_series(
	timestamp '2011-01-01 12:00:00',
	timestamp '2011-01-31 12:00:00',
	interval '2 weeks');

select *
from generate_series(
	timestamp '2013-01-01 12:00:00',
	timestamp '2011-01-01 12:00:00',
	interval '-2 months');

select *
from generate_series(
	'2011-01-01 12:00:00',
	'2012-01-01 12:00:00',
	interval '0 sec');

-- generate_series with timestamptz

select *
from generate_series(
	timestamptz '2011-01-01 12:00:00 UTC',
	timestamptz '2011-01-01 12:00:00 UTC',
	interval '1 year');

select *
from generate_series(
	timestamptz '2011-01-01 12:00:00 UTC',
	timestamptz '2012-01-01 12:00:00 UTC',
	interval '1 month');

select *
from generate_series(
	timestamptz '2012-01-01 12:00:00 UTC',
	timestamptz '2013-01-01 12:00:00 UTC',
	interval '1 month');

select *
from generate_series(
	timestamptz '2011-01-01 12:00:00 UTC',
	timestamptz '2011-01-31 12:00:00 UTC',
	interval '2 weeks');

select *
from generate_series(
	timestamptz '2013-01-01 12:00:00 UTC',
	timestamptz '2011-01-01 12:00:00 UTC',
	interval '-2 months');

select *
from generate_series(
	'2011-01-01 12:00:00 UTC',
	'2012-01-01 12:00:00 UTC',
	interval '0 sec');

-- interval_bound with numeric

select interval_bound(0, 0);
select interval_bound(0, -1);

select v, w, r, s,
    interval_bound(v, w) as "normal",
    interval_bound(v, w, s) as "shifted s",
    interval_bound(v, w, s, r) as "and registered to r"
from ( values
        (10, 1, 0.5, 4),
        (10, 0.5, -100, null),
        (0.5, 10, -1, -1),
        (-100, 100, 10, 1),
        (-101, 10, null, 10),
        (5, 2, -100.5, 1),
        (null, 10, 0, 0),
        (55, null, 20, 0),
        ('NaN', 10, 10, 0),
        (45.6, 'NaN', 5.5, 2),
        (31, 10, 'NaN', 0)
    ) r(v,w,r,s);

-- interval_bound with timestamp

select interval_bound(timestamp 'now', interval '0 hours');
select interval_bound(timestamp 'now', interval '1 day - 24 hours');
select interval_bound(timestamp 'infinity', interval '1 month');
select interval_bound(timestamp '-infinity', interval '1 month');
select interval_bound(timestamp 'now', interval '2 hours', null, timestamp 'infinity');

select v, w, r, s,
    interval_bound(v, w) as "normal",
    interval_bound(v, w, s) as "shifted s",
    interval_bound(v, w, s, r) as "and registered to r"
from ( values
        (timestamp '2012-01-12 10:00:10', interval '1 week', timestamp '2012-04-02 00:00:00', 4),
        (timestamp '1929-10-29 22:33:44.55', interval '1 year', timestamp '1991-01-17 02:34:56.78', null),
        (timestamp '1991-01-17 02:34:56.78', interval '1 year -1 month', timestamp '1776-07-04 12:34:56', -1),
        (timestamp '2100-03-01 11:11:11.11', interval '100 days', timestamp '1929-10-29 22:33:44.55', 1),
        (timestamp '1776-07-04 12:34:56', interval '1 month', timestamp '2012-04-01 00:00:00', 1),
        (null::timestamp, interval '1 week', timestamp '1911-09-09 15:16:17', 3),
        (timestamp '1999-10-30 13:01:01', null::interval, timestamp '1970-04-05 12:00:00', 1),
        (timestamp '1999-10-30 13:01:01', interval '1 day', null, 1)
    ) r(v,w,r,s);

-- interval_bound with timestamptz

select interval_bound(timestamptz 'now', interval '0 hours');
select interval_bound(timestamptz 'now', interval '1 day - 24 hours');
select interval_bound(timestamptz 'infinity', interval '1 month');
select interval_bound(timestamptz '-infinity', interval '1 month');
select interval_bound(timestamptz 'now', interval '2 hours', null, timestamptz 'infinity');

select v, w, r, s,
    interval_bound(v, w) as "normal",
    interval_bound(v, w, s) as "shifted s",
    interval_bound(v, w, s, r) as "and registered to r"
from ( values
        (timestamptz '2012-01-12 10:00:10', interval '1 week', timestamptz '2012-04-02 00:00:00 US/Eastern', 4),
        (timestamptz '1929-10-29 22:33:44.55', interval '1 year', timestamptz '1991-01-17 02:34:56.78', null),
        (timestamptz '1991-01-17 02:34:56.78', interval '1 year -1 month', timestamptz '1776-07-04 12:34:56 US/Eastern', -1),
        (timestamptz '2100-03-01 11:11:11.11 UTC', interval '100 days', timestamptz '1929-10-29 22:33:44.55', 1),
        (timestamptz '1776-07-04 12:34:56', interval '1 month', timestamptz '2012-04-01 00:00:00 UTC', 1),
        (null::timestamp, interval '1 week', timestamp '1911-09-09 15:16:17', 3),
        (timestamptz '1999-10-30 13:01:01', null::interval, timestamptz '1970-04-05 12:00:00', 1),
        (timestamptz '1999-10-30 13:01:01', interval '1 day', null, 1)
    ) r(v,w,r,s);

-- Test limits
select linear_interpolate(3, 0, 0::bigint, 1, (2^62)::bigint);
select linear_interpolate(3, 0, 0::bigint, 1, (-2^62)::bigint);
select linear_interpolate(3, 0, 0::integer, 1, (2^30)::integer);
select linear_interpolate(3, 0, 0::integer, 1, (-2^30)::integer);
select linear_interpolate(3, 0, 0::smallint, 1, (2^14)::smallint);
select linear_interpolate(3, 0, 0::smallint, 1, (-2^14)::smallint);
select linear_interpolate(null, 0, 0::smallint, 1, (-2^14)::smallint);
select linear_interpolate(null::bigint, null::bigint, null::bigint, null::bigint, null::bigint);
select linear_interpolate(200::float8, 100::float8, 'infinity'::float8, 100::float8, '-infinity'::float8);
select linear_interpolate(200::float8, 'nan'::float8, 'infinity'::float8, 'nan'::float8, '-infinity'::float8);

-- A "generic" unsupported type.

select linear_interpolate('x'::text, 'x0'::text, 0, 'x1'::text, 1000);
select linear_interpolate(5, 0, 'y0'::text, 100, 'y1'::text);


-- Check that  "divide by zero" returns null
select linear_interpolate('2010-01-03T02:00:00'::timestamp, '2010-01-02T09:20:00'::timestamp, 2000::int4, '2010-01-02T09:20:00'::timestamp, 2250::int4);
select linear_interpolate(200::numeric, 100::numeric, 2000::int4, 100::numeric, 2250::int4);
select linear_interpolate(200::int4, 100::int4, 2000::int4, 100::int4, 2250::int4);
select linear_interpolate(200::float8, 100::float8, 2000::int4, 100::float8, 2250::int4);
select linear_interpolate(200::int2, 100::int2, 2000::int4, 100::int2, 2250::int4);
select linear_interpolate('2001-01-21'::date, '2001-01-11'::date, 2000::int4, '2001-01-11'::date, 2250::int4);
select linear_interpolate(200::int8, 100::int8, 2000::int4, 100::int8, 2250::int4);
select linear_interpolate('85 minutes'::interval, '65 minutes'::interval, 2000::int4, '65 minutes'::interval, 2250::int4);
select linear_interpolate('2010-01-03T02:00:00'::timestamptz, '2010-01-02T09:20:00'::timestamptz, 2000::int4, '2010-01-02T09:20:00'::timestamptz, 2250::int4);
select linear_interpolate('10:25:00'::time, '10:20:00'::time, 2000::int4, '10:20:00'::time, 2250::int4);
select linear_interpolate(200::float4, 100::float4, 2000::int4, 100::float4, 2250::int4);
select linear_interpolate(200::int8, 100::int8, 2000::int8, 100::int8, 2250::int8);
select linear_interpolate(200::int2, 100::int2, 300::int2, 100::int2, 400::int2);
select linear_interpolate(200::float8, 100::float8, 2000::float8, 100::float8, 2250::float8);
select linear_interpolate('2001-01-21'::date, '2001-01-11'::date, '2001-01-25'::date, '2001-01-11'::date, '2001-01-26'::date);
select linear_interpolate('2010-01-03T02:00:00'::timestamp, '2010-01-02T09:20:00'::timestamp, '2010-01-02T06:20:00'::timestamp, '2010-01-02T09:20:00'::timestamp, '2010-01-02T08:20:00'::timestamp);
select linear_interpolate('2010-01-03T02:00:00'::timestamptz, '2010-01-02T09:20:00'::timestamptz, '2010-01-02T05:20:00'::timestamptz, '2010-01-02T09:20:00'::timestamptz, '2010-01-02T08:20:00'::timestamptz);
select linear_interpolate('85 minutes'::interval, '65 minutes'::interval, '75 minutes'::interval, '65 minutes'::interval, '95 minutes'::interval);
select linear_interpolate('10:25:00'::time, '10:20:00'::time, '11:20:00'::time, '10:20:00'::time, '09:20:00'::time);
select linear_interpolate(200::numeric, 100::numeric, 2000::numeric, 100::numeric, 2250::numeric);
select linear_interpolate(200::float4, 100::float4, 2000::float4, 100::float4, 2250::float4);

-- Abscissa: timestamp, Ordinate: int4
-- Check correctness - all results should have match = t

select 
    linear_interpolate('2010-01-03T02:00:00'::timestamp, '2010-01-02T09:20:00'::timestamp, 2000::int4, '2010-01-05T20:40:00'::timestamp, 2250::int4),
     2050::int4 as answer,
     2050::int4 = linear_interpolate('2010-01-03T02:00:00'::timestamp, '2010-01-02T09:20:00'::timestamp, 2000::int4, '2010-01-05T20:40:00'::timestamp, 2250::int4) as match ;

select 
    linear_interpolate('2010-01-03T02:00:00'::timestamp, '2010-01-05T20:40:00'::timestamp, 2250::int4, '2010-01-02T09:20:00'::timestamp, 2000::int4),
     2050::int4 as answer,
     2050::int4 = linear_interpolate('2010-01-03T02:00:00'::timestamp, '2010-01-05T20:40:00'::timestamp, 2250::int4, '2010-01-02T09:20:00'::timestamp, 2000::int4) as match ;

select 
    linear_interpolate('2010-01-02T09:20:00'::timestamp, '2010-01-03T02:00:00'::timestamp, 2050::int4, '2010-01-05T20:40:00'::timestamp, 2250::int4),
     2000::int4 as answer,
     2000::int4 = linear_interpolate('2010-01-02T09:20:00'::timestamp, '2010-01-03T02:00:00'::timestamp, 2050::int4, '2010-01-05T20:40:00'::timestamp, 2250::int4) as match ;

select 
    linear_interpolate('2010-01-02T09:20:00'::timestamp, '2010-01-05T20:40:00'::timestamp, 2250::int4, '2010-01-03T02:00:00'::timestamp, 2050::int4),
     2000::int4 as answer,
     2000::int4 = linear_interpolate('2010-01-02T09:20:00'::timestamp, '2010-01-05T20:40:00'::timestamp, 2250::int4, '2010-01-03T02:00:00'::timestamp, 2050::int4) as match ;

select 
    linear_interpolate('2010-01-05T20:40:00'::timestamp, '2010-01-03T02:00:00'::timestamp, 2050::int4, '2010-01-02T09:20:00'::timestamp, 2000::int4),
     2250::int4 as answer,
     2250::int4 = linear_interpolate('2010-01-05T20:40:00'::timestamp, '2010-01-03T02:00:00'::timestamp, 2050::int4, '2010-01-02T09:20:00'::timestamp, 2000::int4) as match ;

select 
    linear_interpolate('2010-01-05T20:40:00'::timestamp, '2010-01-02T09:20:00'::timestamp, 2000::int4, '2010-01-03T02:00:00'::timestamp, 2050::int4),
     2250::int4 as answer,
     2250::int4 = linear_interpolate('2010-01-05T20:40:00'::timestamp, '2010-01-02T09:20:00'::timestamp, 2000::int4, '2010-01-03T02:00:00'::timestamp, 2050::int4) as match ;

select 
    linear_interpolate('2010-01-03T02:00:00'::timestamp, '2010-01-03T02:00:00'::timestamp, 2050::int4, '2010-01-03T02:00:00'::timestamp, 2050::int4),
     2050::int4 as answer,
     2050::int4 = linear_interpolate('2010-01-03T02:00:00'::timestamp, '2010-01-03T02:00:00'::timestamp, 2050::int4, '2010-01-03T02:00:00'::timestamp, 2050::int4) as match ;

-- Abscissa: numeric, Ordinate: int4
-- Check correctness - all results should have match = t

select 
    linear_interpolate(200::numeric, 100::numeric, 2000::int4, 600::numeric, 2250::int4),
     2050::int4 as answer,
     2050::int4 = linear_interpolate(200::numeric, 100::numeric, 2000::int4, 600::numeric, 2250::int4) as match ;

select 
    linear_interpolate(200::numeric, 600::numeric, 2250::int4, 100::numeric, 2000::int4),
     2050::int4 as answer,
     2050::int4 = linear_interpolate(200::numeric, 600::numeric, 2250::int4, 100::numeric, 2000::int4) as match ;

select 
    linear_interpolate(100::numeric, 200::numeric, 2050::int4, 600::numeric, 2250::int4),
     2000::int4 as answer,
     2000::int4 = linear_interpolate(100::numeric, 200::numeric, 2050::int4, 600::numeric, 2250::int4) as match ;

select 
    linear_interpolate(100::numeric, 600::numeric, 2250::int4, 200::numeric, 2050::int4),
     2000::int4 as answer,
     2000::int4 = linear_interpolate(100::numeric, 600::numeric, 2250::int4, 200::numeric, 2050::int4) as match ;

select 
    linear_interpolate(600::numeric, 200::numeric, 2050::int4, 100::numeric, 2000::int4),
     2250::int4 as answer,
     2250::int4 = linear_interpolate(600::numeric, 200::numeric, 2050::int4, 100::numeric, 2000::int4) as match ;

select 
    linear_interpolate(600::numeric, 100::numeric, 2000::int4, 200::numeric, 2050::int4),
     2250::int4 as answer,
     2250::int4 = linear_interpolate(600::numeric, 100::numeric, 2000::int4, 200::numeric, 2050::int4) as match ;

select 
    linear_interpolate(200::numeric, 200::numeric, 2050::int4, 200::numeric, 2050::int4),
     2050::int4 as answer,
     2050::int4 = linear_interpolate(200::numeric, 200::numeric, 2050::int4, 200::numeric, 2050::int4) as match ;

-- Abscissa: int4, Ordinate: int4
-- Check correctness - all results should have match = t

select 
    linear_interpolate(200::int4, 100::int4, 2000::int4, 600::int4, 2250::int4),
     2050::int4 as answer,
     2050::int4 = linear_interpolate(200::int4, 100::int4, 2000::int4, 600::int4, 2250::int4) as match ;

select 
    linear_interpolate(200::int4, 600::int4, 2250::int4, 100::int4, 2000::int4),
     2050::int4 as answer,
     2050::int4 = linear_interpolate(200::int4, 600::int4, 2250::int4, 100::int4, 2000::int4) as match ;

select 
    linear_interpolate(100::int4, 200::int4, 2050::int4, 600::int4, 2250::int4),
     2000::int4 as answer,
     2000::int4 = linear_interpolate(100::int4, 200::int4, 2050::int4, 600::int4, 2250::int4) as match ;

select 
    linear_interpolate(100::int4, 600::int4, 2250::int4, 200::int4, 2050::int4),
     2000::int4 as answer,
     2000::int4 = linear_interpolate(100::int4, 600::int4, 2250::int4, 200::int4, 2050::int4) as match ;

select 
    linear_interpolate(600::int4, 200::int4, 2050::int4, 100::int4, 2000::int4),
     2250::int4 as answer,
     2250::int4 = linear_interpolate(600::int4, 200::int4, 2050::int4, 100::int4, 2000::int4) as match ;

select 
    linear_interpolate(600::int4, 100::int4, 2000::int4, 200::int4, 2050::int4),
     2250::int4 as answer,
     2250::int4 = linear_interpolate(600::int4, 100::int4, 2000::int4, 200::int4, 2050::int4) as match ;

select 
    linear_interpolate(200::int4, 200::int4, 2050::int4, 200::int4, 2050::int4),
     2050::int4 as answer,
     2050::int4 = linear_interpolate(200::int4, 200::int4, 2050::int4, 200::int4, 2050::int4) as match ;

-- Abscissa: float8, Ordinate: int4
-- Check correctness - all results should have match = t

select 
    linear_interpolate(200::float8, 100::float8, 2000::int4, 600::float8, 2250::int4),
     2050::int4 as answer,
     2050::int4 = linear_interpolate(200::float8, 100::float8, 2000::int4, 600::float8, 2250::int4) as match ;

select 
    linear_interpolate(200::float8, 600::float8, 2250::int4, 100::float8, 2000::int4),
     2050::int4 as answer,
     2050::int4 = linear_interpolate(200::float8, 600::float8, 2250::int4, 100::float8, 2000::int4) as match ;

select 
    linear_interpolate(100::float8, 200::float8, 2050::int4, 600::float8, 2250::int4),
     2000::int4 as answer,
     2000::int4 = linear_interpolate(100::float8, 200::float8, 2050::int4, 600::float8, 2250::int4) as match ;

select 
    linear_interpolate(100::float8, 600::float8, 2250::int4, 200::float8, 2050::int4),
     2000::int4 as answer,
     2000::int4 = linear_interpolate(100::float8, 600::float8, 2250::int4, 200::float8, 2050::int4) as match ;

select 
    linear_interpolate(600::float8, 200::float8, 2050::int4, 100::float8, 2000::int4),
     2250::int4 as answer,
     2250::int4 = linear_interpolate(600::float8, 200::float8, 2050::int4, 100::float8, 2000::int4) as match ;

select 
    linear_interpolate(600::float8, 100::float8, 2000::int4, 200::float8, 2050::int4),
     2250::int4 as answer,
     2250::int4 = linear_interpolate(600::float8, 100::float8, 2000::int4, 200::float8, 2050::int4) as match ;

select 
    linear_interpolate(200::float8, 200::float8, 2050::int4, 200::float8, 2050::int4),
     2050::int4 as answer,
     2050::int4 = linear_interpolate(200::float8, 200::float8, 2050::int4, 200::float8, 2050::int4) as match ;

-- Abscissa: int2, Ordinate: int4
-- Check correctness - all results should have match = t

select 
    linear_interpolate(200::int2, 100::int2, 2000::int4, 600::int2, 2250::int4),
     2050::int4 as answer,
     2050::int4 = linear_interpolate(200::int2, 100::int2, 2000::int4, 600::int2, 2250::int4) as match ;

select 
    linear_interpolate(200::int2, 600::int2, 2250::int4, 100::int2, 2000::int4),
     2050::int4 as answer,
     2050::int4 = linear_interpolate(200::int2, 600::int2, 2250::int4, 100::int2, 2000::int4) as match ;

select 
    linear_interpolate(100::int2, 200::int2, 2050::int4, 600::int2, 2250::int4),
     2000::int4 as answer,
     2000::int4 = linear_interpolate(100::int2, 200::int2, 2050::int4, 600::int2, 2250::int4) as match ;

select 
    linear_interpolate(100::int2, 600::int2, 2250::int4, 200::int2, 2050::int4),
     2000::int4 as answer,
     2000::int4 = linear_interpolate(100::int2, 600::int2, 2250::int4, 200::int2, 2050::int4) as match ;

select 
    linear_interpolate(600::int2, 200::int2, 2050::int4, 100::int2, 2000::int4),
     2250::int4 as answer,
     2250::int4 = linear_interpolate(600::int2, 200::int2, 2050::int4, 100::int2, 2000::int4) as match ;

select 
    linear_interpolate(600::int2, 100::int2, 2000::int4, 200::int2, 2050::int4),
     2250::int4 as answer,
     2250::int4 = linear_interpolate(600::int2, 100::int2, 2000::int4, 200::int2, 2050::int4) as match ;

select 
    linear_interpolate(200::int2, 200::int2, 2050::int4, 200::int2, 2050::int4),
     2050::int4 as answer,
     2050::int4 = linear_interpolate(200::int2, 200::int2, 2050::int4, 200::int2, 2050::int4) as match ;

-- Abscissa: date, Ordinate: int4
-- Check correctness - all results should have match = t

select 
    linear_interpolate('2001-01-21'::date, '2001-01-11'::date, 2000::int4, '2001-03-02'::date, 2250::int4),
     2050::int4 as answer,
     2050::int4 = linear_interpolate('2001-01-21'::date, '2001-01-11'::date, 2000::int4, '2001-03-02'::date, 2250::int4) as match ;

select 
    linear_interpolate('2001-01-21'::date, '2001-03-02'::date, 2250::int4, '2001-01-11'::date, 2000::int4),
     2050::int4 as answer,
     2050::int4 = linear_interpolate('2001-01-21'::date, '2001-03-02'::date, 2250::int4, '2001-01-11'::date, 2000::int4) as match ;

select 
    linear_interpolate('2001-01-11'::date, '2001-01-21'::date, 2050::int4, '2001-03-02'::date, 2250::int4),
     2000::int4 as answer,
     2000::int4 = linear_interpolate('2001-01-11'::date, '2001-01-21'::date, 2050::int4, '2001-03-02'::date, 2250::int4) as match ;

select 
    linear_interpolate('2001-01-11'::date, '2001-03-02'::date, 2250::int4, '2001-01-21'::date, 2050::int4),
     2000::int4 as answer,
     2000::int4 = linear_interpolate('2001-01-11'::date, '2001-03-02'::date, 2250::int4, '2001-01-21'::date, 2050::int4) as match ;

select 
    linear_interpolate('2001-03-02'::date, '2001-01-21'::date, 2050::int4, '2001-01-11'::date, 2000::int4),
     2250::int4 as answer,
     2250::int4 = linear_interpolate('2001-03-02'::date, '2001-01-21'::date, 2050::int4, '2001-01-11'::date, 2000::int4) as match ;

select 
    linear_interpolate('2001-03-02'::date, '2001-01-11'::date, 2000::int4, '2001-01-21'::date, 2050::int4),
     2250::int4 as answer,
     2250::int4 = linear_interpolate('2001-03-02'::date, '2001-01-11'::date, 2000::int4, '2001-01-21'::date, 2050::int4) as match ;

select 
    linear_interpolate('2001-01-21'::date, '2001-01-21'::date, 2050::int4, '2001-01-21'::date, 2050::int4),
     2050::int4 as answer,
     2050::int4 = linear_interpolate('2001-01-21'::date, '2001-01-21'::date, 2050::int4, '2001-01-21'::date, 2050::int4) as match ;

-- Abscissa: int8, Ordinate: int4
-- Check correctness - all results should have match = t

select 
    linear_interpolate(200::int8, 100::int8, 2000::int4, 600::int8, 2250::int4),
     2050::int4 as answer,
     2050::int4 = linear_interpolate(200::int8, 100::int8, 2000::int4, 600::int8, 2250::int4) as match ;

select 
    linear_interpolate(200::int8, 600::int8, 2250::int4, 100::int8, 2000::int4),
     2050::int4 as answer,
     2050::int4 = linear_interpolate(200::int8, 600::int8, 2250::int4, 100::int8, 2000::int4) as match ;

select 
    linear_interpolate(100::int8, 200::int8, 2050::int4, 600::int8, 2250::int4),
     2000::int4 as answer,
     2000::int4 = linear_interpolate(100::int8, 200::int8, 2050::int4, 600::int8, 2250::int4) as match ;

select 
    linear_interpolate(100::int8, 600::int8, 2250::int4, 200::int8, 2050::int4),
     2000::int4 as answer,
     2000::int4 = linear_interpolate(100::int8, 600::int8, 2250::int4, 200::int8, 2050::int4) as match ;

select 
    linear_interpolate(600::int8, 200::int8, 2050::int4, 100::int8, 2000::int4),
     2250::int4 as answer,
     2250::int4 = linear_interpolate(600::int8, 200::int8, 2050::int4, 100::int8, 2000::int4) as match ;

select 
    linear_interpolate(600::int8, 100::int8, 2000::int4, 200::int8, 2050::int4),
     2250::int4 as answer,
     2250::int4 = linear_interpolate(600::int8, 100::int8, 2000::int4, 200::int8, 2050::int4) as match ;

select 
    linear_interpolate(200::int8, 200::int8, 2050::int4, 200::int8, 2050::int4),
     2050::int4 as answer,
     2050::int4 = linear_interpolate(200::int8, 200::int8, 2050::int4, 200::int8, 2050::int4) as match ;

-- Abscissa: interval, Ordinate: int4
-- Check correctness - all results should have match = t

select 
    linear_interpolate('85 minutes'::interval, '65 minutes'::interval, 2000::int4, '165 minutes'::interval, 2250::int4),
     2050::int4 as answer,
     2050::int4 = linear_interpolate('85 minutes'::interval, '65 minutes'::interval, 2000::int4, '165 minutes'::interval, 2250::int4) as match ;

select 
    linear_interpolate('85 minutes'::interval, '165 minutes'::interval, 2250::int4, '65 minutes'::interval, 2000::int4),
     2050::int4 as answer,
     2050::int4 = linear_interpolate('85 minutes'::interval, '165 minutes'::interval, 2250::int4, '65 minutes'::interval, 2000::int4) as match ;

select 
    linear_interpolate('65 minutes'::interval, '85 minutes'::interval, 2050::int4, '165 minutes'::interval, 2250::int4),
     2000::int4 as answer,
     2000::int4 = linear_interpolate('65 minutes'::interval, '85 minutes'::interval, 2050::int4, '165 minutes'::interval, 2250::int4) as match ;

select 
    linear_interpolate('65 minutes'::interval, '165 minutes'::interval, 2250::int4, '85 minutes'::interval, 2050::int4),
     2000::int4 as answer,
     2000::int4 = linear_interpolate('65 minutes'::interval, '165 minutes'::interval, 2250::int4, '85 minutes'::interval, 2050::int4) as match ;

select 
    linear_interpolate('165 minutes'::interval, '85 minutes'::interval, 2050::int4, '65 minutes'::interval, 2000::int4),
     2250::int4 as answer,
     2250::int4 = linear_interpolate('165 minutes'::interval, '85 minutes'::interval, 2050::int4, '65 minutes'::interval, 2000::int4) as match ;

select 
    linear_interpolate('165 minutes'::interval, '65 minutes'::interval, 2000::int4, '85 minutes'::interval, 2050::int4),
     2250::int4 as answer,
     2250::int4 = linear_interpolate('165 minutes'::interval, '65 minutes'::interval, 2000::int4, '85 minutes'::interval, 2050::int4) as match ;

select 
    linear_interpolate('85 minutes'::interval, '85 minutes'::interval, 2050::int4, '85 minutes'::interval, 2050::int4),
     2050::int4 as answer,
     2050::int4 = linear_interpolate('85 minutes'::interval, '85 minutes'::interval, 2050::int4, '85 minutes'::interval, 2050::int4) as match ;

-- Abscissa: timestamptz, Ordinate: int4
-- Check correctness - all results should have match = t

select 
    linear_interpolate('2010-01-03T02:00:00'::timestamptz, '2010-01-02T09:20:00'::timestamptz, 2000::int4, '2010-01-05T20:40:00'::timestamptz, 2250::int4),
     2050::int4 as answer,
     2050::int4 = linear_interpolate('2010-01-03T02:00:00'::timestamptz, '2010-01-02T09:20:00'::timestamptz, 2000::int4, '2010-01-05T20:40:00'::timestamptz, 2250::int4) as match ;

select 
    linear_interpolate('2010-01-03T02:00:00'::timestamptz, '2010-01-05T20:40:00'::timestamptz, 2250::int4, '2010-01-02T09:20:00'::timestamptz, 2000::int4),
     2050::int4 as answer,
     2050::int4 = linear_interpolate('2010-01-03T02:00:00'::timestamptz, '2010-01-05T20:40:00'::timestamptz, 2250::int4, '2010-01-02T09:20:00'::timestamptz, 2000::int4) as match ;

select 
    linear_interpolate('2010-01-02T09:20:00'::timestamptz, '2010-01-03T02:00:00'::timestamptz, 2050::int4, '2010-01-05T20:40:00'::timestamptz, 2250::int4),
     2000::int4 as answer,
     2000::int4 = linear_interpolate('2010-01-02T09:20:00'::timestamptz, '2010-01-03T02:00:00'::timestamptz, 2050::int4, '2010-01-05T20:40:00'::timestamptz, 2250::int4) as match ;

select 
    linear_interpolate('2010-01-02T09:20:00'::timestamptz, '2010-01-05T20:40:00'::timestamptz, 2250::int4, '2010-01-03T02:00:00'::timestamptz, 2050::int4),
     2000::int4 as answer,
     2000::int4 = linear_interpolate('2010-01-02T09:20:00'::timestamptz, '2010-01-05T20:40:00'::timestamptz, 2250::int4, '2010-01-03T02:00:00'::timestamptz, 2050::int4) as match ;

select 
    linear_interpolate('2010-01-05T20:40:00'::timestamptz, '2010-01-03T02:00:00'::timestamptz, 2050::int4, '2010-01-02T09:20:00'::timestamptz, 2000::int4),
     2250::int4 as answer,
     2250::int4 = linear_interpolate('2010-01-05T20:40:00'::timestamptz, '2010-01-03T02:00:00'::timestamptz, 2050::int4, '2010-01-02T09:20:00'::timestamptz, 2000::int4) as match ;

select 
    linear_interpolate('2010-01-05T20:40:00'::timestamptz, '2010-01-02T09:20:00'::timestamptz, 2000::int4, '2010-01-03T02:00:00'::timestamptz, 2050::int4),
     2250::int4 as answer,
     2250::int4 = linear_interpolate('2010-01-05T20:40:00'::timestamptz, '2010-01-02T09:20:00'::timestamptz, 2000::int4, '2010-01-03T02:00:00'::timestamptz, 2050::int4) as match ;

select 
    linear_interpolate('2010-01-03T02:00:00'::timestamptz, '2010-01-03T02:00:00'::timestamptz, 2050::int4, '2010-01-03T02:00:00'::timestamptz, 2050::int4),
     2050::int4 as answer,
     2050::int4 = linear_interpolate('2010-01-03T02:00:00'::timestamptz, '2010-01-03T02:00:00'::timestamptz, 2050::int4, '2010-01-03T02:00:00'::timestamptz, 2050::int4) as match ;

-- Abscissa: time, Ordinate: int4
-- Check correctness - all results should have match = t

select 
    linear_interpolate('10:25:00'::time, '10:20:00'::time, 2000::int4, '10:45:00'::time, 2250::int4),
     2050::int4 as answer,
     2050::int4 = linear_interpolate('10:25:00'::time, '10:20:00'::time, 2000::int4, '10:45:00'::time, 2250::int4) as match ;

select 
    linear_interpolate('10:25:00'::time, '10:45:00'::time, 2250::int4, '10:20:00'::time, 2000::int4),
     2050::int4 as answer,
     2050::int4 = linear_interpolate('10:25:00'::time, '10:45:00'::time, 2250::int4, '10:20:00'::time, 2000::int4) as match ;

select 
    linear_interpolate('10:20:00'::time, '10:25:00'::time, 2050::int4, '10:45:00'::time, 2250::int4),
     2000::int4 as answer,
     2000::int4 = linear_interpolate('10:20:00'::time, '10:25:00'::time, 2050::int4, '10:45:00'::time, 2250::int4) as match ;

select 
    linear_interpolate('10:20:00'::time, '10:45:00'::time, 2250::int4, '10:25:00'::time, 2050::int4),
     2000::int4 as answer,
     2000::int4 = linear_interpolate('10:20:00'::time, '10:45:00'::time, 2250::int4, '10:25:00'::time, 2050::int4) as match ;

select 
    linear_interpolate('10:45:00'::time, '10:25:00'::time, 2050::int4, '10:20:00'::time, 2000::int4),
     2250::int4 as answer,
     2250::int4 = linear_interpolate('10:45:00'::time, '10:25:00'::time, 2050::int4, '10:20:00'::time, 2000::int4) as match ;

select 
    linear_interpolate('10:45:00'::time, '10:20:00'::time, 2000::int4, '10:25:00'::time, 2050::int4),
     2250::int4 as answer,
     2250::int4 = linear_interpolate('10:45:00'::time, '10:20:00'::time, 2000::int4, '10:25:00'::time, 2050::int4) as match ;

select 
    linear_interpolate('10:25:00'::time, '10:25:00'::time, 2050::int4, '10:25:00'::time, 2050::int4),
     2050::int4 as answer,
     2050::int4 = linear_interpolate('10:25:00'::time, '10:25:00'::time, 2050::int4, '10:25:00'::time, 2050::int4) as match ;

-- Abscissa: float4, Ordinate: int4
-- Check correctness - all results should have match = t

select 
    linear_interpolate(200::float4, 100::float4, 2000::int4, 600::float4, 2250::int4),
     2050::int4 as answer,
     2050::int4 = linear_interpolate(200::float4, 100::float4, 2000::int4, 600::float4, 2250::int4) as match ;

select 
    linear_interpolate(200::float4, 600::float4, 2250::int4, 100::float4, 2000::int4),
     2050::int4 as answer,
     2050::int4 = linear_interpolate(200::float4, 600::float4, 2250::int4, 100::float4, 2000::int4) as match ;

select 
    linear_interpolate(100::float4, 200::float4, 2050::int4, 600::float4, 2250::int4),
     2000::int4 as answer,
     2000::int4 = linear_interpolate(100::float4, 200::float4, 2050::int4, 600::float4, 2250::int4) as match ;

select 
    linear_interpolate(100::float4, 600::float4, 2250::int4, 200::float4, 2050::int4),
     2000::int4 as answer,
     2000::int4 = linear_interpolate(100::float4, 600::float4, 2250::int4, 200::float4, 2050::int4) as match ;

select 
    linear_interpolate(600::float4, 200::float4, 2050::int4, 100::float4, 2000::int4),
     2250::int4 as answer,
     2250::int4 = linear_interpolate(600::float4, 200::float4, 2050::int4, 100::float4, 2000::int4) as match ;

select 
    linear_interpolate(600::float4, 100::float4, 2000::int4, 200::float4, 2050::int4),
     2250::int4 as answer,
     2250::int4 = linear_interpolate(600::float4, 100::float4, 2000::int4, 200::float4, 2050::int4) as match ;

select 
    linear_interpolate(200::float4, 200::float4, 2050::int4, 200::float4, 2050::int4),
     2050::int4 as answer,
     2050::int4 = linear_interpolate(200::float4, 200::float4, 2050::int4, 200::float4, 2050::int4) as match ;

-- Abscissa: int4, Ordinate: timestamp
-- Check correctness - all results should have match = t

select 
    linear_interpolate(200::int4, 100::int4, '2012-06-01T16:40:00'::timestamp, 600::int4, '2012-06-19T01:20:00'::timestamp),
     '2012-06-05T04:00:00'::timestamp as answer,
     '2012-06-05T04:00:00'::timestamp = linear_interpolate(200::int4, 100::int4, '2012-06-01T16:40:00'::timestamp, 600::int4, '2012-06-19T01:20:00'::timestamp) as match ;

select 
    linear_interpolate(200::int4, 600::int4, '2012-06-19T01:20:00'::timestamp, 100::int4, '2012-06-01T16:40:00'::timestamp),
     '2012-06-05T04:00:00'::timestamp as answer,
     '2012-06-05T04:00:00'::timestamp = linear_interpolate(200::int4, 600::int4, '2012-06-19T01:20:00'::timestamp, 100::int4, '2012-06-01T16:40:00'::timestamp) as match ;

select 
    linear_interpolate(100::int4, 200::int4, '2012-06-05T04:00:00'::timestamp, 600::int4, '2012-06-19T01:20:00'::timestamp),
     '2012-06-01T16:40:00'::timestamp as answer,
     '2012-06-01T16:40:00'::timestamp = linear_interpolate(100::int4, 200::int4, '2012-06-05T04:00:00'::timestamp, 600::int4, '2012-06-19T01:20:00'::timestamp) as match ;

select 
    linear_interpolate(100::int4, 600::int4, '2012-06-19T01:20:00'::timestamp, 200::int4, '2012-06-05T04:00:00'::timestamp),
     '2012-06-01T16:40:00'::timestamp as answer,
     '2012-06-01T16:40:00'::timestamp = linear_interpolate(100::int4, 600::int4, '2012-06-19T01:20:00'::timestamp, 200::int4, '2012-06-05T04:00:00'::timestamp) as match ;

select 
    linear_interpolate(600::int4, 200::int4, '2012-06-05T04:00:00'::timestamp, 100::int4, '2012-06-01T16:40:00'::timestamp),
     '2012-06-19T01:20:00'::timestamp as answer,
     '2012-06-19T01:20:00'::timestamp = linear_interpolate(600::int4, 200::int4, '2012-06-05T04:00:00'::timestamp, 100::int4, '2012-06-01T16:40:00'::timestamp) as match ;

select 
    linear_interpolate(600::int4, 100::int4, '2012-06-01T16:40:00'::timestamp, 200::int4, '2012-06-05T04:00:00'::timestamp),
     '2012-06-19T01:20:00'::timestamp as answer,
     '2012-06-19T01:20:00'::timestamp = linear_interpolate(600::int4, 100::int4, '2012-06-01T16:40:00'::timestamp, 200::int4, '2012-06-05T04:00:00'::timestamp) as match ;

select 
    linear_interpolate(200::int4, 200::int4, '2012-06-05T04:00:00'::timestamp, 200::int4, '2012-06-05T04:00:00'::timestamp),
     '2012-06-05T04:00:00'::timestamp as answer,
     '2012-06-05T04:00:00'::timestamp = linear_interpolate(200::int4, 200::int4, '2012-06-05T04:00:00'::timestamp, 200::int4, '2012-06-05T04:00:00'::timestamp) as match ;

-- Abscissa: int4, Ordinate: numeric
-- Check correctness - all results should have match = t

select 
    linear_interpolate(200::int4, 100::int4, 2000::numeric, 600::int4, 2250::numeric),
     2050::numeric as answer,
     2050::numeric = linear_interpolate(200::int4, 100::int4, 2000::numeric, 600::int4, 2250::numeric) as match ;

select 
    linear_interpolate(200::int4, 600::int4, 2250::numeric, 100::int4, 2000::numeric),
     2050::numeric as answer,
     2050::numeric = linear_interpolate(200::int4, 600::int4, 2250::numeric, 100::int4, 2000::numeric) as match ;

select 
    linear_interpolate(100::int4, 200::int4, 2050::numeric, 600::int4, 2250::numeric),
     2000::numeric as answer,
     2000::numeric = linear_interpolate(100::int4, 200::int4, 2050::numeric, 600::int4, 2250::numeric) as match ;

select 
    linear_interpolate(100::int4, 600::int4, 2250::numeric, 200::int4, 2050::numeric),
     2000::numeric as answer,
     2000::numeric = linear_interpolate(100::int4, 600::int4, 2250::numeric, 200::int4, 2050::numeric) as match ;

select 
    linear_interpolate(600::int4, 200::int4, 2050::numeric, 100::int4, 2000::numeric),
     2250::numeric as answer,
     2250::numeric = linear_interpolate(600::int4, 200::int4, 2050::numeric, 100::int4, 2000::numeric) as match ;

select 
    linear_interpolate(600::int4, 100::int4, 2000::numeric, 200::int4, 2050::numeric),
     2250::numeric as answer,
     2250::numeric = linear_interpolate(600::int4, 100::int4, 2000::numeric, 200::int4, 2050::numeric) as match ;

select 
    linear_interpolate(200::int4, 200::int4, 2050::numeric, 200::int4, 2050::numeric),
     2050::numeric as answer,
     2050::numeric = linear_interpolate(200::int4, 200::int4, 2050::numeric, 200::int4, 2050::numeric) as match ;

-- Abscissa: int4, Ordinate: int4
-- Check correctness - all results should have match = t

select 
    linear_interpolate(200::int4, 100::int4, 2000::int4, 600::int4, 2250::int4),
     2050::int4 as answer,
     2050::int4 = linear_interpolate(200::int4, 100::int4, 2000::int4, 600::int4, 2250::int4) as match ;

select 
    linear_interpolate(200::int4, 600::int4, 2250::int4, 100::int4, 2000::int4),
     2050::int4 as answer,
     2050::int4 = linear_interpolate(200::int4, 600::int4, 2250::int4, 100::int4, 2000::int4) as match ;

select 
    linear_interpolate(100::int4, 200::int4, 2050::int4, 600::int4, 2250::int4),
     2000::int4 as answer,
     2000::int4 = linear_interpolate(100::int4, 200::int4, 2050::int4, 600::int4, 2250::int4) as match ;

select 
    linear_interpolate(100::int4, 600::int4, 2250::int4, 200::int4, 2050::int4),
     2000::int4 as answer,
     2000::int4 = linear_interpolate(100::int4, 600::int4, 2250::int4, 200::int4, 2050::int4) as match ;

select 
    linear_interpolate(600::int4, 200::int4, 2050::int4, 100::int4, 2000::int4),
     2250::int4 as answer,
     2250::int4 = linear_interpolate(600::int4, 200::int4, 2050::int4, 100::int4, 2000::int4) as match ;

select 
    linear_interpolate(600::int4, 100::int4, 2000::int4, 200::int4, 2050::int4),
     2250::int4 as answer,
     2250::int4 = linear_interpolate(600::int4, 100::int4, 2000::int4, 200::int4, 2050::int4) as match ;

select 
    linear_interpolate(200::int4, 200::int4, 2050::int4, 200::int4, 2050::int4),
     2050::int4 as answer,
     2050::int4 = linear_interpolate(200::int4, 200::int4, 2050::int4, 200::int4, 2050::int4) as match ;

-- Abscissa: int4, Ordinate: float8
-- Check correctness - all results should have match = t

select 
    linear_interpolate(200::int4, 100::int4, 2000::float8, 600::int4, 2250::float8),
     2050::float8 as answer,
     2050::float8 = linear_interpolate(200::int4, 100::int4, 2000::float8, 600::int4, 2250::float8) as match ;

select 
    linear_interpolate(200::int4, 600::int4, 2250::float8, 100::int4, 2000::float8),
     2050::float8 as answer,
     2050::float8 = linear_interpolate(200::int4, 600::int4, 2250::float8, 100::int4, 2000::float8) as match ;

select 
    linear_interpolate(100::int4, 200::int4, 2050::float8, 600::int4, 2250::float8),
     2000::float8 as answer,
     2000::float8 = linear_interpolate(100::int4, 200::int4, 2050::float8, 600::int4, 2250::float8) as match ;

select 
    linear_interpolate(100::int4, 600::int4, 2250::float8, 200::int4, 2050::float8),
     2000::float8 as answer,
     2000::float8 = linear_interpolate(100::int4, 600::int4, 2250::float8, 200::int4, 2050::float8) as match ;

select 
    linear_interpolate(600::int4, 200::int4, 2050::float8, 100::int4, 2000::float8),
     2250::float8 as answer,
     2250::float8 = linear_interpolate(600::int4, 200::int4, 2050::float8, 100::int4, 2000::float8) as match ;

select 
    linear_interpolate(600::int4, 100::int4, 2000::float8, 200::int4, 2050::float8),
     2250::float8 as answer,
     2250::float8 = linear_interpolate(600::int4, 100::int4, 2000::float8, 200::int4, 2050::float8) as match ;

select 
    linear_interpolate(200::int4, 200::int4, 2050::float8, 200::int4, 2050::float8),
     2050::float8 as answer,
     2050::float8 = linear_interpolate(200::int4, 200::int4, 2050::float8, 200::int4, 2050::float8) as match ;

-- Abscissa: int4, Ordinate: int2
-- Check correctness - all results should have match = t

select 
    linear_interpolate(200::int4, 100::int4, 2000::int2, 600::int4, 2250::int2),
     2050::int2 as answer,
     2050::int2 = linear_interpolate(200::int4, 100::int4, 2000::int2, 600::int4, 2250::int2) as match ;

select 
    linear_interpolate(200::int4, 600::int4, 2250::int2, 100::int4, 2000::int2),
     2050::int2 as answer,
     2050::int2 = linear_interpolate(200::int4, 600::int4, 2250::int2, 100::int4, 2000::int2) as match ;

select 
    linear_interpolate(100::int4, 200::int4, 2050::int2, 600::int4, 2250::int2),
     2000::int2 as answer,
     2000::int2 = linear_interpolate(100::int4, 200::int4, 2050::int2, 600::int4, 2250::int2) as match ;

select 
    linear_interpolate(100::int4, 600::int4, 2250::int2, 200::int4, 2050::int2),
     2000::int2 as answer,
     2000::int2 = linear_interpolate(100::int4, 600::int4, 2250::int2, 200::int4, 2050::int2) as match ;

select 
    linear_interpolate(600::int4, 200::int4, 2050::int2, 100::int4, 2000::int2),
     2250::int2 as answer,
     2250::int2 = linear_interpolate(600::int4, 200::int4, 2050::int2, 100::int4, 2000::int2) as match ;

select 
    linear_interpolate(600::int4, 100::int4, 2000::int2, 200::int4, 2050::int2),
     2250::int2 as answer,
     2250::int2 = linear_interpolate(600::int4, 100::int4, 2000::int2, 200::int4, 2050::int2) as match ;

select 
    linear_interpolate(200::int4, 200::int4, 2050::int2, 200::int4, 2050::int2),
     2050::int2 as answer,
     2050::int2 = linear_interpolate(200::int4, 200::int4, 2050::int2, 200::int4, 2050::int2) as match ;

-- Abscissa: int4, Ordinate: date
-- Check correctness - all results should have match = t

select 
    linear_interpolate(200::int4, 100::int4, '2010-01-21'::date, 600::int4, '2010-03-12'::date),
     '2010-01-31'::date as answer,
     '2010-01-31'::date = linear_interpolate(200::int4, 100::int4, '2010-01-21'::date, 600::int4, '2010-03-12'::date) as match ;

select 
    linear_interpolate(200::int4, 600::int4, '2010-03-12'::date, 100::int4, '2010-01-21'::date),
     '2010-01-31'::date as answer,
     '2010-01-31'::date = linear_interpolate(200::int4, 600::int4, '2010-03-12'::date, 100::int4, '2010-01-21'::date) as match ;

select 
    linear_interpolate(100::int4, 200::int4, '2010-01-31'::date, 600::int4, '2010-03-12'::date),
     '2010-01-21'::date as answer,
     '2010-01-21'::date = linear_interpolate(100::int4, 200::int4, '2010-01-31'::date, 600::int4, '2010-03-12'::date) as match ;

select 
    linear_interpolate(100::int4, 600::int4, '2010-03-12'::date, 200::int4, '2010-01-31'::date),
     '2010-01-21'::date as answer,
     '2010-01-21'::date = linear_interpolate(100::int4, 600::int4, '2010-03-12'::date, 200::int4, '2010-01-31'::date) as match ;

select 
    linear_interpolate(600::int4, 200::int4, '2010-01-31'::date, 100::int4, '2010-01-21'::date),
     '2010-03-12'::date as answer,
     '2010-03-12'::date = linear_interpolate(600::int4, 200::int4, '2010-01-31'::date, 100::int4, '2010-01-21'::date) as match ;

select 
    linear_interpolate(600::int4, 100::int4, '2010-01-21'::date, 200::int4, '2010-01-31'::date),
     '2010-03-12'::date as answer,
     '2010-03-12'::date = linear_interpolate(600::int4, 100::int4, '2010-01-21'::date, 200::int4, '2010-01-31'::date) as match ;

select 
    linear_interpolate(200::int4, 200::int4, '2010-01-31'::date, 200::int4, '2010-01-31'::date),
     '2010-01-31'::date as answer,
     '2010-01-31'::date = linear_interpolate(200::int4, 200::int4, '2010-01-31'::date, 200::int4, '2010-01-31'::date) as match ;

-- Abscissa: int4, Ordinate: int8
-- Check correctness - all results should have match = t

select 
    linear_interpolate(200::int4, 100::int4, 2000::int8, 600::int4, 2250::int8),
     2050::int8 as answer,
     2050::int8 = linear_interpolate(200::int4, 100::int4, 2000::int8, 600::int4, 2250::int8) as match ;

select 
    linear_interpolate(200::int4, 600::int4, 2250::int8, 100::int4, 2000::int8),
     2050::int8 as answer,
     2050::int8 = linear_interpolate(200::int4, 600::int4, 2250::int8, 100::int4, 2000::int8) as match ;

select 
    linear_interpolate(100::int4, 200::int4, 2050::int8, 600::int4, 2250::int8),
     2000::int8 as answer,
     2000::int8 = linear_interpolate(100::int4, 200::int4, 2050::int8, 600::int4, 2250::int8) as match ;

select 
    linear_interpolate(100::int4, 600::int4, 2250::int8, 200::int4, 2050::int8),
     2000::int8 as answer,
     2000::int8 = linear_interpolate(100::int4, 600::int4, 2250::int8, 200::int4, 2050::int8) as match ;

select 
    linear_interpolate(600::int4, 200::int4, 2050::int8, 100::int4, 2000::int8),
     2250::int8 as answer,
     2250::int8 = linear_interpolate(600::int4, 200::int4, 2050::int8, 100::int4, 2000::int8) as match ;

select 
    linear_interpolate(600::int4, 100::int4, 2000::int8, 200::int4, 2050::int8),
     2250::int8 as answer,
     2250::int8 = linear_interpolate(600::int4, 100::int4, 2000::int8, 200::int4, 2050::int8) as match ;

select 
    linear_interpolate(200::int4, 200::int4, 2050::int8, 200::int4, 2050::int8),
     2050::int8 as answer,
     2050::int8 = linear_interpolate(200::int4, 200::int4, 2050::int8, 200::int4, 2050::int8) as match ;

-- Abscissa: int4, Ordinate: interval
-- Check correctness - all results should have match = t

select 
    linear_interpolate(200::int4, 100::int4, '50 minutes'::interval, 600::int4, '100 minutes'::interval),
     '60 minutes'::interval as answer,
     '60 minutes'::interval = linear_interpolate(200::int4, 100::int4, '50 minutes'::interval, 600::int4, '100 minutes'::interval) as match ;

select 
    linear_interpolate(200::int4, 600::int4, '100 minutes'::interval, 100::int4, '50 minutes'::interval),
     '60 minutes'::interval as answer,
     '60 minutes'::interval = linear_interpolate(200::int4, 600::int4, '100 minutes'::interval, 100::int4, '50 minutes'::interval) as match ;

select 
    linear_interpolate(100::int4, 200::int4, '60 minutes'::interval, 600::int4, '100 minutes'::interval),
     '50 minutes'::interval as answer,
     '50 minutes'::interval = linear_interpolate(100::int4, 200::int4, '60 minutes'::interval, 600::int4, '100 minutes'::interval) as match ;

select 
    linear_interpolate(100::int4, 600::int4, '100 minutes'::interval, 200::int4, '60 minutes'::interval),
     '50 minutes'::interval as answer,
     '50 minutes'::interval = linear_interpolate(100::int4, 600::int4, '100 minutes'::interval, 200::int4, '60 minutes'::interval) as match ;

select 
    linear_interpolate(600::int4, 200::int4, '60 minutes'::interval, 100::int4, '50 minutes'::interval),
     '100 minutes'::interval as answer,
     '100 minutes'::interval = linear_interpolate(600::int4, 200::int4, '60 minutes'::interval, 100::int4, '50 minutes'::interval) as match ;

select 
    linear_interpolate(600::int4, 100::int4, '50 minutes'::interval, 200::int4, '60 minutes'::interval),
     '100 minutes'::interval as answer,
     '100 minutes'::interval = linear_interpolate(600::int4, 100::int4, '50 minutes'::interval, 200::int4, '60 minutes'::interval) as match ;

select 
    linear_interpolate(200::int4, 200::int4, '60 minutes'::interval, 200::int4, '60 minutes'::interval),
     '60 minutes'::interval as answer,
     '60 minutes'::interval = linear_interpolate(200::int4, 200::int4, '60 minutes'::interval, 200::int4, '60 minutes'::interval) as match ;

-- Abscissa: int4, Ordinate: timestamptz
-- Check correctness - all results should have match = t

select 
    linear_interpolate(200::int4, 100::int4, '2012-06-01T16:40:00'::timestamptz, 600::int4, '2012-06-19T01:20:00'::timestamptz),
     '2012-06-05T04:00:00'::timestamptz as answer,
     '2012-06-05T04:00:00'::timestamptz = linear_interpolate(200::int4, 100::int4, '2012-06-01T16:40:00'::timestamptz, 600::int4, '2012-06-19T01:20:00'::timestamptz) as match ;

select 
    linear_interpolate(200::int4, 600::int4, '2012-06-19T01:20:00'::timestamptz, 100::int4, '2012-06-01T16:40:00'::timestamptz),
     '2012-06-05T04:00:00'::timestamptz as answer,
     '2012-06-05T04:00:00'::timestamptz = linear_interpolate(200::int4, 600::int4, '2012-06-19T01:20:00'::timestamptz, 100::int4, '2012-06-01T16:40:00'::timestamptz) as match ;

select 
    linear_interpolate(100::int4, 200::int4, '2012-06-05T04:00:00'::timestamptz, 600::int4, '2012-06-19T01:20:00'::timestamptz),
     '2012-06-01T16:40:00'::timestamptz as answer,
     '2012-06-01T16:40:00'::timestamptz = linear_interpolate(100::int4, 200::int4, '2012-06-05T04:00:00'::timestamptz, 600::int4, '2012-06-19T01:20:00'::timestamptz) as match ;

select 
    linear_interpolate(100::int4, 600::int4, '2012-06-19T01:20:00'::timestamptz, 200::int4, '2012-06-05T04:00:00'::timestamptz),
     '2012-06-01T16:40:00'::timestamptz as answer,
     '2012-06-01T16:40:00'::timestamptz = linear_interpolate(100::int4, 600::int4, '2012-06-19T01:20:00'::timestamptz, 200::int4, '2012-06-05T04:00:00'::timestamptz) as match ;

select 
    linear_interpolate(600::int4, 200::int4, '2012-06-05T04:00:00'::timestamptz, 100::int4, '2012-06-01T16:40:00'::timestamptz),
     '2012-06-19T01:20:00'::timestamptz as answer,
     '2012-06-19T01:20:00'::timestamptz = linear_interpolate(600::int4, 200::int4, '2012-06-05T04:00:00'::timestamptz, 100::int4, '2012-06-01T16:40:00'::timestamptz) as match ;

select 
    linear_interpolate(600::int4, 100::int4, '2012-06-01T16:40:00'::timestamptz, 200::int4, '2012-06-05T04:00:00'::timestamptz),
     '2012-06-19T01:20:00'::timestamptz as answer,
     '2012-06-19T01:20:00'::timestamptz = linear_interpolate(600::int4, 100::int4, '2012-06-01T16:40:00'::timestamptz, 200::int4, '2012-06-05T04:00:00'::timestamptz) as match ;

select 
    linear_interpolate(200::int4, 200::int4, '2012-06-05T04:00:00'::timestamptz, 200::int4, '2012-06-05T04:00:00'::timestamptz),
     '2012-06-05T04:00:00'::timestamptz as answer,
     '2012-06-05T04:00:00'::timestamptz = linear_interpolate(200::int4, 200::int4, '2012-06-05T04:00:00'::timestamptz, 200::int4, '2012-06-05T04:00:00'::timestamptz) as match ;

-- Abscissa: int4, Ordinate: time
-- Check correctness - all results should have match = t

select 
    linear_interpolate(200::int4, 100::int4, '15:00:00'::time, 600::int4, '15:50:00'::time),
     '15:10:00'::time as answer,
     '15:10:00'::time = linear_interpolate(200::int4, 100::int4, '15:00:00'::time, 600::int4, '15:50:00'::time) as match ;

select 
    linear_interpolate(200::int4, 600::int4, '15:50:00'::time, 100::int4, '15:00:00'::time),
     '15:10:00'::time as answer,
     '15:10:00'::time = linear_interpolate(200::int4, 600::int4, '15:50:00'::time, 100::int4, '15:00:00'::time) as match ;

select 
    linear_interpolate(100::int4, 200::int4, '15:10:00'::time, 600::int4, '15:50:00'::time),
     '15:00:00'::time as answer,
     '15:00:00'::time = linear_interpolate(100::int4, 200::int4, '15:10:00'::time, 600::int4, '15:50:00'::time) as match ;

select 
    linear_interpolate(100::int4, 600::int4, '15:50:00'::time, 200::int4, '15:10:00'::time),
     '15:00:00'::time as answer,
     '15:00:00'::time = linear_interpolate(100::int4, 600::int4, '15:50:00'::time, 200::int4, '15:10:00'::time) as match ;

select 
    linear_interpolate(600::int4, 200::int4, '15:10:00'::time, 100::int4, '15:00:00'::time),
     '15:50:00'::time as answer,
     '15:50:00'::time = linear_interpolate(600::int4, 200::int4, '15:10:00'::time, 100::int4, '15:00:00'::time) as match ;

select 
    linear_interpolate(600::int4, 100::int4, '15:00:00'::time, 200::int4, '15:10:00'::time),
     '15:50:00'::time as answer,
     '15:50:00'::time = linear_interpolate(600::int4, 100::int4, '15:00:00'::time, 200::int4, '15:10:00'::time) as match ;

select 
    linear_interpolate(200::int4, 200::int4, '15:10:00'::time, 200::int4, '15:10:00'::time),
     '15:10:00'::time as answer,
     '15:10:00'::time = linear_interpolate(200::int4, 200::int4, '15:10:00'::time, 200::int4, '15:10:00'::time) as match ;

-- Abscissa: int4, Ordinate: float4
-- Check correctness - all results should have match = t

select 
    linear_interpolate(200::int4, 100::int4, 2000::float4, 600::int4, 2250::float4),
     2050::float4 as answer,
     2050::float4 = linear_interpolate(200::int4, 100::int4, 2000::float4, 600::int4, 2250::float4) as match ;

select 
    linear_interpolate(200::int4, 600::int4, 2250::float4, 100::int4, 2000::float4),
     2050::float4 as answer,
     2050::float4 = linear_interpolate(200::int4, 600::int4, 2250::float4, 100::int4, 2000::float4) as match ;

select 
    linear_interpolate(100::int4, 200::int4, 2050::float4, 600::int4, 2250::float4),
     2000::float4 as answer,
     2000::float4 = linear_interpolate(100::int4, 200::int4, 2050::float4, 600::int4, 2250::float4) as match ;

select 
    linear_interpolate(100::int4, 600::int4, 2250::float4, 200::int4, 2050::float4),
     2000::float4 as answer,
     2000::float4 = linear_interpolate(100::int4, 600::int4, 2250::float4, 200::int4, 2050::float4) as match ;

select 
    linear_interpolate(600::int4, 200::int4, 2050::float4, 100::int4, 2000::float4),
     2250::float4 as answer,
     2250::float4 = linear_interpolate(600::int4, 200::int4, 2050::float4, 100::int4, 2000::float4) as match ;

select 
    linear_interpolate(600::int4, 100::int4, 2000::float4, 200::int4, 2050::float4),
     2250::float4 as answer,
     2250::float4 = linear_interpolate(600::int4, 100::int4, 2000::float4, 200::int4, 2050::float4) as match ;

select 
    linear_interpolate(200::int4, 200::int4, 2050::float4, 200::int4, 2050::float4),
     2050::float4 as answer,
     2050::float4 = linear_interpolate(200::int4, 200::int4, 2050::float4, 200::int4, 2050::float4) as match ;
