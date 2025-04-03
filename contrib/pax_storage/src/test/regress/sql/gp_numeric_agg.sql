-- tests MPP-6135
create table utable (
    tstart timestamp,
    tfinish timestamp,
    utxt text,
    unum numeric
    )
    distributed by (tstart);

create view uview(period, ctxt, wsum) as
select
    period, 
    utxt,
    sum(usum) over (partition by period)
from
	(   select 
		    to_char( tstart, 'YYYY-MM-DD HH24:00'), 
		    utxt, 
		    sum(unum)
		from

		(
			select 
			    tstart, 
			    utxt,
			    case -- this case seems critical
			        when tfinish >tstart
			        then unum 
			        else 0 
			        end
			from utable
		) x(tstart, utxt, unum)
		
		group by 1, 2
	) y(period, utxt, usum);


insert into utable values
    (timestamp '2009-05-01 01:01:10', timestamp '2009-05-01 02:01:10', 'a', 1.0),
    (timestamp '2009-05-01 02:01:10', timestamp '2009-05-01 01:01:10', 'a', 1.0),
    (timestamp '2009-05-01 02:01:10', timestamp '2009-05-01 03:01:10', 'a', 1.0),
    (timestamp '2009-05-01 03:01:10', timestamp '2009-05-01 03:01:10', 'b', 1.0),
    (timestamp '2009-05-01 01:01:09', timestamp '2009-05-01 02:01:10', 'b', 1.0),
    (timestamp '2009-05-03 01:01:10', timestamp '2009-05-01 02:01:10', 'b', 1.0);


-- This works.
select 
    period::date,
--    avg( case ctxt when 'a' then wsum end) as "a"
    avg( case ctxt when 'b' then wsum end) as "b"
from
    uview 
group by 1;


-- This fails on an assert on a segment (palloc)
select 
    period::date, -- the cast is significant 
    avg( case ctxt when 'a' then wsum end) as "a", 
    avg( case ctxt when 'b' then wsum end) as "b"
from
    uview 
group by 1;

-- start_ignore
drop view uview;
drop table utable;
-- end_ignore
