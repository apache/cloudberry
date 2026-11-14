-- trunc()/round() and adding business days leave infinite dates as they
-- are; the plvdate functions that need a calendar date reject them.
SELECT oracle.trunc(date 'infinity', 'MM') AS trunc_inf,
       oracle.trunc(date '-infinity', 'Y') AS trunc_ninf,
       oracle.round(date 'infinity', 'Y') AS round_inf,
       oracle.round(date '-infinity', 'MM') AS round_ninf;
SELECT plvdate.add_bizdays(date 'infinity', 1) AS add_inf,
       plvdate.next_bizday(date '-infinity') AS next_ninf,
       plvdate.prev_bizday(date 'infinity') AS prev_inf,
       plvdate.nearest_bizday(date 'infinity') AS nearest_inf;
SELECT plvdate.isbizday(date 'infinity');
SELECT plvdate.days_inmonth(date '-infinity');
SELECT plvdate.isleapyear(date 'infinity');
SELECT plvdate.set_nonbizday(date 'infinity', false);
SELECT plvdate.unset_nonbizday(date '-infinity', true);
-- rounding past the last valid date
SELECT oracle.round(date '5874897-12-31', 'Y');
