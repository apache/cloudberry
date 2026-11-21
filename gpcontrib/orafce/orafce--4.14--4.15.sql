-- dbms_alert is removed in GPDB because it is not possible to run it in MPP
-- environments as implemented in postgres.
/*
alter function dbms_alert.signal ( text, text) security invoker;
*/
