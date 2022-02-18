select * from diskquota.status();

\! gpconfig -c "diskquota.hard_limit" -v "on" > /dev/null
\! gpstop -u > /dev/null
select * from diskquota.status();

\! gpconfig -c "diskquota.hard_limit" -v "off" > /dev/null
\! gpstop -u > /dev/null
select * from diskquota.status();

select from diskquota.pause();
select * from diskquota.status();

\! gpconfig -c "diskquota.hard_limit" -v "on" > /dev/null
\! gpstop -u > /dev/null
select * from diskquota.status();

\! gpconfig -c "diskquota.hard_limit" -v "off" > /dev/null
\! gpstop -u > /dev/null
select * from diskquota.status();

select from diskquota.resume();
\! gpconfig -c "diskquota.hard_limit" -v "off" > /dev/null
\! gpstop -u > /dev/null
select * from diskquota.status();
