-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--
--   http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
-- KIND, either express or implied.  See the License for the
-- specific language governing permissions and limitations
-- under the License.

set default_table_access_method = pax;

set pax_enable_toast to true;

-- test compress failed
-- random varchar always make compress toast failed
create or replace function random_string(integer)
returns text as
$body$
    select upper(array_to_string(array(select substring('0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz' 
    FROM (ceil(random()*62))::int FOR 1) FROM generate_series(1, $1)), ''));
$body$
language sql volatile;

create table pax_toasttest_compress_failed(v text);
insert into pax_toasttest_compress_failed values(random_string(1000000));
insert into pax_toasttest_compress_failed values(random_string(1000000));
insert into pax_toasttest_compress_failed values(random_string(1000000));
insert into pax_toasttest_compress_failed values(random_string(1000000));
insert into pax_toasttest_compress_failed values(random_string(1000000));

select length(v) from pax_toasttest_compress_failed;
drop function random_string;
drop table pax_toasttest_compress_failed;
