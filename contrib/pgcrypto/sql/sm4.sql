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
--
-- SM4 cipher (aka sm4-128)
-- including sm4-128-ecb and sm4-128-cbc
--
-- `icw_bash` will used `--with-openssl` version.
-- but openssl version which cbdb required have not SM2/SM3/SM4
-- 
-- start_ignore

-- ensure consistent test output regardless of the default bytea format
SET bytea_output TO escape;

-- some standard SM4 testvalues
SELECT encode(encrypt(
decode('00112233445566778899aabbccddeeff', 'hex'),
decode('000102030405060708090a0b0c0d0e0f', 'hex'),
'sm4-ecb/pad:none'), 'hex');

SELECT encode(encrypt(
decode('0123456789abcdeffedcba9876543210', 'hex'),
decode('0123456789abcdeffedcba9876543210', 'hex'),
'sm4-ecb/pad:none'), 'hex');

-- cbc 
SELECT encode(encrypt(
decode('00112233445566778899aabbccddeeff', 'hex'),
decode('000102030405060708090a0b0c0d0e0f', 'hex'),
'sm4-cbc/pad:none'), 'hex');

SELECT encode(encrypt(
decode('0123456789abcdeffedcba9876543210', 'hex'),
decode('0123456789abcdeffedcba9876543210', 'hex'),
'sm4-cbc/pad:none'), 'hex');

SELECT encode(encrypt_iv(
decode('00112233445566778899aabbccddeeff', 'hex'),
decode('000102030405060708090a0b0c0d0e0f', 'hex'),
decode('000102030405060708090a0b0c0d0e0f', 'hex'),
'sm4-cbc/pad:none'), 'hex');

SELECT encode(encrypt_iv(
decode('0123456789abcdeffedcba9876543210', 'hex'),
decode('0123456789abcdeffedcba9876543210', 'hex'),
decode('0123456789abcdeffedcba9876543210', 'hex'),
'sm4-cbc/pad:none'), 'hex');

-- key padding

SELECT encode(encrypt(
decode('00112233445566778899aabbccddeeff', 'hex'),
decode('000102030405060708090a0b0c0d0e0f', 'hex'),
'sm4-ecb'), 'hex');

SELECT encode(encrypt(
decode('0123456789abcdeffedcba9876543210', 'hex'),
decode('0123456789abcdeffedcba9876543210', 'hex'),
'sm4-ecb'), 'hex');

-- cbc without iv
SELECT encode(encrypt(
decode('00112233445566778899aabbccddeeff', 'hex'),
decode('000102030405060708090a0b0c0d0e0f', 'hex'),
'sm4-cbc'), 'hex');

SELECT encode(encrypt(
decode('0123456789abcdeffedcba9876543210', 'hex'),
decode('0123456789abcdeffedcba9876543210', 'hex'),
'sm4-cbc'), 'hex');

SELECT encode(encrypt_iv(
decode('00112233445566778899aabbccddeeff', 'hex'),
decode('000102030405060708090a0b0c0d0e0f', 'hex'),
decode('000102030405060708090a0b0c0d0e0f', 'hex'),
'sm4-cbc'), 'hex');

SELECT encode(encrypt_iv(
decode('0123456789abcdeffedcba9876543210', 'hex'),
decode('0123456789abcdeffedcba9876543210', 'hex'),
decode('0123456789abcdeffedcba9876543210', 'hex'),
'sm4-cbc'), 'hex');

-- data test
select encode(encrypt('', 'foo', 'sm4-ecb'), 'hex');
select encode(encrypt('foo', '0123456789', 'sm4-ecb'), 'hex');
select encode(encrypt('foo', '0123456789012345678901', 'sm4-ecb'), 'hex');

select encode(encrypt('', 'foo', 'sm4-cbc'), 'hex');
select encode(encrypt('foo', '0123456789', 'sm4-cbc'), 'hex');
select encode(encrypt('foo', '0123456789012345678901', 'sm4-cbc'), 'hex');

select encode(encrypt_iv('', 'foo', 'foo', 'sm4-cbc'), 'hex');
select encode(encrypt_iv('foo', '0123456789', '0123456789', 'sm4-cbc'), 'hex');
select encode(encrypt_iv('foo', '0123456789012345678901', '0123456789012345678901','sm4-cbc'), 'hex');

-- decrypt
select decrypt(encrypt('', 'foo', 'sm4-ecb'), 'foo', 'sm4-ecb');
select decrypt(encrypt('foo', '0123456', 'sm4-ecb'), '0123456', 'sm4-ecb');
select decrypt(encrypt('foofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoo', '01234567', 'sm4-ecb'), '01234567', 'sm4-ecb');

select decrypt(encrypt('', 'foo', 'sm4-cbc'), 'foo', 'sm4-cbc');
select decrypt(encrypt('foo', '0123456', 'sm4-cbc'), '0123456', 'sm4-cbc');
select decrypt(encrypt('foofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoo', '01234567', 'sm4-cbc'), '01234567', 'sm4-cbc');

select decrypt_iv(encrypt_iv('', 'foo', 'foo123','sm4-cbc'), 'foo', 'foo123', 'sm4-cbc');
select decrypt_iv(encrypt_iv('foo', '0123456', '012345678', 'sm4-cbc'), '0123456', '012345678', 'sm4-cbc');
select decrypt_iv(encrypt_iv('foofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoo', '01234567', 'foofoo', 'sm4-cbc'), '01234567', 'foofoo', 'sm4-cbc');
-- end_ignore
