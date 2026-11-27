---------------------------------------------------------------------------
--
-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--
--  http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
-- KIND, either express or implied.  See the License for the
-- specific language governing permissions and limitations
-- under the License.
--
-- Equality modulo 10 equates different raw Datum representations. RPT must
-- reject it even though the type is by-value and has default operator classes.
BEGIN;
SET LOCAL client_min_messages = warning;
CREATE EXTENSION robust_predicate_transfer;
CREATE SCHEMA rpt_eq_repro;
SET LOCAL search_path=rpt_eq_repro,pg_catalog;
SET LOCAL optimizer=off;
SET LOCAL statement_timeout='30s';
SET LOCAL anser.runtime_filter=off;
SET LOCAL rpt.enable=off;
-- Legal by-value type whose equality identifies congruent integers mod 10.
CREATE TYPE modint;
CREATE FUNCTION modint_in(cstring) RETURNS modint AS 'int4in' LANGUAGE internal IMMUTABLE STRICT;
CREATE FUNCTION modint_out(modint) RETURNS cstring AS 'int4out' LANGUAGE internal IMMUTABLE STRICT;
CREATE TYPE modint(INPUT=modint_in,OUTPUT=modint_out,INTERNALLENGTH=4,PASSEDBYVALUE,ALIGNMENT=int4);
CREATE CAST (modint AS integer) WITHOUT FUNCTION AS IMPLICIT;
CREATE CAST (integer AS modint) WITHOUT FUNCTION AS IMPLICIT;
CREATE FUNCTION modint_eq(modint,modint) RETURNS boolean LANGUAGE plpgsql IMMUTABLE STRICT
 AS $$ BEGIN RETURN ($1::integer % 10)=($2::integer % 10); END $$;
CREATE FUNCTION modint_hash(modint) RETURNS integer LANGUAGE sql IMMUTABLE STRICT
 AS 'SELECT hashint4($1::integer % 10)';
CREATE OPERATOR = (LEFTARG=modint,RIGHTARG=modint,FUNCTION=modint_eq,COMMUTATOR='=',HASHES);
CREATE OPERATOR CLASS modint_hash_ops DEFAULT FOR TYPE modint USING hash AS
 OPERATOR 1 = (modint,modint), FUNCTION 1 modint_hash(modint);
-- Supply the type's own btree family as well: otherwise PostgreSQL can
-- choose integer's default family via the explicitly binary-compatible cast.
CREATE FUNCTION modint_cmp(modint,modint) RETURNS integer LANGUAGE sql IMMUTABLE STRICT
 AS 'SELECT CASE WHEN $1::integer % 10 < $2::integer % 10 THEN -1 WHEN $1::integer % 10 > $2::integer % 10 THEN 1 ELSE 0 END';
CREATE FUNCTION modint_lt(modint,modint) RETURNS boolean LANGUAGE sql IMMUTABLE STRICT AS 'SELECT modint_cmp($1,$2)<0';
CREATE FUNCTION modint_le(modint,modint) RETURNS boolean LANGUAGE sql IMMUTABLE STRICT AS 'SELECT modint_cmp($1,$2)<=0';
CREATE FUNCTION modint_gt(modint,modint) RETURNS boolean LANGUAGE sql IMMUTABLE STRICT AS 'SELECT modint_cmp($1,$2)>0';
CREATE FUNCTION modint_ge(modint,modint) RETURNS boolean LANGUAGE sql IMMUTABLE STRICT AS 'SELECT modint_cmp($1,$2)>=0';
CREATE OPERATOR < (LEFTARG=modint,RIGHTARG=modint,FUNCTION=modint_lt);
CREATE OPERATOR <= (LEFTARG=modint,RIGHTARG=modint,FUNCTION=modint_le);
CREATE OPERATOR > (LEFTARG=modint,RIGHTARG=modint,FUNCTION=modint_gt);
CREATE OPERATOR >= (LEFTARG=modint,RIGHTARG=modint,FUNCTION=modint_ge);
CREATE OPERATOR CLASS modint_btree_ops DEFAULT FOR TYPE modint USING btree AS
 OPERATOR 1 <, OPERATOR 2 <=, OPERATOR 3 =, OPERATOR 4 >=, OPERATOR 5 >, FUNCTION 1 modint_cmp(modint,modint);
CREATE TABLE fact(k modint) DISTRIBUTED RANDOMLY;
CREATE TABLE dim(k modint,a int) DISTRIBUTED RANDOMLY;
INSERT INTO fact SELECT i::modint FROM generate_series(11,1010)i;
INSERT INTO dim SELECT i::modint,i FROM generate_series(1,10)i;
ANALYZE fact;
ANALYZE dim;
SET LOCAL rpt.forward_min_root_rows=0;
SET LOCAL rpt.forward_max_exchange_ms=-1;
SET LOCAL rpt.forward_max_selectivity=1;
DO $$
DECLARE
    opt text;
    baseline bigint;
    filtered bigint;
    dump text;
    q constant text := 'SELECT count(*) FROM fact f JOIN dim d ON f.k = d.k WHERE d.a < 11';
BEGIN
    FOREACH opt IN ARRAY ARRAY['off', 'on'] LOOP
        PERFORM set_config('optimizer', opt, true);
        PERFORM set_config('rpt.enable', 'off', true);
        EXECUTE q INTO baseline;
        PERFORM set_config('rpt.enable', 'on', true);
        dump := public.rpt_transfer_plan(q);
        EXECUTE q INTO filtered;
        IF baseline <> 1000 OR filtered IS DISTINCT FROM baseline THEN
            RAISE EXCEPTION 'custom equality lost rows (optimizer=%): % vs %', opt, filtered, baseline;
        END IF;
        IF dump NOT LIKE 'no transfer regions%' THEN
            RAISE EXCEPTION 'custom equality was accepted (optimizer=%): %', opt, dump;
        END IF;
    END LOOP;
END
$$;
ROLLBACK;
