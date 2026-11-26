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
-- JSON EXPLAIN is returned as one text row per SQL statement by tpcds.explain.
-- Multi-statement templates also emit headings, which are excluded here.
CREATE OR REPLACE FUNCTION tpcds.rf_metrics(q int)
RETURNS TABLE (statement_no bigint, node text, rel text, total_ms numeric,
               child_ms numeric, overhead_ms numeric, rows_out numeric,
               bloom_removed numeric, pushdown_removed numeric,
               local_removed numeric)
LANGUAGE sql VOLATILE AS $$
    WITH RECURSIVE plans AS MATERIALIZED (
        SELECT row_number() OVER (ORDER BY ord) AS statement_no, line::jsonb AS plan
        FROM tpcds.explain(q, 'ANALYZE, BUFFERS, FORMAT JSON')
             WITH ORDINALITY AS p(line, ord)
        WHERE left(ltrim(line, E' \t\r\n'), 1) = '['
    ), walk(statement_no, n) AS (
        SELECT statement_no, plan->0->'Plan' FROM plans
        UNION ALL
        SELECT w.statement_no, c.node
        FROM walk w CROSS JOIN LATERAL jsonb_array_elements(coalesce(w.n->'Plans', '[]')) c(node)
    )
    SELECT statement_no, coalesce(n->>'Custom Plan Provider', n->>'Node Type'),
           n->>'Relation Name', (n->>'Actual Total Time')::numeric,
           (n->'Plans'->0->>'Actual Total Time')::numeric,
           CASE WHEN n->>'Custom Plan Provider' LIKE '%Bloom%' THEN
                (n->>'Actual Total Time')::numeric - (n->'Plans'->0->>'Actual Total Time')::numeric
           END,
           (n->>'Actual Rows')::numeric,
           (n->>'Rows Removed by Bloom Filter')::numeric,
           (n->>'Rows Removed by Pushdown Runtime Filter')::numeric,
           (n->>'Rows Removed by Local Bloom Filter')::numeric
    FROM walk
    WHERE n->>'Custom Plan Provider' LIKE '%Bloom%'
       OR n ? 'Rows Removed by Pushdown Runtime Filter'
       OR n->>'Parent Relationship' = 'InitPlan';
$$;
