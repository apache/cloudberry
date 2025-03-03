INSERT INTO sales_heap (
  product_id,
  is_audited,
  quantity,
  total_sales,
  unit_price,
  discount,
  description,
  sale_date,
  order_date,
  status,
  customer_name,
  price
)
SELECT
  x.id, -- product_id
  CASE
    WHEN random() < 0.5 THEN TRUE
    ELSE FALSE
  END AS is_audited, -- is_audited
  (random() * 100)::smallint, -- quantity
  (random() * 1000000)::bigint, -- total_sales
  (random() * 100)::real, -- unit_price
  (random() * 1)::double precision, -- discount
  'Product description ' || x.id, -- description
  now() - INTERVAL '1 day' * floor(random() * 365 * 3), -- sale_date
  '2023-01-01'::DATE + INTERVAL '1 day' * floor(random() * 365 * 3), -- order_date
  CASE
    WHEN random() < 0.05 THEN 'Cancelled'
    WHEN random() < 0.5 THEN 'Closed'
    ELSE 'Processing'
  END AS status, -- status
  'Customer ' || x.id, -- customer_name
  (random() * 1000000)::decimal(20, 10) -- price
FROM (
  SELECT * FROM generate_series(1000001, 1010000) AS id
) AS x;


INSERT INTO sales_ao (
  product_id,
  is_audited,
  quantity,
  total_sales,
  unit_price,
  discount,
  description,
  sale_date,
  order_date,
  status,
  customer_name,
  price
)
SELECT
  x.id, -- product_id
  CASE
    WHEN random() < 0.5 THEN TRUE
    ELSE FALSE
  END AS is_audited, -- is_audited
  (random() * 100)::smallint, -- quantity
  (random() * 1000000)::bigint, -- total_sales
  (random() * 100)::real, -- unit_price
  (random() * 1)::double precision, -- discount
  'Product description ' || x.id, -- description
  now() - INTERVAL '1 day' * floor(random() * 365 * 3), -- sale_date
  '2023-01-01'::DATE + INTERVAL '1 day' * floor(random() * 365 * 3), -- order_date
  CASE
    WHEN random() < 0.05 THEN 'Cancelled'
    WHEN random() < 0.5 THEN 'Closed'
    ELSE 'Processing'
  END AS status, -- status
  'Customer ' || x.id, -- customer_name
  (random() * 1000000)::decimal(20, 10) -- price
FROM (
  SELECT * FROM generate_series(1000001, 1010000) AS id
) AS x;


INSERT INTO sales_aocs (
  product_id,
  is_audited,
  quantity,
  total_sales,
  unit_price,
  discount,
  description,
  sale_date,
  order_date,
  status,
  customer_name,
  price
)
SELECT
  x.id, -- product_id
  CASE
    WHEN random() < 0.5 THEN TRUE
    ELSE FALSE
  END AS is_audited, -- is_audited
  (random() * 100)::smallint, -- quantity
  (random() * 1000000)::bigint, -- total_sales
  (random() * 100)::real, -- unit_price
  (random() * 1)::double precision, -- discount
  'Product description ' || x.id, -- description
  now() - INTERVAL '1 day' * floor(random() * 365 * 3), -- sale_date
  '2023-01-01'::DATE + INTERVAL '1 day' * floor(random() * 365 * 3), -- order_date
  CASE
    WHEN random() < 0.05 THEN 'Cancelled'
    WHEN random() < 0.5 THEN 'Closed'
    ELSE 'Processing'
  END AS status, -- status
  'Customer ' || x.id, -- customer_name
  (random() * 1000000)::decimal(20, 10) -- price
FROM (
  SELECT * FROM generate_series(1000001, 1010000) AS id
) AS x;

INSERT INTO sales_partition_heap (
  product_id,
  is_audited,
  quantity,
  total_sales,
  unit_price,
  discount,
  description,
  sale_date,
  order_date,
  status,
  customer_name,
  price
)
SELECT
  x.id, -- product_id
  CASE
    WHEN random() < 0.5 THEN TRUE
    ELSE FALSE
  END AS is_audited, -- is_audited
  (random() * 100)::smallint, -- quantity
  (random() * 1000000)::bigint, -- total_sales
  (random() * 100)::real, -- unit_price
  (random() * 1)::double precision, -- discount
  'Product description ' || x.id, -- description
  now() - INTERVAL '1 day' * floor(random() * 365 * 3), -- sale_date
  '2023-01-01'::DATE + INTERVAL '1 day' * floor(random() * 365 * 3), -- order_date
  CASE
    WHEN random() < 0.05 THEN 'Cancelled'
    WHEN random() < 0.5 THEN 'Closed'
    ELSE 'Processing'
  END AS status, -- status
  'Customer ' || x.id, -- customer_name
  (random() * 1000000)::decimal(20, 10) -- price
FROM (
  SELECT * FROM generate_series(1000001, 1010000) AS id
) AS x;

INSERT INTO sales_partition_ao (
  product_id,
  is_audited,
  quantity,
  total_sales,
  unit_price,
  discount,
  description,
  sale_date,
  order_date,
  status,
  customer_name,
  price
)
SELECT
  x.id, -- product_id
  CASE
    WHEN random() < 0.5 THEN TRUE
    ELSE FALSE
  END AS is_audited, -- is_audited
  (random() * 100)::smallint, -- quantity
  (random() * 1000000)::bigint, -- total_sales
  (random() * 100)::real, -- unit_price
  (random() * 1)::double precision, -- discount
  'Product description ' || x.id, -- description
  now() - INTERVAL '1 day' * floor(random() * 365 * 3), -- sale_date
  '2023-01-01'::DATE + INTERVAL '1 day' * floor(random() * 365 * 3), -- order_date
  CASE
    WHEN random() < 0.05 THEN 'Cancelled'
    WHEN random() < 0.5 THEN 'Closed'
    ELSE 'Processing'
  END AS status, -- status
  'Customer ' || x.id, -- customer_name
  (random() * 1000000)::decimal(20, 10) -- price
FROM (
  SELECT * FROM generate_series(1000001, 1010000) AS id
) AS x;

INSERT INTO sales_partition_aocs (
  product_id,
  is_audited,
  quantity,
  total_sales,
  unit_price,
  discount,
  description,
  sale_date,
  order_date,
  status,
  customer_name,
  price
)
SELECT
  x.id, -- product_id
  CASE
    WHEN random() < 0.5 THEN TRUE
    ELSE FALSE
  END AS is_audited, -- is_audited
  (random() * 100)::smallint, -- quantity
  (random() * 1000000)::bigint, -- total_sales
  (random() * 100)::real, -- unit_price
  (random() * 1)::double precision, -- discount
  'Product description ' || x.id, -- description
  now() - INTERVAL '1 day' * floor(random() * 365 * 3), -- sale_date
  '2023-01-01'::DATE + INTERVAL '1 day' * floor(random() * 365 * 3), -- order_date
  CASE
    WHEN random() < 0.05 THEN 'Cancelled'
    WHEN random() < 0.5 THEN 'Closed'
    ELSE 'Processing'
  END AS status, -- status
  'Customer ' || x.id, -- customer_name
  (random() * 1000000)::decimal(20, 10) -- price
FROM (
  SELECT * FROM generate_series(1000001, 1010000) AS id
) AS x;

SELECT COUNT(*) FROM sales_heap;

SELECT COUNT(*) FROM sales_ao;

SELECT COUNT(*) FROM sales_aocs;

SELECT COUNT(*) FROM sales_partition_heap;

SELECT COUNT(*) FROM sales_partition_ao;

SELECT COUNT(*) FROM sales_partition_aocs;
