UPDATE sales_heap
SET status = 'Closed',
    is_audited = TRUE,
    description = description || ' Audited'
WHERE is_audited = FALSE
  AND product_id > 1000000;

UPDATE sales_ao
SET status = 'Closed',
    is_audited = TRUE,
    description = description || ' Audited'
WHERE is_audited = FALSE
  AND product_id > 1000000;

UPDATE sales_aocs
SET status = 'Closed',
    is_audited = TRUE,
    description = description || ' Audited'
WHERE is_audited = FALSE
  AND product_id > 1000000;

UPDATE sales_partition_heap
SET status = 'Closed',
    is_audited = TRUE,
    description = description || ' Audited'
WHERE is_audited = FALSE
  AND product_id > 1000000;

UPDATE sales_partition_ao
SET status = 'Closed',
    is_audited = TRUE,
    description = description || ' Audited'
WHERE is_audited = FALSE
  AND product_id > 1000000;

UPDATE sales_partition_aocs
SET status = 'Closed',
    is_audited = TRUE,
    description = description || ' Audited'
WHERE is_audited = FALSE
  AND product_id > 1000000;

SELECT COUNT(*) FROM sales_heap WHERE is_audited = 'TRUE' AND product_id > 1000000;

SELECT COUNT(*) FROM sales_ao WHERE is_audited = 'TRUE' AND product_id > 1000000;

SELECT COUNT(*) FROM sales_aocs WHERE is_audited = 'TRUE' AND product_id > 1000000;

SELECT COUNT(*) FROM sales_partition_heap WHERE is_audited = 'TRUE' AND product_id > 1000000;

SELECT COUNT(*) FROM sales_partition_ao WHERE is_audited = 'TRUE' AND product_id > 1000000;

SELECT COUNT(*) FROM sales_partition_aocs WHERE is_audited = 'TRUE' AND product_id > 1000000;
                                                 