
set extra_float_digits=-1;
show enable_indexscan;
show enable_bitmapscan;

-- Q1: Select the top 10 products that have not been audited.  
SELECT product_id, description
FROM sales_partition_aocs 
WHERE is_audited = false 
ORDER BY product_id DESC
LIMIT 10;

-- Q2: Select the top 10 cancelled orders, ordered by product_id in descending order. 
SELECT product_id, quantity, total_sales, unit_price, discount, description
FROM sales_partition_aocs
WHERE status = 'Cancelled'
ORDER BY product_id DESC
LIMIT 10;

-- Q3: Select details of the product with ID 1. 
SELECT product_id, quantity, total_sales, unit_price, discount, description, sale_date, customer_name, price
FROM sales_partition_aocs
WHERE product_id = 1;

-- Q4: Select details of the product with ID 1 sold on 2023-08-24 14:03:01.089303. 
SELECT product_id, quantity, total_sales, unit_price, discount, description, sale_date, customer_name, price
FROM sales_partition_aocs
WHERE product_id = 1 AND sale_date = '2023-08-24 14:03:01.089303';

-- Q5: Select details of orders placed by a specific customer.
SELECT product_id, quantity, total_sales, unit_price, discount, description, sale_date, customer_name, price
FROM sales_partition_aocs
WHERE customer_name = 'Customer 100';

-- Q6: Select details of orders placed by a specific customer and product ID.
SELECT product_id, quantity, total_sales, unit_price, discount, description, sale_date, customer_name, price
FROM sales_partition_aocs
WHERE customer_name = 'Customer 10000' AND product_id = 10000;

-- Q7: Select sales data within a specific date range.
SELECT product_id, quantity, total_sales, unit_price, discount, description, sale_date, customer_name, price
FROM sales_partition_aocs
WHERE sale_date BETWEEN '2023-01-02' AND '2023-01-05'
ORDER BY product_id ASC
LIMIT 10;

-- Q8: Select sales data for a specific date.
SELECT product_id, quantity, total_sales, unit_price, discount, description, sale_date, customer_name, price
FROM sales_partition_aocs
WHERE sale_date = '2023-01-02 14:03:01.089303'
ORDER BY product_id ASC
LIMIT 10;

-- Q9: Summarize total sales by status.
SELECT status, SUM(total_sales) AS total_sales
FROM sales_partition_aocs
GROUP BY status
ORDER BY total_sales DESC
LIMIT 10;

-- Q10: Compute the cumulative total sales over time for each product.
SELECT product_id, sale_date, SUM(total_sales) OVER (PARTITION BY product_id ORDER BY sale_date) AS cumulative_sales
FROM sales_partition_aocs
ORDER BY product_id, sale_date
LIMIT 20;

-- Q11: Calculate the monthly sales growth rate for each product.
WITH monthly_sales AS (
    SELECT EXTRACT(YEAR FROM sale_date) AS sale_year, EXTRACT(MONTH FROM sale_date) AS sale_month, product_id, SUM(total_sales) AS monthly_sales
    FROM sales_partition_aocs
    GROUP BY sale_year, sale_month, product_id
)
SELECT ms1.product_id, ms1.sale_year, ms1.sale_month, ms1.monthly_sales, 
       (ms1.monthly_sales - COALESCE(ms2.monthly_sales, 0)) / COALESCE(ms2.monthly_sales, 1) AS growth_rate
FROM monthly_sales ms1
LEFT JOIN monthly_sales ms2 ON ms1.product_id = ms2.product_id AND ms1.sale_year = ms2.sale_year AND ms1.sale_month = ms2.sale_month + 1
ORDER BY product_id, sale_year, sale_month
LIMIT 20;

-- Q12: Determine the average discount applied per product.
SELECT product_id, AVG(discount) AS avg_discount
FROM sales_partition_aocs
GROUP BY product_id
ORDER BY avg_discount DESC
LIMIT 20;

-- Q13: Summarize total sales by customer.
SELECT customer_name, SUM(total_sales) AS total_sales
FROM sales_partition_aocs
GROUP BY customer_name
ORDER BY total_sales DESC
LIMIT 20;

-- Q14: Summarize product sales by month and year.
SELECT EXTRACT(YEAR FROM sale_date) AS sale_year, EXTRACT(MONTH FROM sale_date) AS sale_month, product_id, SUM(total_sales) AS monthly_sales
FROM sales_partition_aocs
GROUP BY sale_year, sale_month, product_id
ORDER BY sale_year, sale_month, monthly_sales DESC
LIMIT 20;

-- Q15: Summarize total sales by customer and product.
SELECT customer_name, product_id, SUM(total_sales) AS total_sales
FROM sales_partition_aocs
GROUP BY customer_name, product_id
ORDER BY total_sales DESC
LIMIT 20;
