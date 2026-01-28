CREATE SCHEMA cte_bug;

SET gp_default_storage_options = '';
SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

SET default_tablespace = '';

SET default_table_access_method = ao_column;

--
-- Name: catalog_sales; Type: TABLE; Schema: cte_bug; Owner: -
--
DROP TABLE IF EXISTS cte_bug.catalog_sales;

CREATE TABLE cte_bug.catalog_sales (
    cs_sold_date_sk integer,
    cs_sold_time_sk integer,
    cs_ship_date_sk integer,
    cs_bill_customer_sk integer,
    cs_bill_cdemo_sk integer,
    cs_bill_hdemo_sk integer,
    cs_bill_addr_sk integer,
    cs_ship_customer_sk integer,
    cs_ship_cdemo_sk integer,
    cs_ship_hdemo_sk integer,
    cs_ship_addr_sk integer,
    cs_call_center_sk integer,
    cs_catalog_page_sk integer,
    cs_ship_mode_sk integer,
    cs_warehouse_sk integer,
    cs_item_sk integer NOT NULL,
    cs_promo_sk integer,
    cs_order_number bigint NOT NULL,
    cs_quantity integer,
    cs_wholesale_cost numeric(7,2),
    cs_list_price numeric(7,2),
    cs_sales_price numeric(7,2),
    cs_ext_discount_amt numeric(7,2),
    cs_ext_sales_price numeric(7,2),
    cs_ext_wholesale_cost numeric(7,2),
    cs_ext_list_price numeric(7,2),
    cs_ext_tax numeric(7,2),
    cs_coupon_amt numeric(7,2),
    cs_ext_ship_cost numeric(7,2),
    cs_net_paid numeric(7,2),
    cs_net_paid_inc_tax numeric(7,2),
    cs_net_paid_inc_ship numeric(7,2),
    cs_net_paid_inc_ship_tax numeric(7,2),
    cs_net_profit numeric(7,2),
    COLUMN cs_sold_date_sk ENCODING (compresstype=zstd,compresslevel=5,blocksize=1048576),
    COLUMN cs_sold_time_sk ENCODING (compresstype=zstd,compresslevel=5,blocksize=1048576),
    COLUMN cs_ship_date_sk ENCODING (compresstype=zstd,compresslevel=5,blocksize=1048576),
    COLUMN cs_bill_customer_sk ENCODING (compresstype=zstd,compresslevel=5,blocksize=1048576),
    COLUMN cs_bill_cdemo_sk ENCODING (compresstype=zstd,compresslevel=5,blocksize=1048576),
    COLUMN cs_bill_hdemo_sk ENCODING (compresstype=zstd,compresslevel=5,blocksize=1048576),
    COLUMN cs_bill_addr_sk ENCODING (compresstype=zstd,compresslevel=5,blocksize=1048576),
    COLUMN cs_ship_customer_sk ENCODING (compresstype=zstd,compresslevel=5,blocksize=1048576),
    COLUMN cs_ship_cdemo_sk ENCODING (compresstype=zstd,compresslevel=5,blocksize=1048576),
    COLUMN cs_ship_hdemo_sk ENCODING (compresstype=zstd,compresslevel=5,blocksize=1048576),
    COLUMN cs_ship_addr_sk ENCODING (compresstype=zstd,compresslevel=5,blocksize=1048576),
    COLUMN cs_call_center_sk ENCODING (compresstype=zstd,compresslevel=5,blocksize=1048576),
    COLUMN cs_catalog_page_sk ENCODING (compresstype=zstd,compresslevel=5,blocksize=1048576),
    COLUMN cs_ship_mode_sk ENCODING (compresstype=zstd,compresslevel=5,blocksize=1048576),
    COLUMN cs_warehouse_sk ENCODING (compresstype=zstd,compresslevel=5,blocksize=1048576),
    COLUMN cs_item_sk ENCODING (compresstype=zstd,compresslevel=5,blocksize=1048576),
    COLUMN cs_promo_sk ENCODING (compresstype=zstd,compresslevel=5,blocksize=1048576),
    COLUMN cs_order_number ENCODING (compresstype=zstd,compresslevel=5,blocksize=1048576),
    COLUMN cs_quantity ENCODING (compresstype=zstd,compresslevel=5,blocksize=1048576),
    COLUMN cs_wholesale_cost ENCODING (compresstype=zstd,compresslevel=5,blocksize=1048576),
    COLUMN cs_list_price ENCODING (compresstype=zstd,compresslevel=5,blocksize=1048576),
    COLUMN cs_sales_price ENCODING (compresstype=zstd,compresslevel=5,blocksize=1048576),
    COLUMN cs_ext_discount_amt ENCODING (compresstype=zstd,compresslevel=5,blocksize=1048576),
    COLUMN cs_ext_sales_price ENCODING (compresstype=zstd,compresslevel=5,blocksize=1048576),
    COLUMN cs_ext_wholesale_cost ENCODING (compresstype=zstd,compresslevel=5,blocksize=1048576),
    COLUMN cs_ext_list_price ENCODING (compresstype=zstd,compresslevel=5,blocksize=1048576),
    COLUMN cs_ext_tax ENCODING (compresstype=zstd,compresslevel=5,blocksize=1048576),
    COLUMN cs_coupon_amt ENCODING (compresstype=zstd,compresslevel=5,blocksize=1048576),
    COLUMN cs_ext_ship_cost ENCODING (compresstype=zstd,compresslevel=5,blocksize=1048576),
    COLUMN cs_net_paid ENCODING (compresstype=zstd,compresslevel=5,blocksize=1048576),
    COLUMN cs_net_paid_inc_tax ENCODING (compresstype=zstd,compresslevel=5,blocksize=1048576),
    COLUMN cs_net_paid_inc_ship ENCODING (compresstype=zstd,compresslevel=5,blocksize=1048576),
    COLUMN cs_net_paid_inc_ship_tax ENCODING (compresstype=zstd,compresslevel=5,blocksize=1048576),
    COLUMN cs_net_profit ENCODING (compresstype=zstd,compresslevel=5,blocksize=1048576)
)
WITH (appendonly = true, orientation = column, compresstype=zstd, compresslevel='5', blocksize='1048576') DISTRIBUTED BY (cs_item_sk);


--
-- Name: customer; Type: TABLE; Schema: cte_bug; Owner: -
--

DROP TABLE IF EXISTS cte_bug.customer;

CREATE TABLE cte_bug.customer (
    c_customer_sk integer NOT NULL,
    c_customer_id character varying(16) NOT NULL,
    c_current_cdemo_sk integer,
    c_current_hdemo_sk integer,
    c_current_addr_sk integer,
    c_first_shipto_date_sk integer,
    c_first_sales_date_sk integer,
    c_salutation character varying(10),
    c_first_name character varying(20),
    c_last_name character varying(30),
    c_preferred_cust_flag character(1),
    c_birth_day integer,
    c_birth_month integer,
    c_birth_year integer,
    c_birth_country character varying(20),
    c_login character varying(13),
    c_email_address character varying(50),
    c_last_review_date integer,
    COLUMN c_customer_sk ENCODING (compresstype=zstd,compresslevel=5,blocksize=1048576),
    COLUMN c_customer_id ENCODING (compresstype=zstd,compresslevel=5,blocksize=1048576),
    COLUMN c_current_cdemo_sk ENCODING (compresstype=zstd,compresslevel=5,blocksize=1048576),
    COLUMN c_current_hdemo_sk ENCODING (compresstype=zstd,compresslevel=5,blocksize=1048576),
    COLUMN c_current_addr_sk ENCODING (compresstype=zstd,compresslevel=5,blocksize=1048576),
    COLUMN c_first_shipto_date_sk ENCODING (compresstype=zstd,compresslevel=5,blocksize=1048576),
    COLUMN c_first_sales_date_sk ENCODING (compresstype=zstd,compresslevel=5,blocksize=1048576),
    COLUMN c_salutation ENCODING (compresstype=zstd,compresslevel=5,blocksize=1048576),
    COLUMN c_first_name ENCODING (compresstype=zstd,compresslevel=5,blocksize=1048576),
    COLUMN c_last_name ENCODING (compresstype=zstd,compresslevel=5,blocksize=1048576),
    COLUMN c_preferred_cust_flag ENCODING (compresstype=zstd,compresslevel=5,blocksize=1048576),
    COLUMN c_birth_day ENCODING (compresstype=zstd,compresslevel=5,blocksize=1048576),
    COLUMN c_birth_month ENCODING (compresstype=zstd,compresslevel=5,blocksize=1048576),
    COLUMN c_birth_year ENCODING (compresstype=zstd,compresslevel=5,blocksize=1048576),
    COLUMN c_birth_country ENCODING (compresstype=zstd,compresslevel=5,blocksize=1048576),
    COLUMN c_login ENCODING (compresstype=zstd,compresslevel=5,blocksize=1048576),
    COLUMN c_email_address ENCODING (compresstype=zstd,compresslevel=5,blocksize=1048576),
    COLUMN c_last_review_date ENCODING (compresstype=zstd,compresslevel=5,blocksize=1048576)
)
WITH (appendonly = true, orientation = column, compresstype=zstd, compresslevel='5', blocksize='1048576') DISTRIBUTED BY (c_customer_sk);


--
-- Name: date_dim; Type: TABLE; Schema: cte_bug; Owner: -
--
DROP TABLE IF EXISTS cte_bug.date_dim;

CREATE TABLE cte_bug.date_dim (
    d_date_sk integer NOT NULL,
    d_date_id character varying(16) NOT NULL,
    d_date date,
    d_month_seq integer,
    d_week_seq integer,
    d_quarter_seq integer,
    d_year integer,
    d_dow integer,
    d_moy integer,
    d_dom integer,
    d_qoy integer,
    d_fy_year integer,
    d_fy_quarter_seq integer,
    d_fy_week_seq integer,
    d_day_name character varying(9),
    d_quarter_name character varying(6),
    d_holiday character(1),
    d_weekend character(1),
    d_following_holiday character(1),
    d_first_dom integer,
    d_last_dom integer,
    d_same_day_ly integer,
    d_same_day_lq integer,
    d_current_day character(1),
    d_current_week character(1),
    d_current_month character(1),
    d_current_quarter character(1),
    d_current_year character(1),
    COLUMN d_date_sk ENCODING (compresstype=zstd,compresslevel=5,blocksize=1048576),
    COLUMN d_date_id ENCODING (compresstype=zstd,compresslevel=5,blocksize=1048576),
    COLUMN d_date ENCODING (compresstype=zstd,compresslevel=5,blocksize=1048576),
    COLUMN d_month_seq ENCODING (compresstype=zstd,compresslevel=5,blocksize=1048576),
    COLUMN d_week_seq ENCODING (compresstype=zstd,compresslevel=5,blocksize=1048576),
    COLUMN d_quarter_seq ENCODING (compresstype=zstd,compresslevel=5,blocksize=1048576),
    COLUMN d_year ENCODING (compresstype=zstd,compresslevel=5,blocksize=1048576),
    COLUMN d_dow ENCODING (compresstype=zstd,compresslevel=5,blocksize=1048576),
    COLUMN d_moy ENCODING (compresstype=zstd,compresslevel=5,blocksize=1048576),
    COLUMN d_dom ENCODING (compresstype=zstd,compresslevel=5,blocksize=1048576),
    COLUMN d_qoy ENCODING (compresstype=zstd,compresslevel=5,blocksize=1048576),
    COLUMN d_fy_year ENCODING (compresstype=zstd,compresslevel=5,blocksize=1048576),
    COLUMN d_fy_quarter_seq ENCODING (compresstype=zstd,compresslevel=5,blocksize=1048576),
    COLUMN d_fy_week_seq ENCODING (compresstype=zstd,compresslevel=5,blocksize=1048576),
    COLUMN d_day_name ENCODING (compresstype=zstd,compresslevel=5,blocksize=1048576),
    COLUMN d_quarter_name ENCODING (compresstype=zstd,compresslevel=5,blocksize=1048576),
    COLUMN d_holiday ENCODING (compresstype=zstd,compresslevel=5,blocksize=1048576),
    COLUMN d_weekend ENCODING (compresstype=zstd,compresslevel=5,blocksize=1048576),
    COLUMN d_following_holiday ENCODING (compresstype=zstd,compresslevel=5,blocksize=1048576),
    COLUMN d_first_dom ENCODING (compresstype=zstd,compresslevel=5,blocksize=1048576),
    COLUMN d_last_dom ENCODING (compresstype=zstd,compresslevel=5,blocksize=1048576),
    COLUMN d_same_day_ly ENCODING (compresstype=zstd,compresslevel=5,blocksize=1048576),
    COLUMN d_same_day_lq ENCODING (compresstype=zstd,compresslevel=5,blocksize=1048576),
    COLUMN d_current_day ENCODING (compresstype=zstd,compresslevel=5,blocksize=1048576),
    COLUMN d_current_week ENCODING (compresstype=zstd,compresslevel=5,blocksize=1048576),
    COLUMN d_current_month ENCODING (compresstype=zstd,compresslevel=5,blocksize=1048576),
    COLUMN d_current_quarter ENCODING (compresstype=zstd,compresslevel=5,blocksize=1048576),
    COLUMN d_current_year ENCODING (compresstype=zstd,compresslevel=5,blocksize=1048576)
)
WITH (appendonly = true, orientation = column, compresstype=zstd, compresslevel='5', blocksize='1048576') DISTRIBUTED BY (d_date_sk);


--
-- Name: store_sales; Type: TABLE; Schema: cte_bug; Owner: -
--
DROP TABLE IF EXISTS cte_bug.store_sales;

CREATE TABLE cte_bug.store_sales (
    ss_sold_date_sk integer,
    ss_sold_time_sk integer,
    ss_item_sk integer NOT NULL,
    ss_customer_sk integer,
    ss_cdemo_sk integer,
    ss_hdemo_sk integer,
    ss_addr_sk integer,
    ss_store_sk integer,
    ss_promo_sk integer,
    ss_ticket_number bigint NOT NULL,
    ss_quantity integer,
    ss_wholesale_cost numeric(7,2),
    ss_list_price numeric(7,2),
    ss_sales_price numeric(7,2),
    ss_ext_discount_amt numeric(7,2),
    ss_ext_sales_price numeric(7,2),
    ss_ext_wholesale_cost numeric(7,2),
    ss_ext_list_price numeric(7,2),
    ss_ext_tax numeric(7,2),
    ss_coupon_amt numeric(7,2),
    ss_net_paid numeric(7,2),
    ss_net_paid_inc_tax numeric(7,2),
    ss_net_profit numeric(7,2),
    COLUMN ss_sold_date_sk ENCODING (compresstype=zstd,compresslevel=5,blocksize=1048576),
    COLUMN ss_sold_time_sk ENCODING (compresstype=zstd,compresslevel=5,blocksize=1048576),
    COLUMN ss_item_sk ENCODING (compresstype=zstd,compresslevel=5,blocksize=1048576),
    COLUMN ss_customer_sk ENCODING (compresstype=zstd,compresslevel=5,blocksize=1048576),
    COLUMN ss_cdemo_sk ENCODING (compresstype=zstd,compresslevel=5,blocksize=1048576),
    COLUMN ss_hdemo_sk ENCODING (compresstype=zstd,compresslevel=5,blocksize=1048576),
    COLUMN ss_addr_sk ENCODING (compresstype=zstd,compresslevel=5,blocksize=1048576),
    COLUMN ss_store_sk ENCODING (compresstype=zstd,compresslevel=5,blocksize=1048576),
    COLUMN ss_promo_sk ENCODING (compresstype=zstd,compresslevel=5,blocksize=1048576),
    COLUMN ss_ticket_number ENCODING (compresstype=zstd,compresslevel=5,blocksize=1048576),
    COLUMN ss_quantity ENCODING (compresstype=zstd,compresslevel=5,blocksize=1048576),
    COLUMN ss_wholesale_cost ENCODING (compresstype=zstd,compresslevel=5,blocksize=1048576),
    COLUMN ss_list_price ENCODING (compresstype=zstd,compresslevel=5,blocksize=1048576),
    COLUMN ss_sales_price ENCODING (compresstype=zstd,compresslevel=5,blocksize=1048576),
    COLUMN ss_ext_discount_amt ENCODING (compresstype=zstd,compresslevel=5,blocksize=1048576),
    COLUMN ss_ext_sales_price ENCODING (compresstype=zstd,compresslevel=5,blocksize=1048576),
    COLUMN ss_ext_wholesale_cost ENCODING (compresstype=zstd,compresslevel=5,blocksize=1048576),
    COLUMN ss_ext_list_price ENCODING (compresstype=zstd,compresslevel=5,blocksize=1048576),
    COLUMN ss_ext_tax ENCODING (compresstype=zstd,compresslevel=5,blocksize=1048576),
    COLUMN ss_coupon_amt ENCODING (compresstype=zstd,compresslevel=5,blocksize=1048576),
    COLUMN ss_net_paid ENCODING (compresstype=zstd,compresslevel=5,blocksize=1048576),
    COLUMN ss_net_paid_inc_tax ENCODING (compresstype=zstd,compresslevel=5,blocksize=1048576),
    COLUMN ss_net_profit ENCODING (compresstype=zstd,compresslevel=5,blocksize=1048576)
)
WITH (appendonly = true, orientation = column, compresstype=zstd, compresslevel='5', blocksize='1048576') DISTRIBUTED BY (ss_item_sk);


--
-- Name: web_sales; Type: TABLE; Schema: cte_bug; Owner: -
--
DROP TABLE IF EXISTS cte_bug.web_sales;

CREATE TABLE cte_bug.web_sales (
    ws_sold_date_sk integer,
    ws_sold_time_sk integer,
    ws_ship_date_sk integer,
    ws_item_sk integer NOT NULL,
    ws_bill_customer_sk integer,
    ws_bill_cdemo_sk integer,
    ws_bill_hdemo_sk integer,
    ws_bill_addr_sk integer,
    ws_ship_customer_sk integer,
    ws_ship_cdemo_sk integer,
    ws_ship_hdemo_sk integer,
    ws_ship_addr_sk integer,
    ws_web_page_sk integer,
    ws_web_site_sk integer,
    ws_ship_mode_sk integer,
    ws_warehouse_sk integer,
    ws_promo_sk integer,
    ws_order_number bigint NOT NULL,
    ws_quantity integer,
    ws_wholesale_cost numeric(7,2),
    ws_list_price numeric(7,2),
    ws_sales_price numeric(7,2),
    ws_ext_discount_amt numeric(7,2),
    ws_ext_sales_price numeric(7,2),
    ws_ext_wholesale_cost numeric(7,2),
    ws_ext_list_price numeric(7,2),
    ws_ext_tax numeric(7,2),
    ws_coupon_amt numeric(7,2),
    ws_ext_ship_cost numeric(7,2),
    ws_net_paid numeric(7,2),
    ws_net_paid_inc_tax numeric(7,2),
    ws_net_paid_inc_ship numeric(7,2),
    ws_net_paid_inc_ship_tax numeric(7,2),
    ws_net_profit numeric(7,2),
    COLUMN ws_sold_date_sk ENCODING (compresstype=zstd,compresslevel=5,blocksize=1048576),
    COLUMN ws_sold_time_sk ENCODING (compresstype=zstd,compresslevel=5,blocksize=1048576),
    COLUMN ws_ship_date_sk ENCODING (compresstype=zstd,compresslevel=5,blocksize=1048576),
    COLUMN ws_item_sk ENCODING (compresstype=zstd,compresslevel=5,blocksize=1048576),
    COLUMN ws_bill_customer_sk ENCODING (compresstype=zstd,compresslevel=5,blocksize=1048576),
    COLUMN ws_bill_cdemo_sk ENCODING (compresstype=zstd,compresslevel=5,blocksize=1048576),
    COLUMN ws_bill_hdemo_sk ENCODING (compresstype=zstd,compresslevel=5,blocksize=1048576),
    COLUMN ws_bill_addr_sk ENCODING (compresstype=zstd,compresslevel=5,blocksize=1048576),
    COLUMN ws_ship_customer_sk ENCODING (compresstype=zstd,compresslevel=5,blocksize=1048576),
    COLUMN ws_ship_cdemo_sk ENCODING (compresstype=zstd,compresslevel=5,blocksize=1048576),
    COLUMN ws_ship_hdemo_sk ENCODING (compresstype=zstd,compresslevel=5,blocksize=1048576),
    COLUMN ws_ship_addr_sk ENCODING (compresstype=zstd,compresslevel=5,blocksize=1048576),
    COLUMN ws_web_page_sk ENCODING (compresstype=zstd,compresslevel=5,blocksize=1048576),
    COLUMN ws_web_site_sk ENCODING (compresstype=zstd,compresslevel=5,blocksize=1048576),
    COLUMN ws_ship_mode_sk ENCODING (compresstype=zstd,compresslevel=5,blocksize=1048576),
    COLUMN ws_warehouse_sk ENCODING (compresstype=zstd,compresslevel=5,blocksize=1048576),
    COLUMN ws_promo_sk ENCODING (compresstype=zstd,compresslevel=5,blocksize=1048576),
    COLUMN ws_order_number ENCODING (compresstype=zstd,compresslevel=5,blocksize=1048576),
    COLUMN ws_quantity ENCODING (compresstype=zstd,compresslevel=5,blocksize=1048576),
    COLUMN ws_wholesale_cost ENCODING (compresstype=zstd,compresslevel=5,blocksize=1048576),
    COLUMN ws_list_price ENCODING (compresstype=zstd,compresslevel=5,blocksize=1048576),
    COLUMN ws_sales_price ENCODING (compresstype=zstd,compresslevel=5,blocksize=1048576),
    COLUMN ws_ext_discount_amt ENCODING (compresstype=zstd,compresslevel=5,blocksize=1048576),
    COLUMN ws_ext_sales_price ENCODING (compresstype=zstd,compresslevel=5,blocksize=1048576),
    COLUMN ws_ext_wholesale_cost ENCODING (compresstype=zstd,compresslevel=5,blocksize=1048576),
    COLUMN ws_ext_list_price ENCODING (compresstype=zstd,compresslevel=5,blocksize=1048576),
    COLUMN ws_ext_tax ENCODING (compresstype=zstd,compresslevel=5,blocksize=1048576),
    COLUMN ws_coupon_amt ENCODING (compresstype=zstd,compresslevel=5,blocksize=1048576),
    COLUMN ws_ext_ship_cost ENCODING (compresstype=zstd,compresslevel=5,blocksize=1048576),
    COLUMN ws_net_paid ENCODING (compresstype=zstd,compresslevel=5,blocksize=1048576),
    COLUMN ws_net_paid_inc_tax ENCODING (compresstype=zstd,compresslevel=5,blocksize=1048576),
    COLUMN ws_net_paid_inc_ship ENCODING (compresstype=zstd,compresslevel=5,blocksize=1048576),
    COLUMN ws_net_paid_inc_ship_tax ENCODING (compresstype=zstd,compresslevel=5,blocksize=1048576),
    COLUMN ws_net_profit ENCODING (compresstype=zstd,compresslevel=5,blocksize=1048576)
)
WITH (appendonly = true, orientation = column, compresstype=zstd, compresslevel='5', blocksize='1048576') DISTRIBUTED BY (ws_item_sk);


--
-- Apache Cloudberry database dump complete
--


-- 
-- Allow system table modifications
-- 
set allow_system_table_mods=true;

-- Table: catalog_sales
UPDATE pg_class
SET
	relpages = 1497467::int,
	reltuples = 720015040.0::real,
	relallvisible = 0::int
WHERE relname = 'catalog_sales' AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'cte_bug');

-- Table: date_dim
UPDATE pg_class
SET
	relpages = 40::int,
	reltuples = 73049.0::real,
	relallvisible = 0::int
WHERE relname = 'date_dim' AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'cte_bug');

-- Table: customer
UPDATE pg_class
SET
	relpages = 2870::int,
	reltuples = 2100000.0::real,
	relallvisible = 0::int
WHERE relname = 'customer' AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'cte_bug');

-- Table: store_sales
UPDATE pg_class
SET
	relpages = 1930653::int,
	reltuples = 1439977500.0::real,
	relallvisible = 0::int
WHERE relname = 'store_sales' AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'cte_bug');

-- Table: web_sales
UPDATE pg_class
SET
	relpages = 736204::int,
	reltuples = 360014100.0::real,
	relallvisible = 0::int
WHERE relname = 'web_sales' AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'cte_bug');

--
-- Table: catalog_sales, Attribute: cs_sold_date_sk
--
-- DELETE FROM pg_statistic WHERE starelid='cte_bug.catalog_sales'::regclass AND staattnum=1;
INSERT INTO pg_statistic VALUES (
	'cte_bug.catalog_sales'::regclass,
	1::smallint,
	False::boolean,
	0.0046333335::real,
	4::integer,
	1836.0::real,
	1::smallint,
	2::smallint,
	3::smallint,
	0::smallint,
	0::smallint,
	96::oid,
	97::oid,
	97::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	'{0.0017666667, 0.0015666666, 0.0015666666, 0.0015, 0.0014666667, 0.0014666667, 0.0014666667, 0.0014666667, 0.0014666667, 0.0014666667, 0.0014666667, 0.0014333334, 0.0014333334, 0.0014333334, 0.0014333334, 0.0014, 0.0014, 0.0014, 0.0014, 0.0013666666, 0.0013666666, 0.0013666666, 0.0013666666, 0.0013666666, 0.0013666666, 0.0013666666, 0.0013666666, 0.0013333333, 0.0013333333, 0.0013333333, 0.0013333333, 0.0013333333, 0.0013333333, 0.0013333333, 0.0013333333, 0.0013333333, 0.0013333333, 0.0013333333, 0.0013333333, 0.0013333333, 0.0013333333, 0.0013, 0.0013, 0.0013, 0.0013, 0.0013, 0.0013, 0.0013, 0.0013, 0.0012666667, 0.0012666667, 0.0012666667, 0.0012666667, 0.0012666667, 0.0012666667, 0.0012666667, 0.0012666667, 0.0012333334, 0.0012333334, 0.0012333334, 0.0012333334, 0.0012333334, 0.0012333334, 0.0012333334, 0.0012333334, 0.0012333334, 0.0012333334, 0.0012333334, 0.0012333334, 0.0012333334, 0.0012333334, 0.0012333334, 0.0012333334, 0.0012333334, 0.0012333334, 0.0012333334, 0.0012333334, 0.0012, 0.0012, 0.0012, 0.0012, 0.0012, 0.0012, 0.0012, 0.0012, 0.0012, 0.0012, 0.0012, 0.0012, 0.0012, 0.0011666666, 0.0011666666, 0.0011666666, 0.0011666666, 0.0011666666, 0.0011666666, 0.0011666666, 0.0011666666, 0.0011666666, 0.0011666666}'::real[],
	NULL::real[],
	'{0.054781195}'::real[],
	NULL::real[],
	NULL::real[],
	E'{"2452618","2451872","2451875","2451537","2451130","2451869","2451873","2451891","2451908","2452223","2452613","2451169","2451486","2451513","2451904","2451163","2451173","2451541","2451885","2451177","2451504","2451876","2451884","2452249","2452264","2452587","2452606","2451138","2451146","2451158","2451491","2451523","2451536","2451540","2451544","2451883","2451899","2452217","2452222","2452248","2452593","2451535","2451864","2451874","2451894","2451896","2452230","2452588","2452597","2451156","2451505","2451514","2452247","2452259","2452269","2452585","2452627","2451150","2451157","2451166","2451171","2451512","2451515","2451519","2451526","2451881","2451886","2451893","2452232","2452254","2452265","2452576","2452583","2452609","2452612","2452616","2452630","2451132","2451136","2451522","2451529","2451887","2452220","2452235","2452498","2452594","2452595","2452601","2452620","2452624","2451029","2451131","2451133","2451139","2451147","2451170","2451525","2451532","2451856","2451858"}'::int4[],
	E'{"2450815","2450843","2450872","2450902","2450927","2450954","2450982","2451010","2451032","2451043","2451055","2451066","2451078","2451090","2451102","2451116","2451125","2451142","2451153","2451165","2451181","2451211","2451241","2451266","2451295","2451323","2451348","2451375","2451397","2451409","2451421","2451434","2451446","2451458","2451471","2451483","2451494","2451506","2451520","2451539","2451563","2451589","2451616","2451645","2451670","2451696","2451724","2451754","2451768","2451782","2451795","2451807","2451818","2451831","2451842","2451853","2451865","2451879","2451898","2451910","2451933","2451960","2451990","2452016","2452042","2452069","2452097","2452122","2452135","2452147","2452158","2452169","2452182","2452195","2452207","2452218","2452231","2452242","2452255","2452267","2452278","2452309","2452340","2452369","2452396","2452424","2452450","2452480","2452497","2452509","2452521","2452533","2452544","2452556","2452568","2452582","2452599","2452610","2452625","2452636","2452654"}'::int4[],
	NULL::int4[],
	NULL::int4[],
	NULL::int4[]);

--
-- Table: catalog_sales, Attribute: cs_sold_time_sk
--
-- DELETE FROM pg_statistic WHERE starelid='cte_bug.catalog_sales'::regclass AND staattnum=2;
INSERT INTO pg_statistic VALUES (
	'cte_bug.catalog_sales'::regclass,
	2::smallint,
	False::boolean,
	0.005::real,
	4::integer,
	72297.0::real,
	1::smallint,
	2::smallint,
	3::smallint,
	0::smallint,
	0::smallint,
	96::oid,
	97::oid,
	97::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	'{0.0002}'::real[],
	NULL::real[],
	'{0.0112536065}'::real[],
	NULL::real[],
	NULL::real[],
	E'{"44417"}'::int4[],
	E'{"19","3608","5437","7239","9103","10773","12524","14298","15582","16852","17926","19155","20275","21514","22764","24007","25186","26042","26962","27910","28794","29659","30546","31449","32213","32885","33617","34346","35082","35807","36509","37180","37892","38557","39244","39936","40721","41398","42183","42868","43569","44109","44624","45170","45703","46274","46759","47492","48133","48850","49628","50310","51472","52776","53993","54839","55874","56759","57696","58416","59123","59800","60516","61221","61836","62429","62989","63592","64155","64738","65238","65703","66205","66700","67247","67791","68275","68782","69195","69611","70077","70485","70923","71369","71847","72304","72827","73342","73877","74404","74966","75510","76220","76902","77680","78437","79149","80343","81522","82791","86392"}'::int4[],
	NULL::int4[],
	NULL::int4[],
	NULL::int4[]);

--
-- Table: catalog_sales, Attribute: cs_ship_date_sk
--
-- DELETE FROM pg_statistic WHERE starelid='cte_bug.catalog_sales'::regclass AND staattnum=3;
INSERT INTO pg_statistic VALUES (
	'cte_bug.catalog_sales'::regclass,
	3::smallint,
	False::boolean,
	0.0052::real,
	4::integer,
	1914.0::real,
	1::smallint,
	2::smallint,
	3::smallint,
	0::smallint,
	0::smallint,
	96::oid,
	97::oid,
	97::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	'{0.0015333333, 0.0014, 0.0013666666, 0.0013666666, 0.0013, 0.0013, 0.0013, 0.0013, 0.0013, 0.0013, 0.0012666667, 0.0012666667, 0.0012333334, 0.0012333334, 0.0012333334, 0.0012, 0.0012, 0.0012, 0.0012, 0.0012, 0.0012, 0.0012, 0.0012, 0.0012, 0.0012, 0.0012, 0.0011666666, 0.0011666666, 0.0011666666, 0.0011666666, 0.0011666666, 0.0011666666, 0.0011333333, 0.0011333333, 0.0011333333, 0.0011333333, 0.0011333333, 0.0011333333, 0.0011333333, 0.0011333333, 0.0011333333, 0.0011333333, 0.0011333333, 0.0011333333, 0.0011333333, 0.0011333333, 0.0011333333, 0.0011, 0.0011, 0.0011, 0.0011, 0.0011, 0.0011, 0.0011, 0.0011, 0.0011, 0.0011, 0.0011, 0.0011, 0.0011, 0.0011, 0.0011, 0.0011, 0.0010666667, 0.0010666667, 0.0010666667, 0.0010666667, 0.0010666667, 0.0010666667, 0.0010666667, 0.0010666667, 0.0010666667, 0.0010666667, 0.0010666667, 0.0010666667, 0.0010333334, 0.0010333334, 0.0010333334, 0.0010333334, 0.0010333334, 0.0010333334, 0.0010333334, 0.0010333334, 0.0010333334, 0.0010333334, 0.0010333334, 0.0010333334, 0.0010333334, 0.0010333334, 0.0010333334, 0.001, 0.001, 0.001, 0.001, 0.001, 0.001, 0.001, 0.001, 0.001, 0.001}'::real[],
	NULL::real[],
	'{0.0545705}'::real[],
	NULL::real[],
	NULL::real[],
	E'{"2451918","2451182","2451209","2452278","2451544","2451907","2452252","2452639","2452648","2452659","2451207","2452590","2451205","2451541","2451559","2451183","2451549","2451909","2451910","2451911","2451933","2452260","2452262","2452624","2452634","2452637","2451906","2451914","2452233","2452242","2452265","2452270","2451121","2451180","2451192","2451526","2451565","2451575","2451896","2451897","2451899","2451912","2451936","2452245","2452618","2452626","2452653","2451161","2451173","2451174","2451191","2451478","2451528","2451555","2451915","2451927","2452261","2452269","2452281","2452290","2452629","2452633","2452643","2451162","2451164","2451185","2451548","2451550","2451584","2451929","2451940","2452236","2452283","2452608","2452654","2451165","2451168","2451481","2451511","2451517","2451553","2451582","2451889","2451917","2452294","2452632","2452641","2452642","2452647","2452660","2451123","2451178","2451184","2451204","2451491","2451512","2451513","2451516","2451520","2451538"}'::int4[],
	E'{"2450821","2450886","2450917","2450945","2450976","2451006","2451032","2451056","2451073","2451088","2451101","2451114","2451128","2451140","2451151","2451166","2451186","2451199","2451214","2451227","2451241","2451259","2451286","2451317","2451344","2451370","2451397","2451421","2451439","2451456","2451470","2451484","2451497","2451510","2451529","2451542","2451561","2451573","2451588","2451603","2451620","2451643","2451671","2451698","2451724","2451752","2451777","2451799","2451817","2451830","2451844","2451858","2451870","2451880","2451892","2451908","2451926","2451941","2451953","2451966","2451982","2452002","2452032","2452056","2452083","2452113","2452139","2452159","2452176","2452190","2452204","2452217","2452229","2452244","2452257","2452274","2452287","2452301","2452312","2452325","2452341","2452360","2452392","2452421","2452448","2452475","2452504","2452523","2452541","2452555","2452569","2452581","2452594","2452606","2452617","2452631","2452651","2452666","2452678","2452695","2452742"}'::int4[],
	NULL::int4[],
	NULL::int4[],
	NULL::int4[]);

--
-- Table: catalog_sales, Attribute: cs_bill_customer_sk
--
-- DELETE FROM pg_statistic WHERE starelid='cte_bug.catalog_sales'::regclass AND staattnum=4;
INSERT INTO pg_statistic VALUES (
	'cte_bug.catalog_sales'::regclass,
	4::smallint,
	False::boolean,
	0.0047333334::real,
	4::integer,
	2076691.0::real,
	2::smallint,
	3::smallint,
	0::smallint,
	0::smallint,
	0::smallint,
	97::oid,
	97::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	NULL::real[],
	'{-0.009644499}'::real[],
	NULL::real[],
	NULL::real[],
	NULL::real[],
	E'{"83","19255","38469","57334","77660","100353","121648","142864","164380","186732","206072","225376","246600","266629","288115","310326","332043","353860","375080","398568","418154","439646","461654","482840","506162","528559","549467","568302","588542","608968","630692","650223","671210","692175","713622","734326","754624","775800","798038","818056","841239","862315","882803","902700","923351","943590","963503","984723","1007978","1026125","1047160","1067477","1087789","1107171","1127082","1149889","1169435","1192194","1212249","1233291","1254464","1277232","1298034","1319408","1340687","1361652","1382794","1402582","1423476","1446047","1468226","1490417","1512333","1534704","1553629","1572989","1592634","1611613","1630695","1653622","1674063","1694178","1714594","1736071","1758421","1781336","1801193","1823164","1844186","1864368","1883410","1905171","1926138","1948282","1969988","1992903","2014079","2034696","2056865","2078611","2099787"}'::int4[],
	NULL::int4[],
	NULL::int4[],
	NULL::int4[],
	NULL::int4[]);

--
-- Table: catalog_sales, Attribute: cs_bill_cdemo_sk
--
-- DELETE FROM pg_statistic WHERE starelid='cte_bug.catalog_sales'::regclass AND staattnum=5;
INSERT INTO pg_statistic VALUES (
	'cte_bug.catalog_sales'::regclass,
	5::smallint,
	False::boolean,
	0.005133333::real,
	4::integer,
	2250439.0::real,
	2::smallint,
	3::smallint,
	0::smallint,
	0::smallint,
	0::smallint,
	97::oid,
	97::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	NULL::real[],
	'{0.0031504256}'::real[],
	NULL::real[],
	NULL::real[],
	NULL::real[],
	E'{"302","18541","37466","54463","73740","92424","112008","130967","151053","169555","190110","209507","228068","249282","268918","288605","307024","325774","343640","360630","381214","401158","419575","438762","458312","478977","498196","516757","537388","556845","575435","593167","612925","631535","649867","668933","688696","708123","729225","748554","766895","786505","806811","825215","844584","864169","883774","902048","921519","939325","959048","977470","995552","1013270","1032128","1049619","1069099","1089358","1107492","1124109","1143285","1161705","1180858","1201502","1221860","1240948","1262383","1281164","1299986","1317873","1338886","1358783","1378683","1398812","1417832","1435737","1456539","1476597","1496447","1515429","1535261","1554963","1573327","1591123","1612033","1630239","1650833","1669983","1688937","1707342","1727798","1746079","1764329","1783178","1803869","1823803","1842989","1862762","1881322","1901969","1920750"}'::int4[],
	NULL::int4[],
	NULL::int4[],
	NULL::int4[],
	NULL::int4[]);

--
-- Table: catalog_sales, Attribute: cs_bill_hdemo_sk
--
-- DELETE FROM pg_statistic WHERE starelid='cte_bug.catalog_sales'::regclass AND staattnum=6;
INSERT INTO pg_statistic VALUES (
	'cte_bug.catalog_sales'::regclass,
	6::smallint,
	False::boolean,
	0.0045666667::real,
	4::integer,
	7211.0::real,
	1::smallint,
	2::smallint,
	3::smallint,
	0::smallint,
	0::smallint,
	96::oid,
	97::oid,
	97::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	'{0.00043333333, 0.00043333333, 0.0004, 0.0004, 0.0004, 0.0004, 0.0004, 0.0004, 0.0004}'::real[],
	NULL::real[],
	'{0.000111478614}'::real[],
	NULL::real[],
	NULL::real[],
	E'{"952","3639","822","1878","3522","3566","4915","5053","5336"}'::int4[],
	E'{"1","69","138","207","278","358","430","510","587","655","728","798","869","942","1011","1079","1148","1221","1293","1363","1436","1515","1590","1658","1727","1796","1869","1943","2011","2089","2164","2238","2309","2383","2458","2534","2607","2674","2744","2820","2896","2971","3050","3121","3192","3259","3330","3406","3478","3554","3622","3693","3766","3837","3910","3986","4059","4136","4203","4276","4352","4423","4495","4568","4638","4711","4786","4851","4924","4995","5071","5139","5209","5275","5348","5419","5491","5562","5630","5702","5783","5855","5928","6005","6074","6145","6221","6287","6362","6437","6509","6581","6652","6718","6791","6859","6925","6992","7067","7135","7200"}'::int4[],
	NULL::int4[],
	NULL::int4[],
	NULL::int4[]);

--
-- Table: catalog_sales, Attribute: cs_bill_addr_sk
--
-- DELETE FROM pg_statistic WHERE starelid='cte_bug.catalog_sales'::regclass AND staattnum=7;
INSERT INTO pg_statistic VALUES (
	'cte_bug.catalog_sales'::regclass,
	7::smallint,
	False::boolean,
	0.0049666665::real,
	4::integer,
	1046873.0::real,
	2::smallint,
	3::smallint,
	0::smallint,
	0::smallint,
	0::smallint,
	97::oid,
	97::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	NULL::real[],
	'{-0.010069353}'::real[],
	NULL::real[],
	NULL::real[],
	NULL::real[],
	E'{"4","11125","22235","33070","44034","54814","66903","76551","86436","96499","107386","117375","127737","138201","149160","158550","169352","179553","191448","202662","213153","223343","232616","242533","253489","263641","274314","284608","294503","305542","316693","327046","337213","348029","358708","369038","379092","389216","399404","411658","421481","431880","442318","452462","463687","473855","484589","494142","504812","515093","525984","536447","546801","557909","568809","579887","590134","600989","611895","621827","631620","641611","653140","664172","674648","685363","696406","706802","717628","727362","737815","748416","758649","768984","779629","790315","800549","811382","821988","832542","842239","852925","861542","872217","882338","893425","903652","914043","925124","935145","946703","957135","966844","978263","987914","998584","1009037","1019530","1030620","1041099","1049900"}'::int4[],
	NULL::int4[],
	NULL::int4[],
	NULL::int4[],
	NULL::int4[]);

--
-- Table: catalog_sales, Attribute: cs_ship_customer_sk
--
-- DELETE FROM pg_statistic WHERE starelid='cte_bug.catalog_sales'::regclass AND staattnum=8;
INSERT INTO pg_statistic VALUES (
	'cte_bug.catalog_sales'::regclass,
	8::smallint,
	False::boolean,
	0.0044::real,
	4::integer,
	1988803.0::real,
	2::smallint,
	3::smallint,
	0::smallint,
	0::smallint,
	0::smallint,
	97::oid,
	97::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	NULL::real[],
	'{-0.012593937}'::real[],
	NULL::real[],
	NULL::real[],
	NULL::real[],
	E'{"112","19457","38552","56966","77106","98752","120684","141455","163448","185974","205916","225714","246798","267145","288778","309669","331415","352688","373910","396777","417012","437984","458412","480601","503081","525455","546630","566430","585396","605761","627797","647297","667174","689078","710793","730789","751531","771586","793233","812933","836114","858314","878748","899241","919733","940900","960199","982017","1005759","1024900","1046467","1067416","1087759","1107005","1127271","1149614","1169010","1191761","1212083","1233029","1254014","1275851","1297280","1318761","1339872","1360476","1383115","1402788","1422877","1446083","1468587","1490686","1513016","1535950","1556692","1575347","1593894","1613402","1634582","1657027","1677173","1698221","1719408","1739440","1762772","1784881","1805037","1826722","1847883","1868042","1887771","1909652","1929728","1951115","1972152","1994703","2015691","2036799","2059398","2079650","2099910"}'::int4[],
	NULL::int4[],
	NULL::int4[],
	NULL::int4[],
	NULL::int4[]);

--
-- Table: catalog_sales, Attribute: cs_ship_cdemo_sk
--
-- DELETE FROM pg_statistic WHERE starelid='cte_bug.catalog_sales'::regclass AND staattnum=9;
INSERT INTO pg_statistic VALUES (
	'cte_bug.catalog_sales'::regclass,
	9::smallint,
	False::boolean,
	0.0047::real,
	4::integer,
	2111792.0::real,
	2::smallint,
	3::smallint,
	0::smallint,
	0::smallint,
	0::smallint,
	97::oid,
	97::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	NULL::real[],
	'{0.0029372792}'::real[],
	NULL::real[],
	NULL::real[],
	NULL::real[],
	E'{"302","18991","38051","55457","73906","92142","111203","130054","150524","168838","188615","208305","226780","247275","267002","286332","305964","323539","341623","359088","378908","398275","417006","435648","456039","475587","494530","514681","534729","554637","572188","589663","608849","627139","645797","665116","684344","704227","724342","743953","761704","781964","800445","819537","838906","859432","878743","896384","916344","935630","954106","974012","992877","1010876","1030092","1047512","1067272","1087763","1106244","1123512","1143152","1162252","1181423","1201399","1221079","1241127","1262335","1281211","1300282","1317917","1338913","1359409","1379678","1399509","1419706","1437833","1458212","1478319","1498253","1517308","1537216","1555785","1574809","1593715","1614555","1632477","1652767","1671255","1689246","1707832","1727015","1745671","1764329","1783647","1803713","1823176","1843528","1863426","1881885","1901131","1920750"}'::int4[],
	NULL::int4[],
	NULL::int4[],
	NULL::int4[],
	NULL::int4[]);

--
-- Table: catalog_sales, Attribute: cs_ship_hdemo_sk
--
-- DELETE FROM pg_statistic WHERE starelid='cte_bug.catalog_sales'::regclass AND staattnum=10;
INSERT INTO pg_statistic VALUES (
	'cte_bug.catalog_sales'::regclass,
	10::smallint,
	False::boolean,
	0.0045::real,
	4::integer,
	7216.0::real,
	1::smallint,
	2::smallint,
	3::smallint,
	0::smallint,
	0::smallint,
	96::oid,
	97::oid,
	97::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	'{0.00046666668, 0.00043333333, 0.00043333333, 0.0004, 0.0004, 0.0004, 0.0004, 0.0004, 0.0004}'::real[],
	NULL::real[],
	'{-0.0024474491}'::real[],
	NULL::real[],
	NULL::real[],
	E'{"3639","952","4257","822","1910","4081","4594","4618","4915"}'::int4[],
	E'{"1","73","142","207","280","358","430","508","582","649","721","795","869","944","1014","1083","1154","1229","1300","1373","1444","1523","1596","1662","1734","1808","1878","1946","2015","2092","2166","2242","2314","2390","2466","2538","2613","2681","2754","2829","2904","2980","3057","3130","3199","3265","3336","3409","3484","3555","3618","3692","3763","3836","3911","3985","4055","4135","4203","4280","4359","4429","4500","4572","4647","4723","4793","4861","4928","5004","5075","5142","5214","5279","5349","5422","5492","5565","5633","5704","5780","5856","5929","6005","6075","6144","6216","6286","6361","6433","6507","6577","6650","6717","6786","6857","6924","6990","7066","7134","7200"}'::int4[],
	NULL::int4[],
	NULL::int4[],
	NULL::int4[]);

--
-- Table: catalog_sales, Attribute: cs_ship_addr_sk
--
-- DELETE FROM pg_statistic WHERE starelid='cte_bug.catalog_sales'::regclass AND staattnum=11;
INSERT INTO pg_statistic VALUES (
	'cte_bug.catalog_sales'::regclass,
	11::smallint,
	False::boolean,
	0.0044::real,
	4::integer,
	1106888.0::real,
	2::smallint,
	3::smallint,
	0::smallint,
	0::smallint,
	0::smallint,
	97::oid,
	97::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	NULL::real[],
	'{-0.0052251625}'::real[],
	NULL::real[],
	NULL::real[],
	NULL::real[],
	E'{"4","10372","21281","32604","43275","54281","66191","75977","85966","96475","106785","117107","127166","137473","148549","158449","169288","179404","191400","202566","212568","223050","231983","242203","252330","262987","273206","284036","293651","303573","314592","325696","336019","346020","357411","367567","377578","387371","397564","409384","419885","430219","441268","451283","462534","472729","482897","493068","503727","514241","524681","535657","546641","557534","568161","579186","589218","600063","610499","620727","630611","640167","650891","661771","672763","683715","694487","704971","715589","725727","735602","745905","755951","765867","776755","787045","798005","808059","818665","829654","840229","849950","859637","870300","880895","892234","902478","912694","923652","933902","945183","955649","965499","976354","986841","997088","1007946","1018197","1029945","1040884","1049912"}'::int4[],
	NULL::int4[],
	NULL::int4[],
	NULL::int4[],
	NULL::int4[]);

--
-- Table: catalog_sales, Attribute: cs_call_center_sk
--
-- DELETE FROM pg_statistic WHERE starelid='cte_bug.catalog_sales'::regclass AND staattnum=12;
INSERT INTO pg_statistic VALUES (
	'cte_bug.catalog_sales'::regclass,
	12::smallint,
	False::boolean,
	0.0049::real,
	4::integer,
	6.0::real,
	1::smallint,
	3::smallint,
	0::smallint,
	0::smallint,
	0::smallint,
	96::oid,
	97::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	'{0.32786667, 0.1994, 0.13533333, 0.13273333, 0.1306, 0.06916667}'::real[],
	'{0.21526349}'::real[],
	NULL::real[],
	NULL::real[],
	NULL::real[],
	E'{"1","2","3","5","4","6"}'::int4[],
	NULL::int4[],
	NULL::int4[],
	NULL::int4[],
	NULL::int4[]);

--
-- Table: catalog_sales, Attribute: cs_catalog_page_sk
--
-- DELETE FROM pg_statistic WHERE starelid='cte_bug.catalog_sales'::regclass AND staattnum=13;
INSERT INTO pg_statistic VALUES (
	'cte_bug.catalog_sales'::regclass,
	13::smallint,
	False::boolean,
	0.0049333335::real,
	4::integer,
	5614.0::real,
	1::smallint,
	2::smallint,
	3::smallint,
	0::smallint,
	0::smallint,
	96::oid,
	97::oid,
	97::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	'{0.0012666667, 0.0012666667, 0.0012333334, 0.0012, 0.0012, 0.0011666666, 0.0011333333, 0.0011333333, 0.0011, 0.0011, 0.0011, 0.0010666667, 0.0010666667, 0.0010666667, 0.0010666667, 0.0010666667, 0.0010666667, 0.0010666667, 0.0010666667, 0.0010333334, 0.0010333334, 0.0010333334, 0.0010333334, 0.001, 0.001, 0.001, 0.001, 0.001, 0.001, 0.001, 0.001, 0.001, 0.001, 0.001, 0.001, 0.001, 0.001, 0.001, 0.0009666667, 0.0009666667, 0.0009666667, 0.0009666667, 0.0009666667, 0.0009666667, 0.0009666667, 0.0009666667, 0.0009666667, 0.0009666667, 0.0009666667, 0.0009666667, 0.0009666667, 0.00093333336, 0.00093333336, 0.00093333336, 0.00093333336, 0.00093333336, 0.00093333336, 0.00093333336, 0.00093333336, 0.00093333336, 0.00093333336, 0.00093333336, 0.00093333336, 0.00093333336, 0.00093333336, 0.00093333336, 0.00093333336, 0.00093333336, 0.00093333336, 0.00093333336, 0.00093333336, 0.00093333336, 0.00093333336, 0.00093333336, 0.00093333336, 0.00093333336, 0.00093333336, 0.00093333336, 0.00093333336, 0.00093333336, 0.0009, 0.0009, 0.0009, 0.0009, 0.0009, 0.0009, 0.0009, 0.0009, 0.0009, 0.0009, 0.0009, 0.0009, 0.0009, 0.0009, 0.0009, 0.0009, 0.0009, 0.0009, 0.0009, 0.0009}'::real[],
	NULL::real[],
	'{0.050873112}'::real[],
	NULL::real[],
	NULL::real[],
	E'{"164","5975","2104","135","4000","7906","2144","5962","5966","7944","7992","2111","2149","5954","6016","6032","6043","7900","7908","2053","2075","4051","7954","150","151","160","183","186","2062","2090","2114","4010","4052","4084","5959","6025","7929","7941","116","117","194","2060","2119","4076","4094","5964","5999","6011","6019","7917","7973","118","120","129","163","190","212","2077","2078","2091","2093","2148","2156","4047","4050","4068","4097","5979","6003","6007","6021","6034","7890","7909","7919","7921","7932","7938","7953","7960","141","157","176","182","214","2118","4016","4028","4053","4065","4073","4086","5961","6010","7911","7914","7922","7923","7936","7956"}'::int4[],
	E'{"1","29","53","79","106","124","139","154","172","189","205","230","290","340","380","413","614","843","1009","1150","1945","1972","1996","2020","2047","2067","2083","2099","2115","2131","2145","2168","2227","2279","2318","2361","2605","2813","2989","3129","3896","3920","3945","3968","3994","4009","4024","4038","4056","4070","4085","4101","4153","4215","4252","4289","4400","4718","4887","5022","5155","5852","5878","5903","5927","5946","5963","5980","5992","6006","6026","6040","6078","6144","6189","6232","6381","6650","6829","6974","7101","7800","7825","7851","7879","7895","7913","7935","7955","7971","7986","8023","8080","8130","8168","8207","8515","8701","8861","8977","9827"}'::int4[],
	NULL::int4[],
	NULL::int4[],
	NULL::int4[]);

--
-- Table: catalog_sales, Attribute: cs_ship_mode_sk
--
-- DELETE FROM pg_statistic WHERE starelid='cte_bug.catalog_sales'::regclass AND staattnum=14;
INSERT INTO pg_statistic VALUES (
	'cte_bug.catalog_sales'::regclass,
	14::smallint,
	False::boolean,
	0.004766667::real,
	4::integer,
	20.0::real,
	1::smallint,
	3::smallint,
	0::smallint,
	0::smallint,
	0::smallint,
	96::oid,
	97::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	'{0.0517, 0.051333334, 0.0507, 0.050466668, 0.0504, 0.050166667, 0.050166667, 0.050066665, 0.049966667, 0.0499, 0.0499, 0.049733333, 0.049333334, 0.0493, 0.049133334, 0.049066667, 0.049066667, 0.04853333, 0.0485, 0.0478}'::real[],
	'{0.05016106}'::real[],
	NULL::real[],
	NULL::real[],
	NULL::real[],
	E'{"10","4","5","15","7","14","20","12","9","3","6","2","1","18","8","17","19","16","13","11"}'::int4[],
	NULL::int4[],
	NULL::int4[],
	NULL::int4[],
	NULL::int4[]);

--
-- Table: catalog_sales, Attribute: cs_warehouse_sk
--
-- DELETE FROM pg_statistic WHERE starelid='cte_bug.catalog_sales'::regclass AND staattnum=15;
INSERT INTO pg_statistic VALUES (
	'cte_bug.catalog_sales'::regclass,
	15::smallint,
	False::boolean,
	0.0047::real,
	4::integer,
	5.0::real,
	1::smallint,
	3::smallint,
	0::smallint,
	0::smallint,
	0::smallint,
	96::oid,
	97::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	'{0.2037, 0.20163333, 0.19836667, 0.19753334, 0.19406667}'::real[],
	'{0.20946348}'::real[],
	NULL::real[],
	NULL::real[],
	NULL::real[],
	E'{"5","4","2","3","1"}'::int4[],
	NULL::int4[],
	NULL::int4[],
	NULL::int4[],
	NULL::int4[]);

--
-- Table: catalog_sales, Attribute: cs_item_sk
--
-- DELETE FROM pg_statistic WHERE starelid='cte_bug.catalog_sales'::regclass AND staattnum=16;
INSERT INTO pg_statistic VALUES (
	'cte_bug.catalog_sales'::regclass,
	16::smallint,
	False::boolean,
	0.0::real,
	4::integer,
	24934.0::real,
	1::smallint,
	2::smallint,
	3::smallint,
	0::smallint,
	0::smallint,
	96::oid,
	97::oid,
	97::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	'{0.00026666667, 0.00026666667, 0.00026666667, 0.00026666667, 0.00026666667, 0.00026666667, 0.00026666667, 0.00026666667, 0.00026666667, 0.00026666667, 0.00023333334, 0.00023333334, 0.00023333334, 0.00023333334, 0.00023333334, 0.00023333334, 0.00023333334, 0.00023333334, 0.00023333334, 0.00023333334, 0.00023333334, 0.00023333334, 0.00023333334, 0.00023333334, 0.00023333334, 0.00023333334, 0.00023333334, 0.00023333334, 0.00023333334, 0.00023333334, 0.00023333334, 0.00023333334, 0.00023333334, 0.00023333334, 0.00023333334, 0.00023333334, 0.00023333334}'::real[],
	NULL::real[],
	'{0.00087319454}'::real[],
	NULL::real[],
	NULL::real[],
	E'{"2149","4123","7117","12451","15001","19603","20557","21523","22867","27235","55","841","2348","2545","4105","4159","4441","4843","5509","7513","7747","8827","9289","12031","13417","15541","15709","16730","18115","18805","18997","19495","20083","21073","23167","24679","26641"}'::int4[],
	E'{"1","320","583","835","1138","1406","1673","1962","2264","2558","2845","3127","3446","3749","4031","4330","4573","4877","5173","5433","5703","5947","6237","6506","6775","7040","7307","7579","7873","8136","8404","8707","8984","9250","9532","9833","10123","10420","10678","10969","11250","11553","11826","12095","12364","12667","12956","13219","13486","13741","14042","14309","14602","14871","15161","15410","15665","15939","16212","16498","16763","17059","17351","17654","17945","18229","18514","18782","19059","19348","19609","19913","20198","20471","20743","21035","21331","21597","21883","22148","22442","22712","22966","23269","23567","23848","24106","24394","24668","24951","25221","25508","25796","26095","26365","26646","26927","27195","27461","27722","28000"}'::int4[],
	NULL::int4[],
	NULL::int4[],
	NULL::int4[]);

--
-- Table: catalog_sales, Attribute: cs_promo_sk
--
-- DELETE FROM pg_statistic WHERE starelid='cte_bug.catalog_sales'::regclass AND staattnum=17;
INSERT INTO pg_statistic VALUES (
	'cte_bug.catalog_sales'::regclass,
	17::smallint,
	False::boolean,
	0.0049333335::real,
	4::integer,
	357.0::real,
	1::smallint,
	2::smallint,
	3::smallint,
	0::smallint,
	0::smallint,
	96::oid,
	97::oid,
	97::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	'{0.0039333333, 0.0036666666, 0.0036333334, 0.0035666667, 0.0035, 0.0035, 0.0035, 0.0035, 0.0034666667}'::real[],
	NULL::real[],
	'{0.012315235}'::real[],
	NULL::real[],
	NULL::real[],
	E'{"305","89","51","343","11","49","195","258","355"}'::int4[],
	E'{"1","4","8","12","16","19","23","26","30","33","37","40","44","47","53","56","60","63","67","70","74","78","81","85","88","92","96","99","102","106","109","113","116","120","123","127","130","134","137","141","144","148","151","155","158","162","165","168","172","175","179","182","186","189","193","197","201","204","208","211","215","218","221","225","228","232","235","239","242","246","249","253","256","261","264","268","271","275","278","282","285","289","293","296","299","303","307","311","314","318","321","325","328","331","335","339","342","346","350","353","357"}'::int4[],
	NULL::int4[],
	NULL::int4[],
	NULL::int4[]);

--
-- Table: catalog_sales, Attribute: cs_order_number
--
-- DELETE FROM pg_statistic WHERE starelid='cte_bug.catalog_sales'::regclass AND staattnum=18;
INSERT INTO pg_statistic VALUES (
	'cte_bug.catalog_sales'::regclass,
	18::smallint,
	False::boolean,
	0.0::real,
	8::integer,
	-0.23808314::real,
	2::smallint,
	3::smallint,
	0::smallint,
	0::smallint,
	0::smallint,
	412::oid,
	412::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	NULL::real[],
	'{0.05413773}'::real[],
	NULL::real[],
	NULL::real[],
	NULL::real[],
	E'{"1093","796094","1645931","2488328","3262945","4034487","4856771","5589623","6321306","7127852","7844455","8614283","9422592","10337715","11178214","11967077","12806598","13604549","14347663","15135994","15897089","16779779","17630658","18360193","19209296","19976804","20750598","21564225","22330137","23141252","23995273","24745890","25625704","26406125","27188856","28177645","29033724","29834605","30623466","31397341","32181036","32930216","33726353","34526912","35320855","36048980","36922780","37717474","38626712","39479583","40217298","41019974","41831233","42654275","43505081","44324401","44976357","45709232","46482483","47262545","48002145","48800151","49567190","50315617","51133372","51910262","52699539","53463422","54279944","55035406","55746486","56570321","57449909","58231103","58998945","59766115","60653209","61448393","62271704","63090076","63926111","64814052","65684644","66478632","67319176","68037038","68897841","69676370","70424558","71249378","72010418","72811451","73574543","74362921","75127096","75960745","76739924","77534318","78299599","79159205","79998254"}'::int8[],
	NULL::int8[],
	NULL::int8[],
	NULL::int8[],
	NULL::int8[]);

--
-- Table: catalog_sales, Attribute: cs_quantity
--
-- DELETE FROM pg_statistic WHERE starelid='cte_bug.catalog_sales'::regclass AND staattnum=19;
INSERT INTO pg_statistic VALUES (
	'cte_bug.catalog_sales'::regclass,
	19::smallint,
	False::boolean,
	0.0044::real,
	4::integer,
	100.0::real,
	1::smallint,
	3::smallint,
	0::smallint,
	0::smallint,
	0::smallint,
	96::oid,
	97::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	'{0.0111, 0.0111, 0.011, 0.011, 0.0109, 0.010833333, 0.010833333, 0.010833333, 0.0108, 0.010666667, 0.010666667, 0.0106, 0.010566667, 0.010566667, 0.010533334, 0.010533334, 0.010533334, 0.0105, 0.010466667, 0.010433333, 0.010433333, 0.010366667, 0.010366667, 0.010366667, 0.010366667, 0.010366667, 0.010333333, 0.0103, 0.0103, 0.010266666, 0.010266666, 0.010266666, 0.0102, 0.0102, 0.0102, 0.0102, 0.0101666665, 0.0101666665, 0.0101666665, 0.0101333335, 0.0101333335, 0.0101, 0.0101, 0.010066667, 0.010033334, 0.010033334, 0.010033334, 0.01, 0.01, 0.009966667, 0.009966667, 0.009933333, 0.009933333, 0.009933333, 0.009933333, 0.009933333, 0.0099, 0.0099, 0.009866667, 0.009866667, 0.009833333, 0.009833333, 0.009833333, 0.009833333, 0.0098, 0.0098, 0.009766666, 0.009766666, 0.009733333, 0.009733333, 0.0097, 0.0097, 0.0097, 0.009666666, 0.009666666, 0.009666666, 0.0096, 0.009566667, 0.009533334, 0.009533334, 0.0095, 0.0095, 0.0095, 0.009466667, 0.009333333, 0.009266667, 0.009266667, 0.009233333, 0.009233333, 0.0092, 0.009133333, 0.009133333, 0.0091, 0.008933334, 0.008866667, 0.008833333, 0.0088, 0.0087, 0.0084, 0.0084}'::real[],
	'{0.009663152}'::real[],
	NULL::real[],
	NULL::real[],
	NULL::real[],
	E'{"41","52","9","44","27","58","86","89","77","36","80","17","16","42","45","46","83","15","91","12","14","57","67","68","74","96","84","25","64","28","71","82","11","56","60","98","1","6","23","7","95","61","94","47","5","69","72","3","48","70","92","34","54","76","79","81","32","38","62","78","2","13","18","55","37","40","75","100","22","97","4","33","51","8","35","73","88","24","49","87","21","26","53","66","99","39","65","20","93","30","43","90","29","63","85","59","19","50","10","31"}'::int4[],
	NULL::int4[],
	NULL::int4[],
	NULL::int4[],
	NULL::int4[]);

--
-- Table: catalog_sales, Attribute: cs_wholesale_cost
--
-- DELETE FROM pg_statistic WHERE starelid='cte_bug.catalog_sales'::regclass AND staattnum=20;
INSERT INTO pg_statistic VALUES (
	'cte_bug.catalog_sales'::regclass,
	20::smallint,
	False::boolean,
	0.0042333333::real,
	6::integer,
	9853.0::real,
	1::smallint,
	2::smallint,
	3::smallint,
	0::smallint,
	0::smallint,
	1752::oid,
	1754::oid,
	1754::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	'{0.0004, 0.00036666667, 0.00036666667, 0.00036666667, 0.00036666667, 0.00036666667, 0.00036666667, 0.00033333333, 0.00033333333, 0.00033333333, 0.00033333333, 0.00033333333, 0.00033333333, 0.00033333333}'::real[],
	NULL::real[],
	'{-0.00037631093}'::real[],
	NULL::real[],
	NULL::real[],
	E'{"72.49","24.31","33.82","41.38","61.68","64.83","90.44","10.81","27.94","34.69","63.73","87.62","93.76","94.23"}'::numeric[],
	E'{"1.00","1.97","2.96","3.92","4.94","5.97","7.01","7.93","8.86","9.79","10.76","11.74","12.69","13.67","14.71","15.61","16.55","17.55","18.59","19.64","20.69","21.72","22.70","23.70","24.74","25.76","26.74","27.83","28.84","29.88","30.96","31.96","32.90","33.95","34.88","35.82","36.86","37.79","38.78","39.75","40.65","41.59","42.57","43.46","44.53","45.59","46.66","47.73","48.74","49.72","50.69","51.60","52.56","53.41","54.44","55.38","56.44","57.38","58.32","59.33","60.46","61.51","62.59","63.51","64.50","65.52","66.49","67.40","68.34","69.30","70.25","71.22","72.19","73.17","74.18","75.21","76.17","77.18","78.10","79.08","80.07","81.02","81.93","82.86","83.87","84.82","85.83","86.83","87.81","88.79","89.69","90.75","91.71","92.69","93.69","94.73","95.80","96.89","97.92","99.02","100.00"}'::numeric[],
	NULL::numeric[],
	NULL::numeric[],
	NULL::numeric[]);

--
-- Table: catalog_sales, Attribute: cs_list_price
--
-- DELETE FROM pg_statistic WHERE starelid='cte_bug.catalog_sales'::regclass AND staattnum=21;
INSERT INTO pg_statistic VALUES (
	'cte_bug.catalog_sales'::regclass,
	21::smallint,
	False::boolean,
	0.0047333334::real,
	6::integer,
	23623.0::real,
	1::smallint,
	2::smallint,
	3::smallint,
	0::smallint,
	0::smallint,
	1752::oid,
	1754::oid,
	1754::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	'{0.00026666667, 0.00026666667, 0.00026666667, 0.00026666667, 0.00026666667, 0.00026666667, 0.00026666667, 0.00026666667}'::real[],
	NULL::real[],
	'{0.0011046942}'::real[],
	NULL::real[],
	NULL::real[],
	E'{"11.59","52.87","62.87","64.43","87.47","95.88","96.45","99.51"}'::numeric[],
	E'{"1.00","3.51","5.35","7.23","9.15","10.91","12.55","14.37","16.10","17.87","19.59","21.31","23.22","24.95","26.59","28.36","30.12","31.92","33.70","35.45","37.37","39.21","40.96","42.63","44.39","46.23","48.04","49.97","52.04","53.81","55.47","57.35","59.13","60.99","62.98","64.76","66.54","68.45","70.35","72.20","74.06","75.90","77.63","79.47","81.31","83.10","84.84","86.86","88.69","90.45","92.15","94.08","95.99","97.69","99.41","101.23","103.18","104.97","107.17","108.94","110.89","112.85","114.87","116.76","119.10","121.18","123.40","125.73","128.23","130.50","132.97","135.31","138.15","140.60","143.29","145.79","148.51","151.34","154.23","157.06","160.08","163.23","166.75","170.63","174.29","177.79","181.55","185.86","189.57","193.79","198.05","202.87","207.95","213.14","219.21","225.93","233.84","241.92","252.53","265.43","299.64"}'::numeric[],
	NULL::numeric[],
	NULL::numeric[],
	NULL::numeric[]);

--
-- Table: catalog_sales, Attribute: cs_sales_price
--
-- DELETE FROM pg_statistic WHERE starelid='cte_bug.catalog_sales'::regclass AND staattnum=22;
INSERT INTO pg_statistic VALUES (
	'cte_bug.catalog_sales'::regclass,
	22::smallint,
	False::boolean,
	0.0046666665::real,
	6::integer,
	14418.0::real,
	1::smallint,
	2::smallint,
	3::smallint,
	0::smallint,
	0::smallint,
	1752::oid,
	1754::oid,
	1754::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	'{0.009733333, 0.0005, 0.00046666668, 0.00046666668, 0.00046666668, 0.00046666668, 0.00046666668, 0.00046666668, 0.00043333333, 0.00043333333, 0.00043333333, 0.00043333333, 0.00043333333, 0.00043333333, 0.00043333333, 0.00043333333, 0.00043333333, 0.00043333333, 0.00043333333, 0.00043333333, 0.0004, 0.0004, 0.0004, 0.0004, 0.0004, 0.0004, 0.0004, 0.0004, 0.0004, 0.0004, 0.0004, 0.0004, 0.0004, 0.0004, 0.0004, 0.00036666667, 0.00036666667, 0.00036666667, 0.00036666667, 0.00036666667, 0.00036666667, 0.00036666667, 0.00036666667, 0.00036666667, 0.00036666667, 0.00036666667, 0.00036666667, 0.00036666667, 0.00036666667, 0.00036666667, 0.00036666667, 0.00036666667, 0.00036666667, 0.00036666667, 0.00036666667, 0.00036666667, 0.00036666667, 0.00036666667, 0.00036666667, 0.00036666667, 0.00036666667, 0.00036666667, 0.00036666667, 0.00036666667, 0.00036666667, 0.00036666667, 0.00036666667, 0.00036666667, 0.00036666667, 0.00036666667, 0.00033333333, 0.00033333333, 0.00033333333, 0.00033333333, 0.00033333333, 0.00033333333, 0.00033333333, 0.00033333333, 0.00033333333, 0.00033333333, 0.00033333333, 0.00033333333, 0.00033333333, 0.00033333333, 0.00033333333, 0.00033333333, 0.00033333333, 0.00033333333, 0.00033333333, 0.00033333333, 0.00033333333, 0.00033333333, 0.00033333333, 0.00033333333, 0.00033333333, 0.00033333333, 0.00033333333, 0.00033333333, 0.00033333333, 0.00033333333}'::real[],
	NULL::real[],
	'{0.00090382085}'::real[],
	NULL::real[],
	NULL::real[],
	E'{"0.00","4.11","1.12","1.14","1.19","1.41","2.07","3.47","0.45","0.86","0.99","1.11","1.66","1.79","2.22","2.36","4.79","5.00","5.07","19.14","0.34","0.77","0.98","1.17","1.38","1.39","1.53","1.60","2.13","2.14","2.41","2.70","2.91","3.55","5.02","0.32","0.61","0.81","1.02","1.20","1.63","1.78","1.83","1.98","2.16","2.57","2.58","2.63","2.71","2.82","3.02","3.07","3.31","3.49","4.09","4.70","5.83","6.20","6.42","6.65","6.71","6.81","7.47","7.73","8.32","8.90","11.94","13.00","23.92","25.86","0.52","0.60","0.68","0.83","0.85","0.90","0.92","1.00","1.03","1.22","1.40","1.42","1.52","1.62","1.70","1.89","1.90","1.92","1.95","1.99","2.08","2.45","2.48","2.80","2.85","2.86","3.05","3.57","3.58","4.26"}'::numeric[],
	E'{"0.01","0.65","1.28","1.88","2.49","3.12","3.69","4.24","4.73","5.29","5.72","6.26","6.84","7.36","7.90","8.49","9.12","9.69","10.29","10.91","11.52","12.20","12.88","13.55","14.26","15.04","15.79","16.49","17.27","18.12","18.88","19.69","20.47","21.31","22.14","23.04","23.89","24.71","25.66","26.63","27.57","28.59","29.53","30.53","31.55","32.67","33.80","34.83","35.95","37.16","38.26","39.31","40.43","41.57","42.77","43.90","45.13","46.36","47.81","49.19","50.47","52.07","53.50","55.01","56.52","58.13","59.74","61.42","63.22","65.02","66.74","68.61","70.40","72.18","74.20","76.33","79.00","81.18","83.67","85.79","88.11","90.68","93.43","96.30","99.26","102.27","105.67","109.56","113.32","117.45","121.86","127.22","132.40","138.99","144.98","152.44","160.57","173.02","186.46","208.92","291.96"}'::numeric[],
	NULL::numeric[],
	NULL::numeric[],
	NULL::numeric[]);

--
-- Table: catalog_sales, Attribute: cs_ext_discount_amt
--
-- DELETE FROM pg_statistic WHERE starelid='cte_bug.catalog_sales'::regclass AND staattnum=23;
INSERT INTO pg_statistic VALUES (
	'cte_bug.catalog_sales'::regclass,
	23::smallint,
	False::boolean,
	0.0044::real,
	6::integer,
	142728.0::real,
	1::smallint,
	2::smallint,
	3::smallint,
	0::smallint,
	0::smallint,
	1752::oid,
	1754::oid,
	1754::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	'{0.0095}'::real[],
	NULL::real[],
	'{-0.004464352}'::real[],
	NULL::real[],
	NULL::real[],
	E'{"0.00"}'::numeric[],
	E'{"0.17","11.20","20.91","30.80","40.84","51.00","63.78","75.00","87.44","99.94","114.70","128.10","142.35","157.32","174.42","192.54","210.80","230.40","248.40","268.05","290.88","314.16","335.72","359.45","382.80","407.16","434.34","460.80","487.56","514.71","543.62","576.10","607.10","638.58","674.96","708.51","744.12","781.44","816.90","854.24","892.67","937.72","978.12","1024.92","1066.60","1115.40","1163.76","1213.44","1265.92","1319.64","1377.33","1435.50","1492.72","1553.06","1615.12","1676.76","1740.50","1811.01","1879.47","1944.96","2021.00","2099.24","2180.82","2254.08","2338.24","2431.94","2518.37","2613.97","2705.79","2811.90","2915.52","3030.87","3147.78","3268.80","3398.64","3537.80","3693.20","3853.00","4025.84","4178.16","4342.57","4533.75","4713.69","4897.32","5111.40","5372.25","5621.20","5899.18","6182.22","6506.64","6877.80","7259.75","7668.62","8199.00","8717.40","9363.69","10163.70","11203.92","12513.40","14893.36","26922.00"}'::numeric[],
	NULL::numeric[],
	NULL::numeric[],
	NULL::numeric[]);

--
-- Table: catalog_sales, Attribute: cs_ext_sales_price
--
-- DELETE FROM pg_statistic WHERE starelid='cte_bug.catalog_sales'::regclass AND staattnum=24;
INSERT INTO pg_statistic VALUES (
	'cte_bug.catalog_sales'::regclass,
	24::smallint,
	False::boolean,
	0.0043666665::real,
	6::integer,
	142257.0::real,
	1::smallint,
	2::smallint,
	3::smallint,
	0::smallint,
	0::smallint,
	1752::oid,
	1754::oid,
	1754::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	'{0.009733333, 0.0002, 0.0002, 0.0002, 0.0002}'::real[],
	NULL::real[],
	'{-0.003978702}'::real[],
	NULL::real[],
	NULL::real[],
	E'{"0.00","46.08","221.76","229.32","324.00"}'::numeric[],
	E'{"0.12","10.50","20.06","29.43","40.50","50.88","62.55","73.71","86.88","99.80","112.70","127.52","143.62","159.84","176.54","195.30","213.76","232.57","250.81","273.00","293.44","314.88","338.10","359.04","380.19","405.00","429.60","451.68","477.47","502.86","527.96","553.70","584.44","614.88","648.96","678.59","714.00","752.49","789.24","832.00","873.60","918.00","961.62","1002.87","1045.32","1088.01","1138.32","1187.80","1240.32","1290.56","1343.52","1398.63","1453.80","1514.52","1579.89","1641.03","1707.15","1777.62","1850.16","1929.60","2006.40","2085.16","2172.12","2258.24","2346.43","2432.08","2522.40","2612.40","2716.00","2822.00","2941.68","3047.40","3167.66","3290.34","3408.50","3542.00","3672.90","3827.25","3964.16","4119.20","4284.36","4443.82","4620.28","4815.00","5049.41","5287.50","5540.15","5817.90","6091.80","6419.70","6763.11","7107.12","7533.20","7984.68","8549.10","9239.68","10028.92","11141.40","12375.32","14442.82","25866.02"}'::numeric[],
	NULL::numeric[],
	NULL::numeric[],
	NULL::numeric[]);

--
-- Table: catalog_sales, Attribute: cs_ext_wholesale_cost
--
-- DELETE FROM pg_statistic WHERE starelid='cte_bug.catalog_sales'::regclass AND staattnum=25;
INSERT INTO pg_statistic VALUES (
	'cte_bug.catalog_sales'::regclass,
	25::smallint,
	False::boolean,
	0.004466667::real,
	6::integer,
	199389.0::real,
	2::smallint,
	3::smallint,
	0::smallint,
	0::smallint,
	0::smallint,
	1754::oid,
	1754::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	NULL::real[],
	'{-0.0034636194}'::real[],
	NULL::real[],
	NULL::real[],
	NULL::real[],
	E'{"1.06","33.44","56.88","76.70","95.46","117.98","141.00","166.72","189.80","211.64","240.48","270.13","296.05","322.14","351.56","382.20","408.80","443.17","471.27","503.04","536.48","569.53","605.88","642.30","676.20","713.25","748.00","784.08","822.58","862.92","904.12","944.00","988.12","1033.94","1078.22","1125.57","1174.44","1219.20","1271.47","1321.32","1373.73","1424.13","1473.56","1523.06","1577.80","1632.80","1692.33","1751.88","1804.98","1865.64","1925.04","1986.64","2048.64","2112.36","2177.07","2241.60","2304.72","2377.20","2443.50","2515.50","2594.70","2670.36","2747.52","2824.00","2907.76","2981.60","3064.35","3151.04","3236.05","3335.04","3423.42","3510.24","3609.13","3694.74","3790.59","3900.42","3997.84","4093.60","4201.20","4315.14","4440.32","4580.68","4717.80","4850.32","4981.08","5120.57","5295.40","5446.35","5591.49","5754.84","5939.64","6145.90","6353.16","6573.59","6818.15","7085.50","7369.09","7709.87","8107.01","8627.71","9908.00"}'::numeric[],
	NULL::numeric[],
	NULL::numeric[],
	NULL::numeric[],
	NULL::numeric[]);

--
-- Table: catalog_sales, Attribute: cs_ext_list_price
--
-- DELETE FROM pg_statistic WHERE starelid='cte_bug.catalog_sales'::regclass AND staattnum=26;
INSERT INTO pg_statistic VALUES (
	'cte_bug.catalog_sales'::regclass,
	26::smallint,
	False::boolean,
	0.0046::real,
	7::integer,
	360309.0::real,
	2::smallint,
	3::smallint,
	0::smallint,
	0::smallint,
	0::smallint,
	1754::oid,
	1754::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	NULL::real[],
	'{-0.0035410712}'::real[],
	NULL::real[],
	NULL::real[],
	NULL::real[],
	E'{"1.31","59.20","97.76","139.16","175.10","215.86","258.20","301.76","346.30","395.71","441.67","491.37","543.03","588.96","647.90","704.77","761.60","820.05","876.37","931.44","988.52","1051.24","1114.54","1179.42","1241.24","1315.60","1390.35","1465.83","1545.12","1616.40","1698.12","1773.90","1846.20","1925.82","1999.80","2079.66","2167.27","2254.16","2349.06","2439.60","2538.36","2639.88","2745.00","2842.88","2945.84","3061.35","3159.96","3268.20","3385.82","3497.60","3619.28","3731.16","3844.31","3972.32","4086.60","4223.18","4350.00","4491.60","4633.60","4772.85","4900.80","5039.44","5187.14","5331.15","5491.76","5659.20","5821.20","5993.74","6151.74","6324.80","6490.96","6680.54","6873.60","7073.04","7286.40","7516.05","7726.84","7955.22","8181.76","8428.98","8698.56","8945.39","9241.96","9521.28","9816.00","10132.22","10488.06","10859.64","11241.16","11644.83","12054.38","12542.40","13138.72","13710.72","14384.79","15172.80","16106.68","17329.62","18721.00","21017.28","28889.00"}'::numeric[],
	NULL::numeric[],
	NULL::numeric[],
	NULL::numeric[],
	NULL::numeric[]);

--
-- Table: catalog_sales, Attribute: cs_ext_tax
--
-- DELETE FROM pg_statistic WHERE starelid='cte_bug.catalog_sales'::regclass AND staattnum=27;
INSERT INTO pg_statistic VALUES (
	'cte_bug.catalog_sales'::regclass,
	27::smallint,
	False::boolean,
	0.004766667::real,
	6::integer,
	21954.0::real,
	1::smallint,
	2::smallint,
	3::smallint,
	0::smallint,
	0::smallint,
	1752::oid,
	1754::oid,
	1754::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	'{0.10706667, 0.00063333334, 0.0006, 0.00056666665, 0.00056666665, 0.00056666665, 0.00056666665, 0.00056666665, 0.00053333334, 0.00053333334, 0.00053333334, 0.00053333334, 0.00053333334, 0.0005, 0.0005, 0.0005, 0.0005, 0.0005, 0.0005, 0.0005, 0.0005, 0.00046666668, 0.00046666668, 0.00046666668, 0.00046666668, 0.00046666668, 0.00046666668, 0.00046666668, 0.00046666668, 0.00046666668, 0.00043333333, 0.00043333333, 0.00043333333, 0.00043333333, 0.00043333333, 0.00043333333, 0.00043333333, 0.00043333333, 0.00043333333, 0.00043333333, 0.00043333333, 0.00043333333, 0.00043333333, 0.00043333333, 0.0004, 0.0004, 0.0004, 0.0004, 0.0004, 0.0004, 0.0004, 0.0004, 0.0004, 0.0004, 0.0004, 0.0004, 0.0004, 0.0004, 0.0004, 0.0004, 0.0004, 0.0004, 0.0004, 0.0004, 0.0004, 0.0004, 0.0004, 0.0004, 0.0004, 0.00036666667, 0.00036666667, 0.00036666667, 0.00036666667, 0.00036666667, 0.00036666667, 0.00036666667, 0.00036666667, 0.00036666667, 0.00036666667, 0.00036666667, 0.00036666667, 0.00036666667, 0.00036666667, 0.00036666667, 0.00036666667, 0.00036666667, 0.00036666667, 0.00036666667, 0.00036666667, 0.00036666667, 0.00036666667, 0.00036666667, 0.00036666667, 0.00036666667, 0.00036666667, 0.00036666667, 0.00036666667, 0.00036666667, 0.00036666667, 0.00033333333}'::real[],
	NULL::real[],
	'{0.004360509}'::real[],
	NULL::real[],
	NULL::real[],
	E'{"0.00","0.04","0.17","0.02","0.28","0.84","1.06","1.14","0.13","0.14","0.25","0.36","0.79","0.03","0.33","0.34","0.39","0.48","0.90","0.91","1.10","0.08","0.09","0.18","0.42","0.45","0.54","1.25","1.40","1.41","0.07","0.30","0.32","0.50","0.63","0.78","0.93","1.56","1.58","2.01","2.83","4.17","4.23","5.38","0.01","0.05","0.06","0.23","0.35","0.37","0.44","0.51","0.65","0.66","0.69","0.72","0.92","1.07","1.20","1.91","2.25","2.35","2.39","2.49","2.59","2.61","2.84","3.46","5.14","0.11","0.12","0.29","0.31","0.55","0.64","0.86","0.94","0.97","1.04","1.11","1.27","1.34","1.72","1.74","1.82","2.07","2.41","2.63","3.16","3.22","3.31","4.24","4.37","4.50","4.67","6.56","7.04","7.48","9.72","0.20"}'::numeric[],
	E'{"0.10","0.76","1.28","1.69","2.10","2.54","3.00","3.45","3.87","4.42","4.93","5.42","5.92","6.50","7.06","7.75","8.39","9.02","9.63","10.39","11.20","12.02","12.86","13.70","14.51","15.26","16.17","17.18","18.15","19.08","20.18","21.20","22.36","23.50","24.63","25.72","26.96","28.39","29.87","31.24","32.78","34.20","35.67","37.38","39.11","41.00","42.83","44.65","46.61","48.72","50.66","53.20","55.29","57.88","60.49","63.18","66.04","69.09","72.25","75.29","78.51","82.02","85.29","89.36","92.98","96.95","101.40","105.70","110.69","116.16","120.93","126.18","131.91","137.88","143.80","151.06","157.08","164.57","172.99","181.14","190.17","200.65","210.92","221.02","232.90","244.78","259.12","273.65","291.32","310.11","330.23","353.23","380.19","411.93","449.37","493.85","546.42","616.37","721.39","893.95","2142.09"}'::numeric[],
	NULL::numeric[],
	NULL::numeric[],
	NULL::numeric[]);

--
-- Table: catalog_sales, Attribute: cs_coupon_amt
--
-- DELETE FROM pg_statistic WHERE starelid='cte_bug.catalog_sales'::regclass AND staattnum=28;
INSERT INTO pg_statistic VALUES (
	'cte_bug.catalog_sales'::regclass,
	28::smallint,
	False::boolean,
	0.0052333334::real,
	3::integer,
	6889.0::real,
	1::smallint,
	2::smallint,
	3::smallint,
	0::smallint,
	0::smallint,
	1752::oid,
	1754::oid,
	1754::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	'{0.80296665}'::real[],
	NULL::real[],
	'{0.6553299}'::real[],
	NULL::real[],
	NULL::real[],
	E'{"0.00"}'::numeric[],
	E'{"0.01","2.20","4.55","7.28","10.58","14.36","17.92","22.46","25.33","29.09","32.44","36.89","41.40","45.19","49.75","55.22","62.09","68.46","74.86","81.76","91.57","99.10","106.96","114.24","121.21","127.94","137.96","148.71","157.54","170.28","181.68","193.93","204.76","214.01","227.90","241.64","255.16","269.95","283.85","297.98","314.58","330.18","348.86","367.79","387.60","402.79","419.99","435.31","461.47","484.00","509.09","533.90","558.08","588.06","619.44","645.35","672.88","700.29","729.03","760.23","796.67","836.28","873.12","909.36","951.28","1008.91","1061.09","1112.98","1155.96","1201.35","1259.28","1324.66","1399.44","1477.63","1548.31","1632.65","1703.81","1774.10","1847.07","1944.10","2036.97","2136.30","2215.42","2317.48","2479.32","2652.32","2791.40","2968.30","3152.68","3345.17","3578.36","3851.51","4127.20","4451.55","4914.34","5421.31","6009.40","6794.45","7819.40","9995.94","21255.41"}'::numeric[],
	NULL::numeric[],
	NULL::numeric[],
	NULL::numeric[]);

--
-- Table: catalog_sales, Attribute: cs_ext_ship_cost
--
-- DELETE FROM pg_statistic WHERE starelid='cte_bug.catalog_sales'::regclass AND staattnum=29;
INSERT INTO pg_statistic VALUES (
	'cte_bug.catalog_sales'::regclass,
	29::smallint,
	False::boolean,
	0.0045::real,
	6::integer,
	81417.0::real,
	1::smallint,
	2::smallint,
	3::smallint,
	0::smallint,
	0::smallint,
	1752::oid,
	1754::oid,
	1754::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	'{0.0188, 0.0003, 0.00026666667, 0.00023333334, 0.00023333334, 0.00023333334, 0.00023333334, 0.00023333334, 0.0002, 0.0002, 0.0002, 0.0002, 0.0002, 0.0002, 0.0002, 0.0002, 0.0002, 0.0002, 0.0002}'::real[],
	NULL::real[],
	'{-0.0046927556}'::real[],
	NULL::real[],
	NULL::real[],
	E'{"0.00","124.80","123.20","39.60","54.00","79.80","138.60","216.72","14.00","23.40","45.60","48.30","73.44","82.80","83.52","128.64","148.50","201.60","369.60"}'::numeric[],
	E'{"0.01","5.94","11.44","16.77","22.24","28.70","34.72","40.68","46.76","54.04","61.00","68.16","76.50","85.26","94.55","103.02","111.44","121.44","132.24","143.40","153.90","165.76","177.65","188.67","201.11","212.38","224.93","238.72","252.96","266.76","282.10","299.12","315.56","331.39","347.22","363.12","379.85","399.00","416.25","434.16","453.96","475.60","497.55","521.70","543.90","568.03","592.20","618.64","644.00","669.06","697.92","724.13","755.70","784.09","816.31","849.52","882.75","915.75","950.25","987.00","1022.35","1059.10","1097.07","1144.50","1187.60","1237.04","1284.01","1339.45","1388.34","1443.94","1496.00","1543.70","1598.74","1656.20","1727.70","1794.60","1866.41","1936.12","2007.84","2091.57","2178.28","2270.17","2367.40","2465.85","2573.55","2691.00","2809.66","2948.40","3100.80","3253.56","3417.26","3620.70","3826.12","4070.04","4341.72","4685.14","5057.36","5597.70","6345.93","7552.84","14029.29"}'::numeric[],
	NULL::numeric[],
	NULL::numeric[],
	NULL::numeric[]);

--
-- Table: catalog_sales, Attribute: cs_net_paid
--
-- DELETE FROM pg_statistic WHERE starelid='cte_bug.catalog_sales'::regclass AND staattnum=30;
INSERT INTO pg_statistic VALUES (
	'cte_bug.catalog_sales'::regclass,
	30::smallint,
	False::boolean,
	0.0045666667::real,
	6::integer,
	155617.0::real,
	1::smallint,
	2::smallint,
	3::smallint,
	0::smallint,
	0::smallint,
	1752::oid,
	1754::oid,
	1754::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	'{0.011266666, 0.0002, 0.0002}'::real[],
	NULL::real[],
	'{-0.0071978415}'::real[],
	NULL::real[],
	NULL::real[],
	E'{"0.00","221.76","267.30"}'::numeric[],
	E'{"0.04","6.30","13.46","20.10","27.38","36.00","45.37","54.42","64.17","74.16","84.95","95.83","107.47","120.56","133.68","147.52","161.55","176.09","192.62","209.85","225.68","242.40","260.64","280.40","299.63","319.88","342.68","361.62","383.67","407.20","430.95","452.40","478.20","502.86","526.93","551.28","580.65","609.84","645.84","672.01","705.60","742.28","777.65","819.77","859.65","905.32","946.21","988.00","1030.29","1072.29","1120.20","1169.28","1222.48","1277.88","1335.95","1393.40","1452.30","1510.88","1577.52","1643.88","1712.18","1783.60","1864.20","1941.02","2021.80","2110.48","2194.50","2288.80","2382.00","2480.31","2575.77","2686.53","2796.40","2918.30","3030.33","3153.62","3270.28","3405.85","3555.37","3689.67","3856.30","4006.50","4182.84","4362.48","4551.55","4780.32","5045.92","5298.28","5580.25","5880.00","6219.92","6583.20","6993.36","7441.75","7943.75","8616.00","9434.75","10515.84","11757.96","14014.08","25785.76"}'::numeric[],
	NULL::numeric[],
	NULL::numeric[],
	NULL::numeric[]);

--
-- Table: catalog_sales, Attribute: cs_net_paid_inc_tax
--
-- DELETE FROM pg_statistic WHERE starelid='cte_bug.catalog_sales'::regclass AND staattnum=31;
INSERT INTO pg_statistic VALUES (
	'cte_bug.catalog_sales'::regclass,
	31::smallint,
	False::boolean,
	0.004766667::real,
	6::integer,
	266415.0::real,
	1::smallint,
	2::smallint,
	3::smallint,
	0::smallint,
	0::smallint,
	1752::oid,
	1754::oid,
	1754::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	'{0.011266666}'::real[],
	NULL::real[],
	'{-0.007137176}'::real[],
	NULL::real[],
	NULL::real[],
	E'{"0.00"}'::numeric[],
	E'{"0.04","6.57","13.94","21.03","28.62","37.23","47.06","56.65","66.91","77.14","88.19","99.88","111.66","125.70","139.15","153.96","167.98","183.70","200.51","218.37","234.41","252.12","271.21","292.28","311.64","332.88","354.70","376.40","397.68","423.75","447.30","472.27","496.81","520.51","546.40","573.95","604.97","637.86","669.29","702.85","736.56","771.82","808.78","850.00","894.59","941.49","983.86","1030.60","1072.51","1120.44","1164.50","1218.82","1276.89","1328.94","1392.76","1450.62","1515.35","1577.79","1642.09","1715.87","1785.64","1867.43","1945.94","2025.64","2113.90","2200.02","2292.15","2386.61","2484.22","2590.13","2687.89","2800.02","2917.72","3043.84","3167.64","3291.60","3406.12","3562.87","3712.26","3872.48","4020.94","4187.61","4364.95","4559.15","4755.57","4989.93","5272.48","5532.24","5829.06","6143.38","6489.24","6878.77","7303.39","7746.09","8295.79","9014.30","9837.86","11001.37","12381.74","14679.09","26662.34"}'::numeric[],
	NULL::numeric[],
	NULL::numeric[],
	NULL::numeric[]);

--
-- Table: catalog_sales, Attribute: cs_net_paid_inc_ship
--
-- DELETE FROM pg_statistic WHERE starelid='cte_bug.catalog_sales'::regclass AND staattnum=32;
INSERT INTO pg_statistic VALUES (
	'cte_bug.catalog_sales'::regclass,
	32::smallint,
	False::boolean,
	0.0::real,
	7::integer,
	280640.0::real,
	2::smallint,
	3::smallint,
	0::smallint,
	0::smallint,
	0::smallint,
	1754::oid,
	1754::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	NULL::real[],
	'{-0.0071525415}'::real[],
	NULL::real[],
	NULL::real[],
	NULL::real[],
	E'{"0.00","23.85","47.34","67.95","89.80","110.58","132.44","153.00","179.04","202.51","229.06","255.64","281.80","311.22","340.96","371.09","403.00","434.83","466.69","497.15","530.10","562.00","593.67","628.20","664.69","704.00","739.88","780.85","825.34","866.72","907.27","949.41","994.16","1044.45","1096.50","1144.92","1195.21","1250.15","1305.36","1361.76","1416.36","1475.91","1536.25","1598.70","1665.83","1732.92","1800.90","1873.10","1941.75","2016.72","2092.86","2175.34","2253.86","2345.76","2434.51","2518.72","2608.45","2696.29","2793.56","2894.40","2985.12","3079.88","3181.62","3294.00","3407.10","3528.30","3649.14","3775.68","3899.28","4033.51","4169.60","4307.38","4456.66","4620.28","4793.89","4963.42","5138.76","5318.80","5502.90","5715.60","5907.94","6131.28","6356.80","6629.70","6889.46","7169.33","7478.59","7780.37","8131.05","8498.52","8903.58","9369.99","9838.92","10426.64","11123.84","11986.61","13051.50","14332.97","16169.95","19117.60","36471.09"}'::numeric[],
	NULL::numeric[],
	NULL::numeric[],
	NULL::numeric[],
	NULL::numeric[]);

--
-- Table: catalog_sales, Attribute: cs_net_paid_inc_ship_tax
--
-- DELETE FROM pg_statistic WHERE starelid='cte_bug.catalog_sales'::regclass AND staattnum=33;
INSERT INTO pg_statistic VALUES (
	'cte_bug.catalog_sales'::regclass,
	33::smallint,
	False::boolean,
	0.0::real,
	7::integer,
	613201.0::real,
	2::smallint,
	3::smallint,
	0::smallint,
	0::smallint,
	0::smallint,
	1754::oid,
	1754::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	NULL::real[],
	'{-0.00728921}'::real[],
	NULL::real[],
	NULL::real[],
	NULL::real[],
	E'{"0.00","24.55","48.62","69.15","91.91","112.84","135.45","157.97","183.66","207.27","233.71","261.95","289.12","318.33","350.11","381.22","413.11","444.48","477.58","508.92","542.83","576.18","609.45","644.62","682.66","719.94","759.00","801.51","845.42","887.38","930.05","971.99","1022.12","1070.40","1121.18","1175.11","1228.42","1282.74","1334.77","1397.05","1456.76","1508.61","1575.93","1641.47","1709.47","1778.38","1844.93","1917.32","1990.71","2063.03","2147.81","2231.80","2316.78","2402.40","2493.91","2591.15","2678.00","2770.78","2865.32","2974.50","3066.56","3162.22","3269.67","3378.18","3496.07","3622.04","3743.00","3878.25","4003.40","4149.54","4287.78","4434.50","4591.26","4750.74","4944.45","5105.41","5289.66","5467.44","5660.87","5871.93","6090.64","6310.11","6551.52","6808.71","7096.07","7374.30","7678.33","8022.91","8355.36","8765.02","9187.25","9660.31","10158.98","10789.68","11521.51","12322.88","13498.43","14789.11","16663.22","19689.06","37177.23"}'::numeric[],
	NULL::numeric[],
	NULL::numeric[],
	NULL::numeric[],
	NULL::numeric[]);

--
-- Table: catalog_sales, Attribute: cs_net_profit
--
-- DELETE FROM pg_statistic WHERE starelid='cte_bug.catalog_sales'::regclass AND staattnum=34;
INSERT INTO pg_statistic VALUES (
	'cte_bug.catalog_sales'::regclass,
	34::smallint,
	False::boolean,
	0.0::real,
	6::integer,
	206524.0::real,
	1::smallint,
	2::smallint,
	3::smallint,
	0::smallint,
	0::smallint,
	1752::oid,
	1754::oid,
	1754::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	'{0.0003, 0.0002}'::real[],
	NULL::real[],
	'{-0.0019606797}'::real[],
	NULL::real[],
	NULL::real[],
	E'{"0.00","-47.52"}'::numeric[],
	E'{"-9478.75","-6317.17","-5427.84","-4811.17","-4310.01","-3964.66","-3670.14","-3411.00","-3196.27","-2980.12","-2792.84","-2624.60","-2471.95","-2325.60","-2192.15","-2061.36","-1940.00","-1830.51","-1735.50","-1637.76","-1537.14","-1452.35","-1375.59","-1298.36","-1224.32","-1155.27","-1092.42","-1022.40","-963.48","-909.46","-846.45","-791.42","-741.60","-693.68","-651.75","-611.00","-571.48","-529.55","-492.82","-458.06","-424.16","-393.25","-361.82","-332.56","-302.38","-277.25","-248.65","-223.78","-199.01","-180.17","-158.72","-138.32","-121.68","-105.41","-89.61","-74.48","-59.72","-46.63","-35.01","-23.56","-13.29","-4.92","3.90","16.20","29.60","44.02","63.55","84.33","106.65","130.83","159.30","193.63","231.83","274.32","317.52","364.48","414.57","470.10","538.53","608.95","685.44","768.20","856.98","953.40","1053.60","1164.76","1294.00","1444.00","1596.00","1771.35","1966.12","2176.68","2427.26","2738.40","3132.80","3548.87","4041.96","4805.58","5798.10","7455.03","16965.90"}'::numeric[],
	NULL::numeric[],
	NULL::numeric[],
	NULL::numeric[]);

--
-- Table: customer, Attribute: c_customer_sk
--
-- DELETE FROM pg_statistic WHERE starelid='cte_bug.customer'::regclass AND staattnum=1;
INSERT INTO pg_statistic VALUES (
	'cte_bug.customer'::regclass,
	1::smallint,
	False::boolean,
	0.0::real,
	4::integer,
	-1.0::real,
	2::smallint,
	3::smallint,
	0::smallint,
	0::smallint,
	0::smallint,
	97::oid,
	97::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	NULL::real[],
	'{0.09763528}'::real[],
	NULL::real[],
	NULL::real[],
	NULL::real[],
	E'{"9","21261","42021","62080","82255","103369","125043","144232","165965","190286","211001","230315","251065","272203","292790","312482","334187","353856","373058","396690","415673","436316","456746","480529","502046","524523","546632","568546","589481","610972","631185","654707","675842","698400","718405","739653","760815","781559","803530","825502","846209","866622","888173","908015","929893","950771","971557","991734","1015944","1037799","1059421","1080667","1101711","1122753","1142324","1163272","1183361","1204818","1225216","1247329","1269800","1290928","1313000","1333417","1354039","1374731","1393758","1413066","1434044","1455682","1476112","1499430","1518599","1539394","1559012","1578764","1600604","1620632","1642082","1661234","1681629","1701960","1723759","1745015","1765218","1786153","1808002","1827671","1848579","1869421","1889087","1911370","1931677","1953626","1974278","1994274","2014813","2037922","2058859","2079452","2099872"}'::int4[],
	NULL::int4[],
	NULL::int4[],
	NULL::int4[],
	NULL::int4[]);

--
-- Table: customer, Attribute: c_customer_id
--
-- DELETE FROM pg_statistic WHERE starelid='cte_bug.customer'::regclass AND staattnum=2;
INSERT INTO pg_statistic VALUES (
	'cte_bug.customer'::regclass,
	2::smallint,
	False::boolean,
	0.0::real,
	17::integer,
	-1.0::real,
	2::smallint,
	3::smallint,
	0::smallint,
	0::smallint,
	0::smallint,
	664::oid,
	664::oid,
	0::oid,
	0::oid,
	0::oid,
	100::oid,
	100::oid,
	0::oid,
	0::oid,
	0::oid,
	NULL::real[],
	'{-0.008318781}'::real[],
	NULL::real[],
	NULL::real[],
	NULL::real[],
	E'{"AAAAAAAAAAABNBAA","AAAAAAAAACGBGAAA","AAAAAAAAAELNKBAA","AAAAAAAAAHEPIBAA","AAAAAAAAAJKFCAAA","AAAAAAAAAMDPPBAA","AAAAAAAAAOPLIAAA","AAAAAAAABBKGGAAA","AAAAAAAABEDPKBAA","AAAAAAAABGMDNAAA","AAAAAAAABJECAAAA","AAAAAAAABMDEJBAA","AAAAAAAABONCCAAA","AAAAAAAACBEIBBAA","AAAAAAAACDPKGBAA","AAAAAAAACGLFEAAA","AAAAAAAACJEIMAAA","AAAAAAAACMCDAAAA","AAAAAAAACOLBDBAA","AAAAAAAADBIDOAAA","AAAAAAAADEGFPBAA","AAAAAAAADGPJIAAA","AAAAAAAADJHBOAAA","AAAAAAAADMAFGAAA","AAAAAAAADOHCCAAA","AAAAAAAAEAOAEBAA","AAAAAAAAEDNGDBAA","AAAAAAAAEGIFIAAA","AAAAAAAAEJAMPBAA","AAAAAAAAELLKDAAA","AAAAAAAAEOCLPAAA","AAAAAAAAFAINOAAA","AAAAAAAAFDBIJBAA","AAAAAAAAFFHIMBAA","AAAAAAAAFHPMHAAA","AAAAAAAAFKIAPBAA","AAAAAAAAFNCOBAAA","AAAAAAAAFPKHLAAA","AAAAAAAAGCDNHAAA","AAAAAAAAGEJHIBAA","AAAAAAAAGHBCCAAA","AAAAAAAAGJJENBAA","AAAAAAAAGMCKCAAA","AAAAAAAAGOMBABAA","AAAAAAAAHBIPLBAA","AAAAAAAAHDPAKAAA","AAAAAAAAHGJCKBAA","AAAAAAAAHJALPAAA","AAAAAAAAHLOFPAAA","AAAAAAAAHOHAAAAA","AAAAAAAAIAOACBAA","AAAAAAAAIDGNKAAA","AAAAAAAAIGCCNBAA","AAAAAAAAIIMJAAAA","AAAAAAAAILNBOBAA","AAAAAAAAIOCOJBAA","AAAAAAAAJAIBNBAA","AAAAAAAAJDBAABAA","AAAAAAAAJFFOFBAA","AAAAAAAAJICEDBAA","AAAAAAAAJKILJBAA","AAAAAAAAJMOJNBAA","AAAAAAAAJPEMKBAA","AAAAAAAAKBMKDBAA","AAAAAAAAKEDLDAAA","AAAAAAAAKGMFDBAA","AAAAAAAAKJJJCBAA","AAAAAAAAKMAFLBAA","AAAAAAAAKOGELBAA","AAAAAAAALAOCABAA","AAAAAAAALDGGLAAA","AAAAAAAALGAJJBAA","AAAAAAAALIJMEAAA","AAAAAAAALKOLOBAA","AAAAAAAALNFAKBAA","AAAAAAAALPKMDBAA","AAAAAAAAMCDMMBAA","AAAAAAAAMELNCBAA","AAAAAAAAMHFNBBAA","AAAAAAAAMJMFPBAA","AAAAAAAAMMGBFAAA","AAAAAAAAMPBPFAAA","AAAAAAAANBJMLAAA","AAAAAAAANECPPBAA","AAAAAAAANHBEAAAA","AAAAAAAANJIOCBAA","AAAAAAAANMDCBAAA","AAAAAAAANPAGFBAA","AAAAAAAAOBIDJBAA","AAAAAAAAODNGFAAA","AAAAAAAAOGECJBAA","AAAAAAAAOINLGAAA","AAAAAAAAOLGCEBAA","AAAAAAAAONPNAAAA","AAAAAAAAPAKLGAAA","AAAAAAAAPDDNKAAA","AAAAAAAAPFNALBAA","AAAAAAAAPIFFFAAA","AAAAAAAAPLABOAAA","AAAAAAAAPNGKCBAA","AAAAAAAAPPPOAAAA"}'::varchar[],
	NULL::varchar[],
	NULL::varchar[],
	NULL::varchar[],
	NULL::varchar[]);

--
-- Table: customer, Attribute: c_current_cdemo_sk
--
-- DELETE FROM pg_statistic WHERE starelid='cte_bug.customer'::regclass AND staattnum=3;
INSERT INTO pg_statistic VALUES (
	'cte_bug.customer'::regclass,
	3::smallint,
	False::boolean,
	0.036333334::real,
	4::integer,
	-0.50137764::real,
	2::smallint,
	3::smallint,
	0::smallint,
	0::smallint,
	0::smallint,
	97::oid,
	97::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	NULL::real[],
	'{0.0013170508}'::real[],
	NULL::real[],
	NULL::real[],
	NULL::real[],
	E'{"83","18955","39087","58094","75845","95317","115100","134187","152171","172644","193010","211552","231710","251817","270655","288181","306995","323507","344088","362797","382712","404200","423621","442226","460401","479260","497142","517339","536731","556732","574796","592497","612826","632068","650345","670861","690813","710439","729562","747865","768032","784796","804004","823342","840808","862022","880748","899717","920319","938794","957931","976962","994731","1014023","1033686","1051262","1072185","1092147","1113436","1132849","1151543","1170064","1188829","1208390","1228056","1247895","1268450","1286523","1306787","1325642","1344821","1361249","1380841","1399617","1418271","1436735","1456195","1476598","1496520","1514478","1535680","1554729","1573897","1593768","1614360","1634204","1654338","1672759","1691901","1711514","1731470","1749749","1767712","1787017","1807176","1824761","1843120","1863530","1883265","1901706","1920535"}'::int4[],
	NULL::int4[],
	NULL::int4[],
	NULL::int4[],
	NULL::int4[]);

--
-- Table: customer, Attribute: c_current_hdemo_sk
--
-- DELETE FROM pg_statistic WHERE starelid='cte_bug.customer'::regclass AND staattnum=4;
INSERT INTO pg_statistic VALUES (
	'cte_bug.customer'::regclass,
	4::smallint,
	False::boolean,
	0.0347::real,
	4::integer,
	7192.0::real,
	1::smallint,
	2::smallint,
	3::smallint,
	0::smallint,
	0::smallint,
	96::oid,
	97::oid,
	97::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	'{0.00043333333, 0.00043333333, 0.00043333333, 0.0004, 0.0004, 0.0004, 0.0004}'::real[],
	NULL::real[],
	'{0.00030779035}'::real[],
	NULL::real[],
	NULL::real[],
	E'{"882","1308","3965","367","666","1327","6987"}'::int4[],
	E'{"1","72","141","218","292","372","440","506","579","647","728","799","868","940","1006","1081","1151","1224","1294","1368","1432","1502","1576","1650","1728","1803","1873","1941","2024","2095","2168","2245","2318","2395","2472","2542","2617","2682","2751","2823","2897","2969","3041","3110","3179","3248","3319","3390","3457","3520","3590","3657","3725","3801","3881","3954","4024","4098","4179","4251","4312","4383","4451","4522","4604","4674","4745","4814","4885","4954","5024","5095","5171","5245","5317","5386","5452","5523","5591","5664","5732","5805","5883","5956","6025","6099","6170","6241","6316","6392","6464","6534","6610","6685","6756","6829","6900","6980","7059","7129","7200"}'::int4[],
	NULL::int4[],
	NULL::int4[],
	NULL::int4[]);

--
-- Table: customer, Attribute: c_current_addr_sk
--
-- DELETE FROM pg_statistic WHERE starelid='cte_bug.customer'::regclass AND staattnum=5;
INSERT INTO pg_statistic VALUES (
	'cte_bug.customer'::regclass,
	5::smallint,
	False::boolean,
	0.0::real,
	4::integer,
	-0.32888332::real,
	2::smallint,
	3::smallint,
	0::smallint,
	0::smallint,
	0::smallint,
	97::oid,
	97::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	NULL::real[],
	'{0.0058509638}'::real[],
	NULL::real[],
	NULL::real[],
	NULL::real[],
	E'{"52","10026","21386","32663","42941","53021","65083","75763","86063","96263","107445","117990","127743","138145","147534","158592","168713","178593","188864","199238","210017","220563","231187","241437","252281","263159","273251","284057","295058","305713","315928","326018","336613","346947","357443","368266","378995","388655","399147","410310","419835","429767","442179","452745","463314","473270","483327","494816","505073","515313","525490","536085","546403","556179","567315","577096","587145","597369","608015","619292","629700","640103","651254","661172","671946","681745","691688","702105","712203","721654","733295","743331","753425","764850","774949","785116","795978","806368","817723","827531","838047","849099","859705","870677","881136","891818","902438","912558","922958","933355","943287","953511","964298","974663","985836","996922","1007782","1017952","1029639","1040067","1049984"}'::int4[],
	NULL::int4[],
	NULL::int4[],
	NULL::int4[],
	NULL::int4[]);

--
-- Table: customer, Attribute: c_first_shipto_date_sk
--
-- DELETE FROM pg_statistic WHERE starelid='cte_bug.customer'::regclass AND staattnum=6;
INSERT INTO pg_statistic VALUES (
	'cte_bug.customer'::regclass,
	6::smallint,
	False::boolean,
	0.035633333::real,
	4::integer,
	3649.0::real,
	1::smallint,
	2::smallint,
	3::smallint,
	0::smallint,
	0::smallint,
	96::oid,
	97::oid,
	97::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	'{0.0006, 0.0006, 0.00056666665, 0.00056666665, 0.00056666665, 0.00056666665, 0.00056666665, 0.00056666665, 0.00056666665}'::real[],
	NULL::real[],
	'{-0.0058256034}'::real[],
	NULL::real[],
	NULL::real[],
	E'{"2450039","2451757","2449267","2449566","2449608","2450648","2450973","2451697","2452197"}'::int4[],
	E'{"2449028","2449061","2449097","2449131","2449163","2449198","2449239","2449274","2449311","2449346","2449381","2449417","2449457","2449492","2449530","2449568","2449601","2449635","2449669","2449706","2449746","2449780","2449818","2449857","2449893","2449930","2449972","2450009","2450045","2450081","2450118","2450155","2450194","2450228","2450265","2450301","2450337","2450372","2450407","2450444","2450483","2450523","2450560","2450594","2450633","2450672","2450708","2450743","2450779","2450812","2450853","2450890","2450930","2450969","2451005","2451044","2451078","2451114","2451147","2451184","2451221","2451261","2451295","2451334","2451370","2451411","2451449","2451486","2451523","2451560","2451594","2451627","2451664","2451702","2451740","2451774","2451810","2451846","2451879","2451918","2451954","2451991","2452028","2452063","2452099","2452135","2452172","2452207","2452242","2452277","2452312","2452352","2452389","2452429","2452461","2452495","2452530","2452566","2452602","2452640","2452678"}'::int4[],
	NULL::int4[],
	NULL::int4[],
	NULL::int4[]);

--
-- Table: customer, Attribute: c_first_sales_date_sk
--
-- DELETE FROM pg_statistic WHERE starelid='cte_bug.customer'::regclass AND staattnum=7;
INSERT INTO pg_statistic VALUES (
	'cte_bug.customer'::regclass,
	7::smallint,
	False::boolean,
	0.034733333::real,
	4::integer,
	3650.0::real,
	1::smallint,
	2::smallint,
	3::smallint,
	0::smallint,
	0::smallint,
	96::oid,
	97::oid,
	97::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	'{0.0006, 0.0006, 0.00056666665, 0.00056666665, 0.00056666665, 0.00056666665, 0.00056666665, 0.00056666665, 0.00056666665, 0.00056666665}'::real[],
	NULL::real[],
	'{-0.008479864}'::real[],
	NULL::real[],
	NULL::real[],
	E'{"2450009","2451727","2449237","2449536","2450618","2450943","2451260","2451667","2452039","2452167"}'::int4[],
	E'{"2448998","2449032","2449067","2449101","2449135","2449170","2449209","2449245","2449281","2449317","2449352","2449389","2449428","2449463","2449500","2449537","2449571","2449603","2449637","2449673","2449713","2449747","2449785","2449823","2449859","2449897","2449940","2449977","2450013","2450049","2450084","2450121","2450161","2450196","2450234","2450270","2450306","2450341","2450375","2450412","2450450","2450489","2450526","2450562","2450599","2450638","2450674","2450708","2450743","2450778","2450819","2450854","2450894","2450934","2450971","2451008","2451044","2451079","2451114","2451151","2451188","2451226","2451263","2451301","2451338","2451379","2451417","2451453","2451492","2451528","2451561","2451594","2451629","2451669","2451706","2451741","2451776","2451811","2451845","2451884","2451921","2451957","2451993","2452029","2452067","2452104","2452140","2452175","2452211","2452245","2452281","2452322","2452358","2452398","2452431","2452465","2452501","2452536","2452573","2452610","2452648"}'::int4[],
	NULL::int4[],
	NULL::int4[],
	NULL::int4[]);

--
-- Table: customer, Attribute: c_salutation
--
-- DELETE FROM pg_statistic WHERE starelid='cte_bug.customer'::regclass AND staattnum=8;
INSERT INTO pg_statistic VALUES (
	'cte_bug.customer'::regclass,
	8::smallint,
	False::boolean,
	0.035633333::real,
	4::integer,
	6.0::real,
	1::smallint,
	3::smallint,
	0::smallint,
	0::smallint,
	0::smallint,
	98::oid,
	664::oid,
	0::oid,
	0::oid,
	0::oid,
	100::oid,
	100::oid,
	0::oid,
	0::oid,
	0::oid,
	'{0.2827, 0.16666667, 0.166, 0.117466666, 0.11726667, 0.114266664}'::real[],
	'{0.19105348}'::real[],
	NULL::real[],
	NULL::real[],
	NULL::real[],
	E'{"Dr.","Mr.","Sir","Miss","Mrs.","Ms."}'::varchar[],
	NULL::varchar[],
	NULL::varchar[],
	NULL::varchar[],
	NULL::varchar[]);

--
-- Table: customer, Attribute: c_first_name
--
-- DELETE FROM pg_statistic WHERE starelid='cte_bug.customer'::regclass AND staattnum=9;
INSERT INTO pg_statistic VALUES (
	'cte_bug.customer'::regclass,
	9::smallint,
	False::boolean,
	0.0351::real,
	6::integer,
	2938.0::real,
	1::smallint,
	2::smallint,
	3::smallint,
	0::smallint,
	0::smallint,
	98::oid,
	664::oid,
	664::oid,
	0::oid,
	0::oid,
	100::oid,
	100::oid,
	100::oid,
	0::oid,
	0::oid,
	'{0.019733334, 0.0178, 0.0177, 0.014733333, 0.013333334, 0.012866667, 0.011166667, 0.0086, 0.008466667, 0.0082, 0.006, 0.0059666666, 0.0059666666, 0.0055, 0.0055, 0.0053666667, 0.0053, 0.0053, 0.0051, 0.0049666665, 0.0049, 0.0049, 0.0047, 0.0045, 0.0042, 0.004166667, 0.0041333335, 0.0040666666, 0.0039666668, 0.0039333333, 0.0039333333, 0.0038666667, 0.0038666667, 0.0038333333, 0.0038333333, 0.0038, 0.0038, 0.0037333334, 0.0036666666, 0.0035, 0.0035, 0.0034666667, 0.0034333332, 0.0034333332, 0.0033666666, 0.0033666666, 0.0033333334, 0.0031666667, 0.0031, 0.0031, 0.0030666667, 0.003, 0.0029666666, 0.0029666666, 0.0028666668, 0.0028333333, 0.0028333333, 0.0028, 0.0027666667, 0.0027, 0.0027, 0.0026666666, 0.0026333334, 0.0026333334, 0.0026, 0.0026, 0.0025666666, 0.0025666666, 0.0025, 0.0024666667, 0.0024666667, 0.0024333333, 0.0024, 0.0024, 0.0023666667, 0.0023333333, 0.0023333333, 0.0023333333, 0.0022666666, 0.0022333334, 0.0022333334, 0.0022, 0.0022, 0.0022, 0.0021666666, 0.0021666666, 0.0021666666, 0.0021666666, 0.0021666666, 0.0021333334, 0.0020666667, 0.0020666667, 0.0020666667, 0.0020333333, 0.002, 0.0019666667, 0.0019666667, 0.0019333333, 0.0019, 0.0018666667}'::real[],
	NULL::real[],
	'{0.0041609285}'::real[],
	NULL::real[],
	NULL::real[],
	E'{"John","James","Robert","Michael","David","William","Richard","Joseph","Charles","Thomas","Christopher","Linda","Mark","Daniel","Donald","Jennifer","Elizabeth","George","Barbara","Kenneth","Margaret","Paul","Dorothy","Edward","Susan","Brian","Jason","Lisa","Steven","Gary","Ronald","Anthony","Larry","Karen","Sandra","Helen","Matthew","Nancy","Kevin","Andrew","Eric","Sharon","Donna","Jeffrey","Betty","Timothy","Raymond","Jose","Frank","Jessica","Angela","Ruth","Melissa","Rebecca","Anna","Gloria","Laura","Stephen","Amy","Brenda","Cynthia","Deborah","Pamela","Patrick","Martha","Virginia","Michelle","Sarah","Diane","Joshua","Kimberly","Ann","Arthur","Scott","Douglas","Christine","Gregory","Harold","Katherine","Dennis","Joyce","Alice","Janet","Stephanie","Jerry","Kathleen","Marie","Peter","Theresa","Walter","Albert","Amanda","Teresa","Debra","Jonathan","Fred","Keith","Willie","Gerald","Lori"}'::varchar[],
	E'{"Aaron","Adrienne","Alexander","Allyson","Andrea","Anne","April","Audrey","Benjamin","Bessie","Billy","Bradley","Brigitte","Caitlyn","Carlos","Carrie","Cecelia","Charlotte","Christian","Clara","Clifton","Cornelius","Cyrus","Danny","Dawn","Delta","Diana","Dora","Earl","Edna","Elena","Elsie","Erika","Ethel","Evelyn","Fleta","Freddie","Georgia","Glen","Gregorio","Hattie","Henriette","Homer","Ingrid","Jack","Jaleesa","Janis","Jed","Jerome","Jimmie","Joe","Jon","Juan","Julia","Justin","Kathy","Kerry","Kurt","Lavon","Leo","Letitia","Lois","Louis","Luis","Mae","Marco","Marilynn","Martin","Maurice","Melvin","Mike","Mitchell","Naomi","Nicholas","Norma","Oscar","Pauline","Phillip","Rachel","Randy","Renee","Robbie","Roger","Rose","Roy","Sabrina","Sara","Sharlene","Sherry","Stanley","Suzi","Tanya","Terry","Tina","Tonya","Tyrone","Veronica","Viola","Warren","Willian","Zulma"}'::varchar[],
	NULL::varchar[],
	NULL::varchar[],
	NULL::varchar[]);

--
-- Table: customer, Attribute: c_last_name
--
-- DELETE FROM pg_statistic WHERE starelid='cte_bug.customer'::regclass AND staattnum=10;
INSERT INTO pg_statistic VALUES (
	'cte_bug.customer'::regclass,
	10::smallint,
	False::boolean,
	0.033933334::real,
	7::integer,
	4529.0::real,
	1::smallint,
	2::smallint,
	3::smallint,
	0::smallint,
	0::smallint,
	98::oid,
	664::oid,
	664::oid,
	0::oid,
	0::oid,
	100::oid,
	100::oid,
	100::oid,
	0::oid,
	0::oid,
	'{0.015133333, 0.011733334, 0.011033333, 0.009233333, 0.008533333, 0.008166667, 0.0059666666, 0.0048666666, 0.0048, 0.0047, 0.0046333335, 0.0046333335, 0.0043666665, 0.0043, 0.0041333335, 0.004, 0.0039666668, 0.0039, 0.0039, 0.0037333334, 0.0036333334, 0.0032666668, 0.0031666667, 0.0031333333, 0.0030666667, 0.003, 0.003, 0.0029, 0.0029, 0.0027666667, 0.0027333333, 0.0027333333, 0.0027, 0.0026333334, 0.0026333334, 0.0026333334, 0.0026, 0.0026, 0.0026, 0.0025666666, 0.0025, 0.0024666667, 0.0024333333, 0.0023666667, 0.0023666667, 0.0023333333, 0.0022, 0.0022, 0.0021333334, 0.0021333334, 0.0021333334, 0.0020666667, 0.0020333333, 0.0019, 0.0019, 0.0018666667, 0.0018666667, 0.0018333333, 0.0018, 0.0017666667, 0.0017666667, 0.0017666667, 0.0017333333, 0.0017333333, 0.0017333333, 0.0017333333, 0.0017, 0.0016666667, 0.0016333334, 0.0016333334, 0.0016333334, 0.0016, 0.0016, 0.0016, 0.0015666666, 0.0015666666, 0.0015333333, 0.0015333333, 0.0015333333, 0.0015333333, 0.0015, 0.0015, 0.0015, 0.0015, 0.0014666667, 0.0014666667, 0.0014666667, 0.0014666667, 0.0014666667, 0.0014333334, 0.0014333334, 0.0014, 0.0014, 0.0014, 0.0013666666, 0.0013666666, 0.0013666666, 0.0013666666, 0.0013333333, 0.0013333333}'::real[],
	NULL::real[],
	'{0.0012389864}'::real[],
	NULL::real[],
	NULL::real[],
	E'{"Smith","Johnson","Williams","Jones","Brown","Davis","Miller","Taylor","Thomas","Jackson","Moore","Wilson","Garcia","Anderson","Martin","Walker","Harris","Martinez","Robinson","Thompson","White","Young","Wright","Lee","Lewis","Hall","King","Rodriguez","Turner","Nelson","Allen","Hill","Roberts","Carter","Clark","Gonzalez","Adams","Hernandez","Scott","Campbell","Lopez","Baker","Green","Bell","Evans","Bailey","Rogers","Torres","Mitchell","Morgan","Stewart","Perez","Edwards","Collins","Sanchez","Morris","Phillips","Gray","Parker","Cox","Reed","Rivera","Brooks","Diaz","Richardson","Watson","Washington","Perry","Murphy","Patterson","Ross","Bryant","Cooper","Kelly","Henderson","Howard","Cook","James","Peterson","Simmons","Long","Mason","Russell","Wood","Gonzales","Murray","Price","Ramirez","Ward","Hughes","Myers","Barnes","Daniels","Harrison","Alexander","Bennett","Ford","Graham","Burns","Powell"}'::varchar[],
	E'{"Aaron","Allison","Armstrong","Ayala","Barlow","Baxter","Benson","Black","Bonilla","Boyer","Bright","Burdick","Butler","Cano","Carroll","Chambers","Chinn","Coats","Comer","Correa","Creech","Cupp","Davies","Dent","Donovan","Duffy","Eaton","Enriquez","Farrington","Fink","Flynn","Frank","Fuller","Garrett","Gibson","Golding","Graves","Gross","Hale","Hardaway","Harvey","Heard","Herrera","Hobbs","Holt","Howe","Hunter","Jacobs","Johnston","Keller","Kirby","Kurtz","Larkin","Ledoux","Lindsey","Lowe","Mack","Markley","May","Mccormick","Mcgrath","Mcneil","Mercier","Moffitt","Morrison","Nagel","Nichols","Ocampo","Orozco","Padilla","Patton","Perkins","Pinto","Pratt","Raines","Reece","Rhodes","Ritter","Roman","Ruby","Salinas","Saylor","Seiler","Shepherd","Simpson","Snyder","Sperry","Stephens","Strickland","Sweeney","Thornton","Trejo","Valencia","Vernon","Walls","Weaver","West","Wiley","Winters","Worsham","Zuniga"}'::varchar[],
	NULL::varchar[],
	NULL::varchar[],
	NULL::varchar[]);

--
-- Table: customer, Attribute: c_preferred_cust_flag
--
-- DELETE FROM pg_statistic WHERE starelid='cte_bug.customer'::regclass AND staattnum=11;
INSERT INTO pg_statistic VALUES (
	'cte_bug.customer'::regclass,
	11::smallint,
	False::boolean,
	0.03533333::real,
	2::integer,
	2.0::real,
	1::smallint,
	3::smallint,
	0::smallint,
	0::smallint,
	0::smallint,
	1054::oid,
	1058::oid,
	0::oid,
	0::oid,
	0::oid,
	100::oid,
	100::oid,
	0::oid,
	0::oid,
	0::oid,
	'{0.49123332, 0.47343335}'::real[],
	'{0.4982882}'::real[],
	NULL::real[],
	NULL::real[],
	NULL::real[],
	E'{"N","Y"}'::bpchar[],
	NULL::bpchar[],
	NULL::bpchar[],
	NULL::bpchar[],
	NULL::bpchar[]);

--
-- Table: customer, Attribute: c_birth_day
--
-- DELETE FROM pg_statistic WHERE starelid='cte_bug.customer'::regclass AND staattnum=12;
INSERT INTO pg_statistic VALUES (
	'cte_bug.customer'::regclass,
	12::smallint,
	False::boolean,
	0.0334::real,
	4::integer,
	31.0::real,
	1::smallint,
	3::smallint,
	0::smallint,
	0::smallint,
	0::smallint,
	96::oid,
	97::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	'{0.0341, 0.0336, 0.0335, 0.033166666, 0.0326, 0.032266665, 0.032233335, 0.0321, 0.031933334, 0.0319, 0.0319, 0.0319, 0.031866666, 0.031833332, 0.031833332, 0.0317, 0.031533334, 0.031533334, 0.0315, 0.031466667, 0.031166667, 0.0311, 0.030966667, 0.030733334, 0.0307, 0.030533334, 0.030066667, 0.029833334, 0.029666666, 0.028233333, 0.019133333}'::real[],
	'{0.03440461}'::real[],
	NULL::real[],
	NULL::real[],
	NULL::real[],
	E'{"27","15","26","5","10","23","21","19","14","6","7","18","12","13","24","17","16","22","1","25","8","11","9","3","28","4","29","2","20","30","31"}'::int4[],
	NULL::int4[],
	NULL::int4[],
	NULL::int4[],
	NULL::int4[]);

--
-- Table: customer, Attribute: c_birth_month
--
-- DELETE FROM pg_statistic WHERE starelid='cte_bug.customer'::regclass AND staattnum=13;
INSERT INTO pg_statistic VALUES (
	'cte_bug.customer'::regclass,
	13::smallint,
	False::boolean,
	0.0355::real,
	4::integer,
	12.0::real,
	1::smallint,
	3::smallint,
	0::smallint,
	0::smallint,
	0::smallint,
	96::oid,
	97::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	'{0.08296666, 0.0828, 0.0826, 0.082166664, 0.08143333, 0.08093333, 0.079966664, 0.079733334, 0.079166666, 0.0789, 0.07836667, 0.07546667}'::real[],
	'{0.08591734}'::real[],
	NULL::real[],
	NULL::real[],
	NULL::real[],
	E'{"3","1","8","7","9","10","5","6","12","4","11","2"}'::int4[],
	NULL::int4[],
	NULL::int4[],
	NULL::int4[],
	NULL::int4[]);

--
-- Table: customer, Attribute: c_birth_year
--
-- DELETE FROM pg_statistic WHERE starelid='cte_bug.customer'::regclass AND staattnum=14;
INSERT INTO pg_statistic VALUES (
	'cte_bug.customer'::regclass,
	14::smallint,
	False::boolean,
	0.036066666::real,
	4::integer,
	69.0::real,
	1::smallint,
	3::smallint,
	0::smallint,
	0::smallint,
	0::smallint,
	96::oid,
	97::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	'{0.0158, 0.015133333, 0.0151, 0.0150666665, 0.014966667, 0.014966667, 0.014966667, 0.014966667, 0.0148, 0.014766667, 0.014733333, 0.0147, 0.0146, 0.0146, 0.014566666, 0.014366667, 0.014366667, 0.014366667, 0.014333333, 0.014333333, 0.0143, 0.014266667, 0.014266667, 0.014266667, 0.014233333, 0.0142, 0.014166667, 0.014166667, 0.014066666, 0.014066666, 0.014033333, 0.014033333, 0.014, 0.013966667, 0.013966667, 0.013966667, 0.013933334, 0.013933334, 0.013933334, 0.013866667, 0.013866667, 0.0138, 0.0138, 0.013733333, 0.013733333, 0.013666667, 0.0136, 0.0135, 0.0135, 0.013466666, 0.0134333335, 0.0134333335, 0.0134, 0.013366667, 0.013366667, 0.013266667, 0.013266667, 0.013266667, 0.013266667, 0.0132, 0.0131, 0.0131, 0.013066667, 0.013066667, 0.013033333, 0.013033333, 0.012933333, 0.012833334, 0.0127}'::real[],
	'{0.023301385}'::real[],
	NULL::real[],
	NULL::real[],
	NULL::real[],
	E'{"1940","1978","1935","1970","1955","1957","1959","1990","1988","1925","1956","1987","1972","1985","1946","1926","1932","1933","1929","1979","1968","1930","1951","1961","1975","1966","1953","1971","1939","1977","1941","1991","1960","1937","1948","1952","1927","1938","1962","1964","1976","1958","1986","1924","1928","1965","1983","1942","1963","1931","1944","1945","1984","1934","1954","1947","1980","1981","1989","1982","1950","1969","1973","1974","1936","1949","1943","1967","1992"}'::int4[],
	NULL::int4[],
	NULL::int4[],
	NULL::int4[],
	NULL::int4[]);

--
-- Table: customer, Attribute: c_birth_country
--
-- DELETE FROM pg_statistic WHERE starelid='cte_bug.customer'::regclass AND staattnum=15;
INSERT INTO pg_statistic VALUES (
	'cte_bug.customer'::regclass,
	15::smallint,
	False::boolean,
	0.035633333::real,
	9::integer,
	211.0::real,
	1::smallint,
	2::smallint,
	3::smallint,
	0::smallint,
	0::smallint,
	98::oid,
	664::oid,
	664::oid,
	0::oid,
	0::oid,
	100::oid,
	100::oid,
	100::oid,
	0::oid,
	0::oid,
	'{0.0056666667, 0.0055333334}'::real[],
	NULL::real[],
	'{0.013934988}'::real[],
	NULL::real[],
	NULL::real[],
	E'{"GEORGIA","GREENLAND"}'::varchar[],
	E'{"AFGHANISTAN","ALBANIA","AMERICAN SAMOA","ANGOLA","ANGUILLA","ARGENTINA","ARUBA","AUSTRIA","BAHAMAS","BANGLADESH","BELARUS","BELIZE","BERMUDA","BOLIVIA","BOUVET ISLAND","BRUNEI DARUSSALAM","BURUNDI","CAMBODIA","CANADA","CAYMAN ISLANDS","CHRISTMAS ISLAND","COSTA RICA","CROATIA","CYPRUS","DENMARK","DOMINICA","EGYPT","EQUATORIAL GUINEA","ETHIOPIA","FIJI","FRANCE","FRENCH POLYNESIA","GAMBIA","GHANA","GREECE","GUADELOUPE","GUATEMALA","GUINEA","HAITI","HONDURAS","ICELAND","INDONESIA","IRELAND","ISRAEL","JAMAICA","JERSEY","KAZAKHSTAN","KIRIBATI","KUWAIT","LEBANON","LIBERIA","LITHUANIA","LUXEMBOURG","MALAWI","MALDIVES","MALTA","MARTINIQUE","MAURITIUS","MEXICO","MONACO","MONTENEGRO","MOROCCO","NAMIBIA","NEPAL","NETHERLANDS ANTILLES","NEW ZEALAND","NIGER","NIUE","NORWAY","PALAU","PAPUA NEW GUINEA","PERU","PITCAIRN","PORTUGAL","QATAR","ROMANIA","RWANDA","SAMOA","SAUDI ARABIA","SERBIA","SIERRA LEONE","SLOVAKIA","SOLOMON ISLANDS","SOUTH AFRICA","SRI LANKA","SURINAME","SWEDEN","SYRIAN ARAB REPUBLIC","THAILAND","TOGO","TONGA","TUNISIA","TUVALU","UKRAINE","UNITED KINGDOM","URUGUAY","VANUATU","VIET NAM","WALLIS AND FUTUNA","YEMEN","ZIMBABWE"}'::varchar[],
	NULL::varchar[],
	NULL::varchar[],
	NULL::varchar[]);

--
-- Table: customer, Attribute: c_login
--
-- DELETE FROM pg_statistic WHERE starelid='cte_bug.customer'::regclass AND staattnum=16;
INSERT INTO pg_statistic VALUES (
	'cte_bug.customer'::regclass,
	16::smallint,
	False::boolean,
	1.0::real,
	0::integer,
	0.0::real,
	0::smallint,
	0::smallint,
	0::smallint,
	0::smallint,
	0::smallint,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	NULL::real[],
	NULL::real[],
	NULL::real[],
	NULL::real[],
	NULL::real[],
	NULL::varchar[],
	NULL::varchar[],
	NULL::varchar[],
	NULL::varchar[],
	NULL::varchar[]);

--
-- Table: customer, Attribute: c_email_address
--
-- DELETE FROM pg_statistic WHERE starelid='cte_bug.customer'::regclass AND staattnum=17;
INSERT INTO pg_statistic VALUES (
	'cte_bug.customer'::regclass,
	17::smallint,
	False::boolean,
	0.03596667::real,
	28::integer,
	-0.9640333::real,
	2::smallint,
	3::smallint,
	0::smallint,
	0::smallint,
	0::smallint,
	664::oid,
	664::oid,
	0::oid,
	0::oid,
	0::oid,
	100::oid,
	100::oid,
	0::oid,
	0::oid,
	0::oid,
	NULL::real[],
	'{-0.00065877277}'::real[],
	NULL::real[],
	NULL::real[],
	NULL::real[],
	E'{"Aaron.Allen@QVp8KYxoqS.edu","Albert.Mcnabb@JqFg1Vfr.edu","Allen.Blackmon@rDQezy0Iy.org","Amy.Moore@lIXLGiGxl.com","Angela.Perez@9CODy4qpPeA8HCLX.com","Annie.Anderson@D1dOEV8V7YH.edu","April.Sanchez@MlJE7Jyr1KnbMizt8.com","Barbara.Bird@Bsz4Y8vF.edu","Bennie.Mcdonald@8a.com","Beverly.Armstrong@QKMtgEU.edu","Brady.Pacheco@V3G7zDs.org","Brian.Otto@5Ue35SXHl043P.org","Candy.Cox@BmsyXv426e.edu","Casey.Steele@TkMmIusn.com","Charles.Brito@qPU9PAt6xF.com","Chelsey.Gil@U7R6AeIVeQNbM.org","Christopher.Caraway@baXEFsAXkiXc4H.com","Clarence.Martinez@Tjom5JSSLsiqH.edu","Corinne.Blount@gG8DjsIs77.edu","Dale.Peterson@UP1FQ.com","Danny.Rudd@Qj.edu","David.Hernandez@XvDstf8oVspNVEq9N2ek.edu","Dawn.Washington@UZL4A2cTelj.org","Delorse.Mann@p1.edu","Diane.Ruiz@tq8V3.edu","Dona.Rucker@U5tPkxBoh3Nvvr.edu","Dorothy.Ledoux@6UXxuie.org","Ebony.Straub@O0ny7G.edu","Edwin.Brown@jifz5d3.com","Elizabeth.Rowe@x7a.com","Erica.Stewart@MjVPLVkiTTx1k9xvn.com","Ethel.Bowers@BchSkuO.edu","Felisha.White@4KHiD3EfpI.org","Fred.Burns@YAxMo0quQt02.org","Gaynell.Duran@PP5khP2OBXNnz.edu","Gerald.Olivas@Hs63PKrg.org","Goldie.Costello@74uH9.com","Harold.Mckinney@0Ymy5L.edu","Helene.Lockhart@4guNOhMAJ.org","Howard.Clarkson@KEs7Z6hdVT5d.edu","Jack.Davis@D7dTycb6ort.org","James.Cockrell@5lqoIx4paJn9P.org","James.Richter@7JahtLhEvS.org","Janice.Choate@ItXxi6VjtefUmkF.org","Jeanette.Mathews@mXCyZTs8MaGK9.org","Jennifer.Emerson@LADJl.com","Jerry.Willoughby@4h6L6um1aOm0JTk.edu","Joanna.French@Dn.com","John.Byrd@xAeLnuIQqDO4V.edu","John.Nail@vdm7UXfYYeeR5.org","Jonathan.Cochran@a1YLy7.com","Josephine.Barton@jbuycDaa1Dih2f6TbHs.org","Joshua.Trent@P5VKT8eLAEqph7.org","Julia.Mcneil@UXUiYMm4Rf1r1ysly2.com","Karen.Salmon@hVtuyn2.org","Kathy.Roush@f1.com","Kenneth.Nolen@fXIxCA8ecA7kUe.edu","Kimberly.Ward@UjmN1i5S0HqjOSB6P.org","Larry.Mccollum@KM3A2VsKR51LR7aSC2.edu","Leann.Lindsay@CYHO5.org","Lillian.Gibbons@ex2.edu","Lisa.Kaiser@2.edu","Lorraine.Wasson@fxUN1RDnuPC.com","Lyn.Green@ab9AnRe6OP.edu","Margaret.Childers@I2BJusIOR.org","Marilyn.Cochran@fXUsGKiG2pQn5p.org","Mark.Townsend@QRk6f4vPmjzGsd.edu","Matthew.Brown@BbgxEpsOa2.edu","Melissa.Brown@tU.org","Michael.Evans@na3Fzya.edu","Michael.Vickery@tGpS.edu","Millie.Wood@CYrA5ZCpBUxN.com","Nancy.Hubbard@1Dij5VOarSDD.com","Nicole.Cartwright@XVpmQfap67h39R.edu","Otha.Mathews@Q.edu","Paul.Bernard@l8hNZT.com","Peter.Bean@1hcfjEMbL.com","Rafael.Walker@DdbVsZ48tXFR.org","Raymond.Willis@FuCYxX.org","Richard.Blue@hk6sJl.org","Richard.Walker@QkOyOiijkoT6tALu.org","Robert.Feliciano@zrxHvBIUT1.org","Robert.Schrader@84hFFO.edu","Ronald.Barajas@SmV.edu","Rose.Rivera@nI5i3amDP9e.com","Ruth.Noble@nsRPMH7imQms.edu","Sandra.Ngo@NpRHjOrv.org","Sean.Young@cSmSeTb.edu","Shelly.Prentice@fOdVDfc3l.org","Stephanie.Maupin@1aO67HVBb4t.org","Stuart.Mintz@sCH9lO.com","Tamika.Walker@LypSN7cml.com","Terry.Pence@9V95I2n.edu","Thomas.Logan@62ZLdPqs6.edu","Tina.Fry@ggnu7.com","Troy.Hunt@HEIC1huvlV2.com","Vicky.Smith@7NTjUV1y.com","Walter.Guinn@q0V9yqO.com","William.Biddle@X3bGbi3YEGk.edu","William.Silva@OsUh7.edu","Zulma.Williams@NNUybLiKUcP.com"}'::varchar[],
	NULL::varchar[],
	NULL::varchar[],
	NULL::varchar[],
	NULL::varchar[]);

--
-- Table: customer, Attribute: c_last_review_date
--
-- DELETE FROM pg_statistic WHERE starelid='cte_bug.customer'::regclass AND staattnum=18;
INSERT INTO pg_statistic VALUES (
	'cte_bug.customer'::regclass,
	18::smallint,
	False::boolean,
	0.03643333::real,
	4::integer,
	366.0::real,
	1::smallint,
	2::smallint,
	3::smallint,
	0::smallint,
	0::smallint,
	96::oid,
	97::oid,
	97::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	'{0.0033333334, 0.0033333334}'::real[],
	NULL::real[],
	'{-0.010210618}'::real[],
	NULL::real[],
	NULL::real[],
	E'{"2452342","2452574"}'::int4[],
	E'{"2452283","2452286","2452290","2452293","2452297","2452301","2452304","2452308","2452312","2452315","2452318","2452322","2452326","2452329","2452333","2452337","2452341","2452345","2452349","2452352","2452356","2452360","2452363","2452367","2452371","2452374","2452378","2452382","2452386","2452389","2452393","2452397","2452400","2452404","2452408","2452411","2452414","2452418","2452422","2452425","2452429","2452433","2452437","2452440","2452444","2452447","2452451","2452455","2452458","2452462","2452465","2452469","2452473","2452476","2452480","2452483","2452487","2452491","2452494","2452498","2452501","2452505","2452508","2452512","2452515","2452519","2452523","2452527","2452531","2452535","2452538","2452542","2452545","2452549","2452553","2452556","2452560","2452563","2452568","2452571","2452576","2452579","2452583","2452587","2452591","2452594","2452598","2452601","2452605","2452608","2452612","2452616","2452619","2452623","2452626","2452630","2452634","2452638","2452641","2452645","2452648"}'::int4[],
	NULL::int4[],
	NULL::int4[],
	NULL::int4[]);

--
-- Table: date_dim, Attribute: d_date_sk
--
-- DELETE FROM pg_statistic WHERE starelid='cte_bug.date_dim'::regclass AND staattnum=1;
INSERT INTO pg_statistic VALUES (
	'cte_bug.date_dim'::regclass,
	1::smallint,
	False::boolean,
	0.0::real,
	4::integer,
	-1.0::real,
	2::smallint,
	3::smallint,
	0::smallint,
	0::smallint,
	0::smallint,
	97::oid,
	97::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	NULL::real[],
	'{1.0}'::real[],
	NULL::real[],
	NULL::real[],
	NULL::real[],
	E'{"2415027","2415771","2416520","2417211","2417903","2418630","2419355","2420088","2420908","2421635","2422337","2423125","2423817","2424576","2425331","2426034","2426734","2427456","2428161","2428886","2429586","2430281","2431030","2431747","2432487","2433188","2433919","2434660","2435416","2436096","2436816","2437551","2438254","2438932","2439677","2440414","2441121","2441884","2442643","2443351","2444066","2444821","2445520","2446305","2447089","2447791","2448548","2449230","2449930","2450652","2451408","2452175","2452928","2453663","2454412","2455152","2455942","2456650","2457349","2458087","2458835","2459531","2460255","2460995","2461677","2462381","2463126","2463880","2464613","2465432","2466146","2466884","2467605","2468353","2469108","2469764","2470509","2471242","2471960","2472732","2473465","2474220","2474963","2475782","2476553","2477254","2477964","2478666","2479406","2480132","2480928","2481630","2482367","2483101","2483817","2484513","2485191","2485905","2486655","2487351","2488066"}'::int4[],
	NULL::int4[],
	NULL::int4[],
	NULL::int4[],
	NULL::int4[]);

--
-- Table: date_dim, Attribute: d_date_id
--
-- DELETE FROM pg_statistic WHERE starelid='cte_bug.date_dim'::regclass AND staattnum=2;
INSERT INTO pg_statistic VALUES (
	'cte_bug.date_dim'::regclass,
	2::smallint,
	False::boolean,
	0.0::real,
	17::integer,
	-1.0::real,
	2::smallint,
	3::smallint,
	0::smallint,
	0::smallint,
	0::smallint,
	664::oid,
	664::oid,
	0::oid,
	0::oid,
	0::oid,
	100::oid,
	100::oid,
	0::oid,
	0::oid,
	0::oid,
	NULL::real[],
	'{-0.004197001}'::real[],
	NULL::real[],
	NULL::real[],
	NULL::real[],
	E'{"AAAAAAAAAAABFCAA","AAAAAAAAACLOFCAA","AAAAAAAAAFFCFCAA","AAAAAAAAAHNPECAA","AAAAAAAAAKHDFCAA","AAAAAAAAAMPOECAA","AAAAAAAAAPIBFCAA","AAAAAAAABBPBFCAA","AAAAAAAABEFFFCAA","AAAAAAAABGOIFCAA","AAAAAAAABJKKFCAA","AAAAAAAABMDCFCAA","AAAAAAAABONBFCAA","AAAAAAAACBFDFCAA","AAAAAAAACDNAFCAA","AAAAAAAACGHOFCAA","AAAAAAAACJCMFCAA","AAAAAAAACLLAFCAA","AAAAAAAACOBLFCAA","AAAAAAAADAMDFCAA","AAAAAAAADDFPECAA","AAAAAAAADGAOFCAA","AAAAAAAADIKLFCAA","AAAAAAAADLDCFCAA","AAAAAAAADOCDFCAA","AAAAAAAAEAKPECAA","AAAAAAAAEDEAFCAA","AAAAAAAAEFPHFCAA","AAAAAAAAEILJFCAA","AAAAAAAAELGOECAA","AAAAAAAAENOOFCAA","AAAAAAAAFAFPECAA","AAAAAAAAFCPJFCAA","AAAAAAAAFFGHFCAA","AAAAAAAAFHMOECAA","AAAAAAAAFKEKFCAA","AAAAAAAAFMMJFCAA","AAAAAAAAFPCKFCAA","AAAAAAAAGBKJFCAA","AAAAAAAAGEDOECAA","AAAAAAAAGGOAFCAA","AAAAAAAAGJKHFCAA","AAAAAAAAGMEBFCAA","AAAAAAAAGONOFCAA","AAAAAAAAHBJHFCAA","AAAAAAAAHECHFCAA","AAAAAAAAHGLOECAA","AAAAAAAAHJFDFCAA","AAAAAAAAHLOBFCAA","AAAAAAAAHOFIFCAA","AAAAAAAAIANCFCAA","AAAAAAAAIDFPECAA","AAAAAAAAIFMFFCAA","AAAAAAAAIIGHFCAA","AAAAAAAAILABFCAA","AAAAAAAAINJEFCAA","AAAAAAAAJADHFCAA","AAAAAAAAJCLBFCAA","AAAAAAAAJFDCFCAA","AAAAAAAAJHLMFCAA","AAAAAAAAJKDBFCAA","AAAAAAAAJMKOFCAA","AAAAAAAAJPCHFCAA","AAAAAAAAKBMIFCAA","AAAAAAAAKEEPECAA","AAAAAAAAKGODFCAA","AAAAAAAAKJGPECAA","AAAAAAAAKLMAFCAA","AAAAAAAAKOFCFCAA","AAAAAAAALBAAFCAA","AAAAAAAALDKOECAA","AAAAAAAALGBIFCAA","AAAAAAAALIGMFCAA","AAAAAAAALKMOECAA","AAAAAAAALNENFCAA","AAAAAAAALPMOFCAA","AAAAAAAAMCFFFCAA","AAAAAAAAMELFFCAA","AAAAAAAAMHFJFCAA","AAAAAAAAMJOIFCAA","AAAAAAAAMMHDFCAA","AAAAAAAAMPBJFCAA","AAAAAAAANBKMFCAA","AAAAAAAANEHIFCAA","AAAAAAAANGMPECAA","AAAAAAAANJIAFCAA","AAAAAAAANMDDFCAA","AAAAAAAANOPGFCAA","AAAAAAAAOBHNFCAA","AAAAAAAAOEAPFCAA","AAAAAAAAOGKIFCAA","AAAAAAAAOJBGFCAA","AAAAAAAAOLIJFCAA","AAAAAAAAOOALFCAA","AAAAAAAAPAJMFCAA","AAAAAAAAPDCOECAA","AAAAAAAAPFLAFCAA","AAAAAAAAPIECFCAA","AAAAAAAAPKNIFCAA","AAAAAAAAPNFPFCAA","AAAAAAAAPPPOFCAA"}'::varchar[],
	NULL::varchar[],
	NULL::varchar[],
	NULL::varchar[],
	NULL::varchar[]);

--
-- Table: date_dim, Attribute: d_date
--
-- DELETE FROM pg_statistic WHERE starelid='cte_bug.date_dim'::regclass AND staattnum=3;
INSERT INTO pg_statistic VALUES (
	'cte_bug.date_dim'::regclass,
	3::smallint,
	False::boolean,
	0.0::real,
	4::integer,
	-1.0::real,
	2::smallint,
	3::smallint,
	0::smallint,
	0::smallint,
	0::smallint,
	1095::oid,
	1095::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	NULL::real[],
	'{1.0}'::real[],
	NULL::real[],
	NULL::real[],
	NULL::real[],
	E'{"1900-01-07","1902-01-21","1904-02-09","1905-12-31","1907-11-23","1909-11-19","1911-11-14","1913-11-16","1916-02-14","1918-02-10","1920-01-13","1922-03-11","1924-02-01","1926-03-01","1928-03-25","1930-02-26","1932-01-27","1934-01-18","1935-12-24","1937-12-18","1939-11-18","1941-10-13","1943-11-01","1945-10-18","1947-10-28","1949-09-28","1951-09-29","1953-10-09","1955-11-04","1957-09-14","1959-09-04","1961-09-08","1963-08-12","1965-06-20","1967-07-05","1969-07-11","1971-06-18","1973-07-20","1975-08-18","1977-07-26","1979-07-11","1981-08-04","1983-07-04","1985-08-27","1987-10-20","1989-09-21","1991-10-18","1993-08-30","1995-07-31","1997-07-22","1999-08-17","2001-09-22","2003-10-15","2005-10-19","2007-11-07","2009-11-16","2012-01-15","2013-12-23","2015-11-22","2017-11-29","2019-12-17","2021-11-12","2023-11-06","2025-11-15","2027-09-28","2029-09-01","2031-09-16","2033-10-09","2035-10-12","2038-01-08","2039-12-23","2041-12-30","2043-12-21","2046-01-07","2048-02-01","2049-11-18","2051-12-03","2053-12-05","2055-11-23","2058-01-03","2060-01-06","2062-01-30","2064-02-12","2066-05-11","2068-06-20","2070-05-22","2072-05-01","2074-04-03","2076-04-12","2078-04-08","2080-06-12","2082-05-15","2084-05-21","2086-05-25","2088-05-10","2090-04-06","2092-02-13","2094-01-27","2096-02-16","2098-01-12","2099-12-28"}'::date[],
	NULL::date[],
	NULL::date[],
	NULL::date[],
	NULL::date[]);

--
-- Table: date_dim, Attribute: d_month_seq
--
-- DELETE FROM pg_statistic WHERE starelid='cte_bug.date_dim'::regclass AND staattnum=4;
INSERT INTO pg_statistic VALUES (
	'cte_bug.date_dim'::regclass,
	4::smallint,
	False::boolean,
	0.0::real,
	4::integer,
	2400.0::real,
	1::smallint,
	2::smallint,
	3::smallint,
	0::smallint,
	0::smallint,
	96::oid,
	97::oid,
	97::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	'{0.00073333335, 0.0007, 0.00066666666, 0.00066666666, 0.00066666666, 0.00066666666, 0.00066666666, 0.00066666666, 0.00066666666, 0.00066666666, 0.00066666666, 0.00066666666}'::real[],
	NULL::real[],
	'{1.0}'::real[],
	NULL::real[],
	NULL::real[],
	E'{"979","1761","198","288","356","445","552","594","751","941","1154","1251"}'::int4[],
	E'{"0","24","48","71","94","117","141","164","191","217","239","265","289","314","339","363","386","409","432","458","480","503","528","551","577","601","625","649","675","695","719","743","768","790","814","838","861","885","911","934","959","985","1009","1034","1059","1083","1106","1130","1152","1176","1201","1226","1252","1277","1301","1326","1351","1373","1396","1421","1444","1467","1491","1515","1537","1560","1585","1609","1633","1661","1684","1707","1731","1755","1781","1803","1827","1851","1874","1900","1923","1948","1973","1999","2024","2048","2070","2094","2117","2141","2167","2190","2213","2238","2261","2283","2306","2329","2354","2376","2399"}'::int4[],
	NULL::int4[],
	NULL::int4[],
	NULL::int4[]);

--
-- Table: date_dim, Attribute: d_week_seq
--
-- DELETE FROM pg_statistic WHERE starelid='cte_bug.date_dim'::regclass AND staattnum=5;
INSERT INTO pg_statistic VALUES (
	'cte_bug.date_dim'::regclass,
	5::smallint,
	False::boolean,
	0.0::real,
	4::integer,
	-0.14316417::real,
	2::smallint,
	3::smallint,
	0::smallint,
	0::smallint,
	0::smallint,
	97::oid,
	97::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	NULL::real[],
	'{1.0}'::real[],
	NULL::real[],
	NULL::real[],
	NULL::real[],
	E'{"1","108","215","313","412","516","620","724","841","945","1046","1158","1257","1365","1473","1574","1674","1777","1878","1981","2081","2180","2287","2390","2496","2596","2700","2806","2914","3011","3114","3219","3319","3416","3523","3628","3729","3838","3946","4048","4150","4258","4357","4470","4582","4682","4790","4887","4987","5091","5199","5308","5416","5521","5628","5733","5846","5947","6047","6153","6260","6359","6462","6568","6666","6766","6873","6980","7085","7202","7304","7409","7512","7619","7727","7821","7927","8032","8135","8245","8350","8457","8564","8681","8791","8891","8992","9093","9198","9302","9416","9516","9621","9726","9828","9928","10025","10127","10234","10333","10435"}'::int4[],
	NULL::int4[],
	NULL::int4[],
	NULL::int4[],
	NULL::int4[]);

--
-- Table: date_dim, Attribute: d_quarter_seq
--
-- DELETE FROM pg_statistic WHERE starelid='cte_bug.date_dim'::regclass AND staattnum=6;
INSERT INTO pg_statistic VALUES (
	'cte_bug.date_dim'::regclass,
	6::smallint,
	False::boolean,
	0.0::real,
	4::integer,
	801.0::real,
	1::smallint,
	2::smallint,
	3::smallint,
	0::smallint,
	0::smallint,
	96::oid,
	97::oid,
	97::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	'{0.0018, 0.0018, 0.0017333333}'::real[],
	NULL::real[],
	'{1.0}'::real[],
	NULL::real[],
	NULL::real[],
	E'{"141","754","588"}'::int4[],
	E'{"1","9","17","25","32","40","48","56","65","73","81","89","97","105","113","121","128","136","145","153","161","169","177","184","192","200","208","216","225","232","240","248","255","263","271","279","287","295","303","311","319","327","335","343","352","359","367","375","383","390","399","407","415","423","431","439","448","456","463","471","480","487","495","503","511","518","526","534","543","551","559","567","575","583","593","600","608","616","624","632","640","648","657","665","674","682","689","697","705","713","721","729","737","745","753","762","769","777","786","793","801"}'::int4[],
	NULL::int4[],
	NULL::int4[],
	NULL::int4[]);

--
-- Table: date_dim, Attribute: d_year
--
-- DELETE FROM pg_statistic WHERE starelid='cte_bug.date_dim'::regclass AND staattnum=7;
INSERT INTO pg_statistic VALUES (
	'cte_bug.date_dim'::regclass,
	7::smallint,
	False::boolean,
	0.0::real,
	4::integer,
	200.0::real,
	1::smallint,
	2::smallint,
	3::smallint,
	0::smallint,
	0::smallint,
	96::oid,
	97::oid,
	97::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	'{0.0060666665, 0.006, 0.0058333334, 0.0057666665, 0.0056666667}'::real[],
	NULL::real[],
	'{1.0}'::real[],
	NULL::real[],
	NULL::real[],
	E'{"1957","2088","1929","1907","2027"}'::int4[],
	E'{"1900","1902","1904","1905","1908","1910","1912","1914","1916","1918","1920","1922","1924","1926","1928","1931","1933","1935","1937","1939","1940","1942","1944","1946","1948","1950","1952","1954","1956","1959","1961","1963","1965","1967","1969","1970","1972","1974","1976","1978","1980","1982","1984","1986","1988","1990","1992","1994","1996","1998","2000","2002","2004","2006","2008","2010","2012","2014","2016","2017","2019","2021","2023","2025","2028","2030","2032","2034","2036","2038","2040","2042","2044","2046","2048","2050","2052","2054","2055","2058","2059","2061","2064","2066","2068","2070","2072","2073","2075","2077","2079","2081","2083","2085","2087","2090","2092","2094","2096","2098","2099"}'::int4[],
	NULL::int4[],
	NULL::int4[],
	NULL::int4[]);

--
-- Table: date_dim, Attribute: d_dow
--
-- DELETE FROM pg_statistic WHERE starelid='cte_bug.date_dim'::regclass AND staattnum=8;
INSERT INTO pg_statistic VALUES (
	'cte_bug.date_dim'::regclass,
	8::smallint,
	False::boolean,
	0.0::real,
	4::integer,
	7.0::real,
	1::smallint,
	3::smallint,
	0::smallint,
	0::smallint,
	0::smallint,
	96::oid,
	97::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	'{0.14473334, 0.14406666, 0.14326666, 0.14316666, 0.14263333, 0.14153333, 0.1406}'::real[],
	'{0.13635832}'::real[],
	NULL::real[],
	NULL::real[],
	NULL::real[],
	E'{"2","3","1","4","0","6","5"}'::int4[],
	NULL::int4[],
	NULL::int4[],
	NULL::int4[],
	NULL::int4[]);

--
-- Table: date_dim, Attribute: d_moy
--
-- DELETE FROM pg_statistic WHERE starelid='cte_bug.date_dim'::regclass AND staattnum=9;
INSERT INTO pg_statistic VALUES (
	'cte_bug.date_dim'::regclass,
	9::smallint,
	False::boolean,
	0.0::real,
	4::integer,
	12.0::real,
	1::smallint,
	3::smallint,
	0::smallint,
	0::smallint,
	0::smallint,
	96::oid,
	97::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	'{0.085533336, 0.085433334, 0.0849, 0.0846, 0.084366664, 0.08426667, 0.08393333, 0.08316667, 0.0829, 0.082, 0.08183333, 0.07706667}'::real[],
	'{0.088353634}'::real[],
	NULL::real[],
	NULL::real[],
	NULL::real[],
	E'{"3","5","10","12","1","7","6","8","9","11","4","2"}'::int4[],
	NULL::int4[],
	NULL::int4[],
	NULL::int4[],
	NULL::int4[]);

--
-- Table: date_dim, Attribute: d_dom
--
-- DELETE FROM pg_statistic WHERE starelid='cte_bug.date_dim'::regclass AND staattnum=10;
INSERT INTO pg_statistic VALUES (
	'cte_bug.date_dim'::regclass,
	10::smallint,
	False::boolean,
	0.0::real,
	4::integer,
	31.0::real,
	1::smallint,
	3::smallint,
	0::smallint,
	0::smallint,
	0::smallint,
	96::oid,
	97::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	'{0.0351, 0.0338, 0.03376667, 0.033733334, 0.033733334, 0.033633333, 0.033466667, 0.033466667, 0.03313333, 0.0331, 0.032966666, 0.032966666, 0.032966666, 0.0329, 0.032866668, 0.032766666, 0.0327, 0.0326, 0.0325, 0.0325, 0.0324, 0.032366667, 0.032333333, 0.032266665, 0.032133333, 0.032, 0.031533334, 0.030833334, 0.0305, 0.0304, 0.018566666}'::real[],
	'{0.03590256}'::real[],
	NULL::real[],
	NULL::real[],
	NULL::real[],
	E'{"13","23","22","1","24","5","7","16","26","21","14","17","28","20","6","2","9","10","12","19","18","8","15","27","4","25","11","30","29","3","31"}'::int4[],
	NULL::int4[],
	NULL::int4[],
	NULL::int4[],
	NULL::int4[]);

--
-- Table: date_dim, Attribute: d_qoy
--
-- DELETE FROM pg_statistic WHERE starelid='cte_bug.date_dim'::regclass AND staattnum=11;
INSERT INTO pg_statistic VALUES (
	'cte_bug.date_dim'::regclass,
	11::smallint,
	False::boolean,
	0.0::real,
	4::integer,
	4.0::real,
	1::smallint,
	3::smallint,
	0::smallint,
	0::smallint,
	0::smallint,
	96::oid,
	97::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	'{0.25106665, 0.25043333, 0.2494, 0.2491}'::real[],
	'{0.25712454}'::real[],
	NULL::real[],
	NULL::real[],
	NULL::real[],
	E'{"2","3","4","1"}'::int4[],
	NULL::int4[],
	NULL::int4[],
	NULL::int4[],
	NULL::int4[]);

--
-- Table: date_dim, Attribute: d_fy_year
--
-- DELETE FROM pg_statistic WHERE starelid='cte_bug.date_dim'::regclass AND staattnum=12;
INSERT INTO pg_statistic VALUES (
	'cte_bug.date_dim'::regclass,
	12::smallint,
	False::boolean,
	0.0::real,
	4::integer,
	200.0::real,
	1::smallint,
	2::smallint,
	3::smallint,
	0::smallint,
	0::smallint,
	96::oid,
	97::oid,
	97::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	'{0.0060666665, 0.006, 0.0058333334, 0.0057666665, 0.0056666667}'::real[],
	NULL::real[],
	'{1.0}'::real[],
	NULL::real[],
	NULL::real[],
	E'{"1957","2088","1929","1907","2027"}'::int4[],
	E'{"1900","1902","1904","1905","1908","1910","1912","1914","1916","1918","1920","1922","1924","1926","1928","1931","1933","1935","1937","1939","1940","1942","1944","1946","1948","1950","1952","1954","1956","1959","1961","1963","1965","1967","1969","1970","1972","1974","1976","1978","1980","1982","1984","1986","1988","1990","1992","1994","1996","1998","2000","2002","2004","2006","2008","2010","2012","2014","2016","2017","2019","2021","2023","2025","2028","2030","2032","2034","2036","2038","2040","2042","2044","2046","2048","2050","2052","2054","2055","2058","2059","2061","2064","2066","2068","2070","2072","2073","2075","2077","2079","2081","2083","2085","2087","2090","2092","2094","2096","2098","2099"}'::int4[],
	NULL::int4[],
	NULL::int4[],
	NULL::int4[]);

--
-- Table: date_dim, Attribute: d_fy_quarter_seq
--
-- DELETE FROM pg_statistic WHERE starelid='cte_bug.date_dim'::regclass AND staattnum=13;
INSERT INTO pg_statistic VALUES (
	'cte_bug.date_dim'::regclass,
	13::smallint,
	False::boolean,
	0.0::real,
	4::integer,
	801.0::real,
	1::smallint,
	2::smallint,
	3::smallint,
	0::smallint,
	0::smallint,
	96::oid,
	97::oid,
	97::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	'{0.0018, 0.0018, 0.0017333333}'::real[],
	NULL::real[],
	'{1.0}'::real[],
	NULL::real[],
	NULL::real[],
	E'{"141","754","588"}'::int4[],
	E'{"1","9","17","25","32","40","48","56","65","73","81","89","97","105","113","121","128","136","145","153","161","169","177","184","192","200","208","216","225","232","240","248","255","263","271","279","287","295","303","311","319","327","335","343","352","359","367","375","383","390","399","407","415","423","431","439","448","456","463","471","480","487","495","503","511","518","526","534","543","551","559","567","575","583","593","600","608","616","624","632","640","648","657","665","674","682","689","697","705","713","721","729","737","745","753","762","769","777","786","793","801"}'::int4[],
	NULL::int4[],
	NULL::int4[],
	NULL::int4[]);

--
-- Table: date_dim, Attribute: d_fy_week_seq
--
-- DELETE FROM pg_statistic WHERE starelid='cte_bug.date_dim'::regclass AND staattnum=14;
INSERT INTO pg_statistic VALUES (
	'cte_bug.date_dim'::regclass,
	14::smallint,
	False::boolean,
	0.0::real,
	4::integer,
	-0.14316417::real,
	2::smallint,
	3::smallint,
	0::smallint,
	0::smallint,
	0::smallint,
	97::oid,
	97::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	NULL::real[],
	'{1.0}'::real[],
	NULL::real[],
	NULL::real[],
	NULL::real[],
	E'{"1","108","215","313","412","516","620","724","841","945","1046","1158","1257","1365","1473","1574","1674","1777","1878","1981","2081","2180","2287","2390","2496","2596","2700","2806","2914","3011","3114","3219","3319","3416","3523","3628","3729","3838","3946","4048","4150","4258","4357","4470","4582","4682","4790","4887","4987","5091","5199","5308","5416","5521","5628","5733","5846","5947","6047","6153","6260","6359","6462","6568","6666","6766","6873","6980","7085","7202","7304","7409","7512","7619","7727","7821","7927","8032","8135","8245","8350","8457","8564","8681","8791","8891","8992","9093","9198","9302","9416","9516","9621","9726","9828","9928","10025","10127","10234","10333","10435"}'::int4[],
	NULL::int4[],
	NULL::int4[],
	NULL::int4[],
	NULL::int4[]);

--
-- Table: date_dim, Attribute: d_day_name
--
-- DELETE FROM pg_statistic WHERE starelid='cte_bug.date_dim'::regclass AND staattnum=15;
INSERT INTO pg_statistic VALUES (
	'cte_bug.date_dim'::regclass,
	15::smallint,
	False::boolean,
	0.0::real,
	8::integer,
	7.0::real,
	1::smallint,
	3::smallint,
	0::smallint,
	0::smallint,
	0::smallint,
	98::oid,
	664::oid,
	0::oid,
	0::oid,
	0::oid,
	100::oid,
	100::oid,
	0::oid,
	0::oid,
	0::oid,
	'{0.14473334, 0.14406666, 0.14326666, 0.14316666, 0.14263333, 0.14153333, 0.1406}'::real[],
	'{0.14524722}'::real[],
	NULL::real[],
	NULL::real[],
	NULL::real[],
	E'{"Tuesday","Wednesday","Monday","Thursday","Sunday","Saturday","Friday"}'::varchar[],
	NULL::varchar[],
	NULL::varchar[],
	NULL::varchar[],
	NULL::varchar[]);

--
-- Table: date_dim, Attribute: d_quarter_name
--
-- DELETE FROM pg_statistic WHERE starelid='cte_bug.date_dim'::regclass AND staattnum=16;
INSERT INTO pg_statistic VALUES (
	'cte_bug.date_dim'::regclass,
	16::smallint,
	False::boolean,
	0.0::real,
	7::integer,
	800.0::real,
	1::smallint,
	2::smallint,
	3::smallint,
	0::smallint,
	0::smallint,
	98::oid,
	664::oid,
	664::oid,
	0::oid,
	0::oid,
	100::oid,
	100::oid,
	100::oid,
	0::oid,
	0::oid,
	'{0.0017666667, 0.0016666667, 0.0016333334, 0.0016333334, 0.0016333334, 0.0016333334, 0.0016333334, 0.0016333334}'::real[],
	NULL::real[],
	'{1.0}'::real[],
	NULL::real[],
	NULL::real[],
	E'{"2088Q2","1957Q2","1907Q3","1937Q1","2049Q2","2051Q2","2055Q2","2096Q4"}'::varchar[],
	E'{"1900Q1","1902Q1","1904Q1","1905Q4","1908Q1","1910Q1","1912Q1","1913Q4","1916Q1","1918Q1","1920Q1","1922Q1","1924Q1","1926Q1","1928Q1","1930Q1","1931Q4","1933Q4","1935Q4","1938Q1","1940Q1","1941Q4","1943Q4","1945Q4","1947Q4","1949Q3","1951Q3","1953Q3","1955Q4","1957Q4","1959Q4","1961Q4","1963Q4","1965Q3","1967Q3","1969Q3","1971Q3","1973Q3","1975Q3","1977Q3","1979Q2","1981Q2","1983Q2","1985Q2","1987Q3","1989Q3","1991Q3","1993Q2","1995Q2","1997Q1","1999Q2","2001Q2","2003Q2","2005Q2","2007Q2","2009Q2","2011Q3","2013Q2","2015Q2","2017Q2","2019Q2","2021Q1","2023Q1","2025Q1","2027Q1","2028Q4","2030Q4","2032Q4","2034Q4","2037Q1","2039Q1","2041Q1","2043Q1","2045Q1","2046Q4","2049Q1","2051Q1","2053Q3","2055Q4","2057Q4","2059Q4","2061Q4","2063Q4","2065Q4","2068Q1","2070Q1","2072Q1","2073Q4","2075Q4","2077Q4","2079Q4","2081Q4","2083Q4","2085Q4","2087Q4","2090Q1","2091Q4","2093Q4","2095Q4","2098Q1","2099Q4"}'::varchar[],
	NULL::varchar[],
	NULL::varchar[],
	NULL::varchar[]);

--
-- Table: date_dim, Attribute: d_holiday
--
-- DELETE FROM pg_statistic WHERE starelid='cte_bug.date_dim'::regclass AND staattnum=17;
INSERT INTO pg_statistic VALUES (
	'cte_bug.date_dim'::regclass,
	17::smallint,
	False::boolean,
	0.0::real,
	2::integer,
	2.0::real,
	1::smallint,
	3::smallint,
	0::smallint,
	0::smallint,
	0::smallint,
	1054::oid,
	1058::oid,
	0::oid,
	0::oid,
	0::oid,
	100::oid,
	100::oid,
	0::oid,
	0::oid,
	0::oid,
	'{0.9912, 0.0088}'::real[],
	'{0.9816509}'::real[],
	NULL::real[],
	NULL::real[],
	NULL::real[],
	E'{"N","Y"}'::bpchar[],
	NULL::bpchar[],
	NULL::bpchar[],
	NULL::bpchar[],
	NULL::bpchar[]);

--
-- Table: date_dim, Attribute: d_weekend
--
-- DELETE FROM pg_statistic WHERE starelid='cte_bug.date_dim'::regclass AND staattnum=18;
INSERT INTO pg_statistic VALUES (
	'cte_bug.date_dim'::regclass,
	18::smallint,
	False::boolean,
	0.0::real,
	2::integer,
	2.0::real,
	1::smallint,
	3::smallint,
	0::smallint,
	0::smallint,
	0::smallint,
	1054::oid,
	1058::oid,
	0::oid,
	0::oid,
	0::oid,
	100::oid,
	100::oid,
	0::oid,
	0::oid,
	0::oid,
	'{0.71786666, 0.28213334}'::real[],
	'{0.59107685}'::real[],
	NULL::real[],
	NULL::real[],
	NULL::real[],
	E'{"N","Y"}'::bpchar[],
	NULL::bpchar[],
	NULL::bpchar[],
	NULL::bpchar[],
	NULL::bpchar[]);

--
-- Table: date_dim, Attribute: d_following_holiday
--
-- DELETE FROM pg_statistic WHERE starelid='cte_bug.date_dim'::regclass AND staattnum=19;
INSERT INTO pg_statistic VALUES (
	'cte_bug.date_dim'::regclass,
	19::smallint,
	False::boolean,
	0.0::real,
	2::integer,
	2.0::real,
	1::smallint,
	3::smallint,
	0::smallint,
	0::smallint,
	0::smallint,
	1054::oid,
	1058::oid,
	0::oid,
	0::oid,
	0::oid,
	100::oid,
	100::oid,
	0::oid,
	0::oid,
	0::oid,
	'{0.9918333, 0.008166667}'::real[],
	'{0.98435473}'::real[],
	NULL::real[],
	NULL::real[],
	NULL::real[],
	E'{"N","Y"}'::bpchar[],
	NULL::bpchar[],
	NULL::bpchar[],
	NULL::bpchar[],
	NULL::bpchar[]);

--
-- Table: date_dim, Attribute: d_first_dom
--
-- DELETE FROM pg_statistic WHERE starelid='cte_bug.date_dim'::regclass AND staattnum=20;
INSERT INTO pg_statistic VALUES (
	'cte_bug.date_dim'::regclass,
	20::smallint,
	False::boolean,
	0.0::real,
	4::integer,
	2400.0::real,
	1::smallint,
	2::smallint,
	3::smallint,
	0::smallint,
	0::smallint,
	96::oid,
	97::oid,
	97::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	'{0.00073333335, 0.0007, 0.00066666666, 0.00066666666, 0.00066666666, 0.00066666666, 0.00066666666, 0.00066666666, 0.00066666666, 0.00066666666, 0.00066666666, 0.00066666666}'::real[],
	NULL::real[],
	'{1.0}'::real[],
	NULL::real[],
	NULL::real[],
	E'{"2444818","2468620","2421046","2423786","2425856","2428566","2431822","2433099","2437878","2443661","2450144","2453097"}'::int4[],
	E'{"2415021","2415751","2416481","2417181","2417881","2418581","2419311","2420012","2420833","2421626","2422294","2423087","2423817","2424576","2425338","2426068","2426768","2427470","2428169","2428959","2429630","2430330","2431091","2431791","2432583","2433314","2434044","2434775","2435565","2436174","2436904","2437635","2438396","2439066","2439796","2440527","2441226","2441957","2442748","2443449","2444209","2445002","2445732","2446491","2447253","2447983","2448683","2449413","2450084","2450815","2451576","2452335","2453127","2453888","2454619","2455379","2456141","2456810","2457510","2458271","2458971","2459671","2460402","2461132","2461803","2462503","2463264","2463995","2464725","2465576","2466276","2466976","2467707","2468437","2469229","2469898","2470629","2471359","2472059","2472850","2473551","2474311","2475073","2475864","2476626","2477356","2478025","2478755","2479456","2480186","2480978","2481677","2482378","2483138","2483839","2484508","2485208","2485910","2486669","2487340","2488039"}'::int4[],
	NULL::int4[],
	NULL::int4[],
	NULL::int4[]);

--
-- Table: date_dim, Attribute: d_last_dom
--
-- DELETE FROM pg_statistic WHERE starelid='cte_bug.date_dim'::regclass AND staattnum=21;
INSERT INTO pg_statistic VALUES (
	'cte_bug.date_dim'::regclass,
	21::smallint,
	False::boolean,
	0.0::real,
	4::integer,
	2400.0::real,
	1::smallint,
	2::smallint,
	3::smallint,
	0::smallint,
	0::smallint,
	96::oid,
	97::oid,
	97::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	'{0.00073333335, 0.0007, 0.00066666666, 0.00066666666, 0.00066666666, 0.00066666666, 0.00066666666, 0.00066666666, 0.00066666666, 0.00066666666, 0.00066666666, 0.00066666666}'::real[],
	NULL::real[],
	'{0.99998605}'::real[],
	NULL::real[],
	NULL::real[],
	E'{"2445029","2468892","2421227","2423785","2426098","2428596","2431821","2433279","2438089","2443811","2450203","2453187"}'::int4[],
	E'{"2415020","2415930","2416661","2417332","2418061","2418734","2419522","2420195","2420984","2421834","2422505","2423236","2424027","2424756","2425489","2426279","2426889","2427619","2428350","2429139","2429810","2430481","2431271","2432002","2432732","2433463","2434194","2434983","2435715","2436385","2437055","2437786","2438576","2439244","2439977","2440705","2441437","2442166","2442899","2443627","2444360","2445151","2445881","2446671","2447404","2448132","2448864","2449534","2450265","2450995","2451786","2452518","2453308","2454039","2454770","2455559","2456293","2456961","2457691","2458422","2459152","2459882","2460553","2461284","2461953","2462683","2463414","2464203","2464935","2465727","2466457","2467128","2467858","2468648","2469380","2470050","2470780","2471511","2472240","2473031","2473702","2474492","2475224","2476016","2476807","2477477","2478206","2478935","2479607","2480337","2481130","2481857","2482529","2483318","2483990","2484660","2485389","2486059","2486790","2487519","2488372"}'::int4[],
	NULL::int4[],
	NULL::int4[],
	NULL::int4[]);

--
-- Table: date_dim, Attribute: d_same_day_ly
--
-- DELETE FROM pg_statistic WHERE starelid='cte_bug.date_dim'::regclass AND staattnum=22;
INSERT INTO pg_statistic VALUES (
	'cte_bug.date_dim'::regclass,
	22::smallint,
	False::boolean,
	0.0::real,
	4::integer,
	-0.9988364::real,
	2::smallint,
	3::smallint,
	0::smallint,
	0::smallint,
	0::smallint,
	97::oid,
	97::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	NULL::real[],
	'{1.0}'::real[],
	NULL::real[],
	NULL::real[],
	NULL::real[],
	E'{"2414662","2415406","2416155","2416846","2417538","2418265","2418990","2419723","2420543","2421270","2421972","2422760","2423452","2424211","2424965","2425669","2426369","2427091","2427796","2428521","2429221","2429916","2430665","2431382","2432122","2432823","2433554","2434295","2435051","2435731","2436451","2437186","2437889","2438567","2439312","2440049","2440756","2441519","2442278","2442986","2443701","2444456","2445155","2445940","2446724","2447426","2448183","2448865","2449565","2450287","2451043","2451810","2452563","2453298","2454047","2454787","2455577","2456285","2456984","2457722","2458470","2459166","2459890","2460630","2461312","2462016","2462761","2463515","2464248","2465067","2465781","2466519","2467240","2467988","2468743","2469399","2470144","2470877","2471595","2472367","2473100","2473855","2474598","2475417","2476187","2476889","2477598","2478301","2479040","2479767","2480562","2481265","2482001","2482736","2483451","2484148","2484826","2485540","2486290","2486986","2487701"}'::int4[],
	NULL::int4[],
	NULL::int4[],
	NULL::int4[],
	NULL::int4[]);

--
-- Table: date_dim, Attribute: d_same_day_lq
--
-- DELETE FROM pg_statistic WHERE starelid='cte_bug.date_dim'::regclass AND staattnum=23;
INSERT INTO pg_statistic VALUES (
	'cte_bug.date_dim'::regclass,
	23::smallint,
	False::boolean,
	0.0::real,
	4::integer,
	-0.99268985::real,
	2::smallint,
	3::smallint,
	0::smallint,
	0::smallint,
	0::smallint,
	97::oid,
	97::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	NULL::real[],
	'{1.0}'::real[],
	NULL::real[],
	NULL::real[],
	NULL::real[],
	E'{"2414935","2415679","2416428","2417119","2417811","2418538","2419263","2419996","2420816","2421543","2422245","2423033","2423725","2424484","2425239","2425942","2426642","2427364","2428069","2428794","2429494","2430189","2430938","2431655","2432395","2433097","2433828","2434568","2435324","2436005","2436725","2437460","2438163","2438842","2439586","2440323","2441031","2441793","2442552","2443260","2443975","2444730","2445429","2446214","2446997","2447700","2448456","2449139","2449839","2450561","2451317","2452084","2452836","2453571","2454320","2455060","2455850","2456558","2457257","2457995","2458743","2459439","2460163","2460903","2461586","2462290","2463035","2463788","2464521","2465340","2466054","2466792","2467513","2468261","2469016","2469672","2470417","2471150","2471868","2472640","2473373","2474128","2474871","2475692","2476462","2477164","2477873","2478576","2479315","2480042","2480837","2481540","2482276","2483011","2483726","2484423","2485099","2485813","2486563","2487259","2487974"}'::int4[],
	NULL::int4[],
	NULL::int4[],
	NULL::int4[],
	NULL::int4[]);

--
-- Table: date_dim, Attribute: d_current_day
--
-- DELETE FROM pg_statistic WHERE starelid='cte_bug.date_dim'::regclass AND staattnum=24;
INSERT INTO pg_statistic VALUES (
	'cte_bug.date_dim'::regclass,
	24::smallint,
	False::boolean,
	0.0::real,
	2::integer,
	1.0::real,
	1::smallint,
	3::smallint,
	0::smallint,
	0::smallint,
	0::smallint,
	1054::oid,
	1058::oid,
	0::oid,
	0::oid,
	0::oid,
	100::oid,
	100::oid,
	0::oid,
	0::oid,
	0::oid,
	'{1.0}'::real[],
	'{1.0}'::real[],
	NULL::real[],
	NULL::real[],
	NULL::real[],
	E'{"N"}'::bpchar[],
	NULL::bpchar[],
	NULL::bpchar[],
	NULL::bpchar[],
	NULL::bpchar[]);

--
-- Table: date_dim, Attribute: d_current_week
--
-- DELETE FROM pg_statistic WHERE starelid='cte_bug.date_dim'::regclass AND staattnum=25;
INSERT INTO pg_statistic VALUES (
	'cte_bug.date_dim'::regclass,
	25::smallint,
	False::boolean,
	0.0::real,
	2::integer,
	1.0::real,
	1::smallint,
	3::smallint,
	0::smallint,
	0::smallint,
	0::smallint,
	1054::oid,
	1058::oid,
	0::oid,
	0::oid,
	0::oid,
	100::oid,
	100::oid,
	0::oid,
	0::oid,
	0::oid,
	'{1.0}'::real[],
	'{1.0}'::real[],
	NULL::real[],
	NULL::real[],
	NULL::real[],
	E'{"N"}'::bpchar[],
	NULL::bpchar[],
	NULL::bpchar[],
	NULL::bpchar[],
	NULL::bpchar[]);

--
-- Table: date_dim, Attribute: d_current_month
--
-- DELETE FROM pg_statistic WHERE starelid='cte_bug.date_dim'::regclass AND staattnum=26;
INSERT INTO pg_statistic VALUES (
	'cte_bug.date_dim'::regclass,
	26::smallint,
	False::boolean,
	0.0::real,
	2::integer,
	2.0::real,
	1::smallint,
	3::smallint,
	0::smallint,
	0::smallint,
	0::smallint,
	1054::oid,
	1058::oid,
	0::oid,
	0::oid,
	0::oid,
	100::oid,
	100::oid,
	0::oid,
	0::oid,
	0::oid,
	'{0.9996, 0.0004}'::real[],
	'{0.9994385}'::real[],
	NULL::real[],
	NULL::real[],
	NULL::real[],
	E'{"N","Y"}'::bpchar[],
	NULL::bpchar[],
	NULL::bpchar[],
	NULL::bpchar[],
	NULL::bpchar[]);

--
-- Table: date_dim, Attribute: d_current_quarter
--
-- DELETE FROM pg_statistic WHERE starelid='cte_bug.date_dim'::regclass AND staattnum=27;
INSERT INTO pg_statistic VALUES (
	'cte_bug.date_dim'::regclass,
	27::smallint,
	False::boolean,
	0.0::real,
	2::integer,
	2.0::real,
	1::smallint,
	3::smallint,
	0::smallint,
	0::smallint,
	0::smallint,
	1054::oid,
	1058::oid,
	0::oid,
	0::oid,
	0::oid,
	100::oid,
	100::oid,
	0::oid,
	0::oid,
	0::oid,
	'{0.99873334, 0.0012666667}'::real[],
	'{0.9982279}'::real[],
	NULL::real[],
	NULL::real[],
	NULL::real[],
	E'{"N","Y"}'::bpchar[],
	NULL::bpchar[],
	NULL::bpchar[],
	NULL::bpchar[],
	NULL::bpchar[]);

--
-- Table: date_dim, Attribute: d_current_year
--
-- DELETE FROM pg_statistic WHERE starelid='cte_bug.date_dim'::regclass AND staattnum=28;
INSERT INTO pg_statistic VALUES (
	'cte_bug.date_dim'::regclass,
	28::smallint,
	False::boolean,
	0.0::real,
	2::integer,
	2.0::real,
	1::smallint,
	3::smallint,
	0::smallint,
	0::smallint,
	0::smallint,
	1054::oid,
	1058::oid,
	0::oid,
	0::oid,
	0::oid,
	100::oid,
	100::oid,
	0::oid,
	0::oid,
	0::oid,
	'{0.99543333, 0.0045666667}'::real[],
	'{0.99365747}'::real[],
	NULL::real[],
	NULL::real[],
	NULL::real[],
	E'{"N","Y"}'::bpchar[],
	NULL::bpchar[],
	NULL::bpchar[],
	NULL::bpchar[],
	NULL::bpchar[]);

--
-- Table: store_sales, Attribute: ss_sold_date_sk
--
-- DELETE FROM pg_statistic WHERE starelid='cte_bug.store_sales'::regclass AND staattnum=1;
INSERT INTO pg_statistic VALUES (
	'cte_bug.store_sales'::regclass,
	1::smallint,
	False::boolean,
	0.044::real,
	4::integer,
	1823.0::real,
	1::smallint,
	2::smallint,
	3::smallint,
	0::smallint,
	0::smallint,
	96::oid,
	97::oid,
	97::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	'{0.0015333333, 0.0015, 0.0014666667, 0.0014666667, 0.0014, 0.0014, 0.0014, 0.0014, 0.0014, 0.0013666666, 0.0013666666, 0.0013666666, 0.0013333333, 0.0013333333, 0.0013333333, 0.0013333333, 0.0013333333, 0.0013333333, 0.0013, 0.0013, 0.0013, 0.0013, 0.0013, 0.0013, 0.0013, 0.0013, 0.0012666667, 0.0012666667, 0.0012666667, 0.0012666667, 0.0012666667, 0.0012666667, 0.0012666667, 0.0012666667, 0.0012666667, 0.0012666667, 0.0012666667, 0.0012333334, 0.0012333334, 0.0012333334, 0.0012333334, 0.0012333334, 0.0012333334, 0.0012333334, 0.0012333334, 0.0012, 0.0012, 0.0012, 0.0012, 0.0012, 0.0012, 0.0012, 0.0012, 0.0012, 0.0012, 0.0012, 0.0012, 0.0012, 0.0012, 0.0012, 0.0011666666, 0.0011666666, 0.0011666666, 0.0011666666, 0.0011666666, 0.0011666666, 0.0011666666, 0.0011666666, 0.0011666666, 0.0011666666, 0.0011666666, 0.0011333333, 0.0011333333, 0.0011333333, 0.0011333333, 0.0011333333, 0.0011333333, 0.0011333333, 0.0011333333, 0.0011333333, 0.0011333333, 0.0011333333, 0.0011333333, 0.0011333333, 0.0011333333, 0.0011333333, 0.0011333333, 0.0011333333, 0.0011333333, 0.0011333333, 0.0011333333, 0.0011, 0.0011, 0.0011, 0.0011, 0.0011, 0.0011, 0.0011, 0.0011, 0.0011}'::real[],
	NULL::real[],
	'{-0.0022345192}'::real[],
	NULL::real[],
	NULL::real[],
	E'{"2451534","2452228","2451129","2452240","2451139","2451161","2451546","2452258","2452621","2452589","2452600","2452625","2451137","2451148","2451181","2451896","2452251","2452260","2451178","2451528","2451904","2451909","2452242","2452250","2452628","2452642","2451121","2451134","2451174","2451496","2451864","2451872","2451879","2451910","2452257","2452271","2452592","2451112","2451149","2451525","2451865","2452232","2452277","2452598","2452640","2451127","2451136","2451153","2451177","2451505","2451541","2451851","2451862","2452230","2452238","2452266","2452607","2452609","2452627","2452641","2451142","2451158","2451160","2451491","2451868","2451885","2451887","2452248","2452601","2452606","2452634","2451123","2451125","2451133","2451138","2451144","2451506","2451509","2451529","2451884","2451898","2452219","2452221","2452225","2452246","2452256","2452274","2452597","2452602","2452614","2452623","2451126","2451141","2451154","2451493","2451494","2451497","2451513","2451516","2451526"}'::int4[],
	E'{"2450816","2450843","2450874","2450900","2450926","2450953","2450982","2451010","2451033","2451045","2451057","2451070","2451082","2451095","2451107","2451119","2451143","2451157","2451169","2451184","2451213","2451241","2451267","2451294","2451322","2451349","2451372","2451398","2451410","2451423","2451435","2451448","2451460","2451472","2451485","2451499","2451511","2451522","2451536","2451545","2451572","2451600","2451626","2451650","2451679","2451703","2451734","2451760","2451772","2451783","2451795","2451806","2451818","2451830","2451843","2451855","2451867","2451877","2451890","2451901","2451916","2451945","2451974","2451999","2452025","2452053","2452081","2452109","2452129","2452140","2452151","2452163","2452173","2452185","2452195","2452207","2452218","2452233","2452245","2452262","2452275","2452301","2452329","2452358","2452385","2452413","2452437","2452464","2452492","2452504","2452516","2452528","2452540","2452553","2452566","2452578","2452587","2452604","2452617","2452630","2452639"}'::int4[],
	NULL::int4[],
	NULL::int4[],
	NULL::int4[]);

--
-- Table: store_sales, Attribute: ss_sold_time_sk
--
-- DELETE FROM pg_statistic WHERE starelid='cte_bug.store_sales'::regclass AND staattnum=2;
INSERT INTO pg_statistic VALUES (
	'cte_bug.store_sales'::regclass,
	2::smallint,
	False::boolean,
	0.04476667::real,
	4::integer,
	42441.0::real,
	1::smallint,
	2::smallint,
	3::smallint,
	0::smallint,
	0::smallint,
	96::oid,
	97::oid,
	97::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	'{0.00023333334}'::real[],
	NULL::real[],
	'{-0.0029273503}'::real[],
	NULL::real[],
	NULL::real[],
	E'{"53826"}'::int4[],
	E'{"28802","29544","30512","31386","32305","32816","33250","33722","34146","34599","35052","35511","35964","36268","36566","36863","37172","37485","37828","38121","38401","38684","38983","39275","39560","40075","40557","41068","41530","42041","42524","43017","43474","43957","44423","44910","45389","45848","46343","46755","47169","47614","48001","48403","48805","49181","49610","50080","50477","50759","51068","51365","51655","51931","52218","52524","52799","53101","53418","53732","54105","54884","55780","56713","57597","58420","59372","60285","61221","61506","61795","62076","62374","62681","62967","63254","63580","63859","64159","64441","64733","65144","65590","66065","66523","66923","67434","67869","68271","68722","69211","69656","70119","70604","71096","71498","71944","72815","73678","74590","75597"}'::int4[],
	NULL::int4[],
	NULL::int4[],
	NULL::int4[]);

--
-- Table: store_sales, Attribute: ss_item_sk
--
-- DELETE FROM pg_statistic WHERE starelid='cte_bug.store_sales'::regclass AND staattnum=3;
INSERT INTO pg_statistic VALUES (
	'cte_bug.store_sales'::regclass,
	3::smallint,
	False::boolean,
	0.0::real,
	4::integer,
	25013.0::real,
	1::smallint,
	2::smallint,
	3::smallint,
	0::smallint,
	0::smallint,
	96::oid,
	97::oid,
	97::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	'{0.0003, 0.00026666667, 0.00026666667, 0.00026666667, 0.00026666667, 0.00026666667, 0.00026666667, 0.00026666667, 0.00026666667, 0.00023333334, 0.00023333334, 0.00023333334, 0.00023333334, 0.00023333334, 0.00023333334, 0.00023333334, 0.00023333334, 0.00023333334, 0.00023333334, 0.00023333334, 0.00023333334, 0.00023333334, 0.00023333334, 0.00023333334, 0.00023333334, 0.00023333334, 0.00023333334, 0.00023333334, 0.00023333334, 0.00023333334, 0.00023333334, 0.00023333334, 0.00023333334, 0.00023333334, 0.00023333334, 0.00023333334, 0.00023333334, 0.00023333334, 0.00023333334, 0.00023333334}'::real[],
	NULL::real[],
	'{0.000308136}'::real[],
	NULL::real[],
	NULL::real[],
	E'{"18140","1183","1921","10183","13837","16333","22268","26593","27325","2035","3793","4081","4525","5833","8575","9103","9211","9703","10507","11761","12685","13513","14792","15871","15907","15998","17095","17455","19111","20875","21085","21457","22741","24703","24817","26851","26971","27337","27709","27919"}'::int4[],
	E'{"1","260","575","888","1162","1441","1707","1975","2264","2523","2847","3115","3368","3650","3951","4214","4523","4827","5071","5329","5587","5894","6181","6496","6776","7024","7312","7591","7855","8113","8398","8682","8957","9245","9530","9823","10114","10405","10699","10959","11247","11525","11781","12061","12360","12613","12889","13190","13453","13731","14017","14294","14575","14877","15152","15457","15733","16015","16291","16579","16861","17155","17437","17702","17984","18247","18519","18783","19092","19380","19642","19943","20221","20467","20723","20995","21311","21596","21870","22136","22427","22700","22970","23261","23533","23805","24082","24344","24628","24932","25220","25519","25800","26063","26331","26591","26879","27151","27422","27705","27999"}'::int4[],
	NULL::int4[],
	NULL::int4[],
	NULL::int4[]);

--
-- Table: store_sales, Attribute: ss_customer_sk
--
-- DELETE FROM pg_statistic WHERE starelid='cte_bug.store_sales'::regclass AND staattnum=4;
INSERT INTO pg_statistic VALUES (
	'cte_bug.store_sales'::regclass,
	4::smallint,
	False::boolean,
	0.043866668::real,
	4::integer,
	1787614.0::real,
	2::smallint,
	3::smallint,
	0::smallint,
	0::smallint,
	0::smallint,
	97::oid,
	97::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	NULL::real[],
	'{0.002228295}'::real[],
	NULL::real[],
	NULL::real[],
	NULL::real[],
	E'{"17","21786","42607","63355","81931","104851","124188","143682","164679","186368","206598","227797","248026","266122","287862","306345","328795","349339","369849","390012","413705","437205","457764","479231","501632","523614","545267","564653","584588","603322","628035","648054","668479","689551","709515","730333","751340","771680","793540","814237","835667","857070","877179","899789","920597","942338","962762","984964","1006607","1027347","1048659","1070611","1092247","1111106","1131352","1152543","1173197","1192658","1215327","1237528","1259725","1279865","1300200","1324729","1344982","1367029","1388243","1408781","1430858","1451533","1472783","1491853","1512116","1533678","1554248","1577279","1600025","1621228","1641577","1663202","1683219","1704339","1723327","1742744","1763526","1783848","1805196","1826090","1845634","1865356","1885579","1905861","1925869","1946542","1969323","1991449","2012218","2035047","2058343","2079610","2099931"}'::int4[],
	NULL::int4[],
	NULL::int4[],
	NULL::int4[],
	NULL::int4[]);

--
-- Table: store_sales, Attribute: ss_cdemo_sk
--
-- DELETE FROM pg_statistic WHERE starelid='cte_bug.store_sales'::regclass AND staattnum=5;
INSERT INTO pg_statistic VALUES (
	'cte_bug.store_sales'::regclass,
	5::smallint,
	False::boolean,
	0.044433333::real,
	4::integer,
	1889709.0::real,
	2::smallint,
	3::smallint,
	0::smallint,
	0::smallint,
	0::smallint,
	97::oid,
	97::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	NULL::real[],
	'{-0.00950842}'::real[],
	NULL::real[],
	NULL::real[],
	NULL::real[],
	E'{"30","17849","37904","56865","76186","96917","115542","134186","152099","172680","192783","211817","229921","248171","269276","287913","306855","326795","345207","365388","382970","404387","425118","443738","463882","483064","501977","520761","538035","556646","576673","595274","615470","633068","652437","671351","689763","709263","728426","748174","767474","786428","805372","823760","843936","863306","880648","898664","917543","936304","956226","976161","994176","1014253","1033333","1051646","1070542","1089363","1107670","1128460","1147427","1166875","1187090","1205515","1224744","1244031","1263305","1283771","1301649","1319446","1337051","1356966","1373991","1393391","1412686","1433469","1453024","1470147","1488228","1507316","1525606","1543945","1563428","1582932","1601688","1622584","1644497","1663783","1684160","1704534","1724056","1744635","1764898","1783571","1802036","1823303","1844256","1864851","1883445","1902230","1920718"}'::int4[],
	NULL::int4[],
	NULL::int4[],
	NULL::int4[],
	NULL::int4[]);

--
-- Table: store_sales, Attribute: ss_hdemo_sk
--
-- DELETE FROM pg_statistic WHERE starelid='cte_bug.store_sales'::regclass AND staattnum=6;
INSERT INTO pg_statistic VALUES (
	'cte_bug.store_sales'::regclass,
	6::smallint,
	False::boolean,
	0.0444::real,
	4::integer,
	7203.0::real,
	1::smallint,
	2::smallint,
	3::smallint,
	0::smallint,
	0::smallint,
	96::oid,
	97::oid,
	97::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	'{0.00043333333, 0.0004}'::real[],
	NULL::real[],
	'{0.0009649638}'::real[],
	NULL::real[],
	NULL::real[],
	E'{"3964","3629"}'::int4[],
	E'{"1","77","150","224","302","367","433","498","566","636","710","784","865","941","1013","1088","1170","1244","1323","1388","1461","1536","1606","1672","1735","1809","1876","1952","2024","2098","2172","2244","2307","2379","2456","2534","2602","2673","2742","2818","2892","2971","3037","3111","3186","3257","3327","3396","3472","3543","3613","3687","3768","3845","3910","3981","4050","4116","4188","4251","4325","4393","4463","4541","4613","4685","4755","4830","4903","4980","5052","5126","5195","5262","5335","5402","5469","5534","5604","5673","5747","5823","5891","5960","6032","6112","6185","6259","6335","6407","6479","6552","6629","6696","6762","6836","6906","6982","7054","7128","7200"}'::int4[],
	NULL::int4[],
	NULL::int4[],
	NULL::int4[]);

--
-- Table: store_sales, Attribute: ss_addr_sk
--
-- DELETE FROM pg_statistic WHERE starelid='cte_bug.store_sales'::regclass AND staattnum=7;
INSERT INTO pg_statistic VALUES (
	'cte_bug.store_sales'::regclass,
	7::smallint,
	False::boolean,
	0.0441::real,
	4::integer,
	1054173.0::real,
	2::smallint,
	3::smallint,
	0::smallint,
	0::smallint,
	0::smallint,
	97::oid,
	97::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	NULL::real[],
	'{-0.009495346}'::real[],
	NULL::real[],
	NULL::real[],
	NULL::real[],
	E'{"24","10492","20174","30656","40366","50491","61387","71217","82320","93176","103530","113999","124922","135328","145700","156238","168283","179107","189968","200308","210858","220931","230791","240825","252093","261999","273446","284421","294475","304990","315199","325963","336804","346964","358483","369478","379508","390126","400826","411839","423548","433723","444375","455042","465928","476227","487109","496816","507138","516971","528312","538979","549252","559578","569918","580340","590219","600402","612301","623316","633219","642905","653631","665000","674723","685655","695157","706546","716090","726191","735744","745583","756508","767077","776705","787088","798590","810484","820976","832448","843248","854009","864628","873989","883994","894860","905009","916085","924744","935185","945700","956390","965553","975624","987328","997741","1008137","1018620","1029054","1039248","1049982"}'::int4[],
	NULL::int4[],
	NULL::int4[],
	NULL::int4[],
	NULL::int4[]);

--
-- Table: store_sales, Attribute: ss_store_sk
--
-- DELETE FROM pg_statistic WHERE starelid='cte_bug.store_sales'::regclass AND staattnum=8;
INSERT INTO pg_statistic VALUES (
	'cte_bug.store_sales'::regclass,
	8::smallint,
	False::boolean,
	0.045766667::real,
	4::integer,
	35.0::real,
	1::smallint,
	3::smallint,
	0::smallint,
	0::smallint,
	0::smallint,
	96::oid,
	97::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	'{0.029033333, 0.0287, 0.028266666, 0.0282, 0.028066667, 0.028, 0.027966667, 0.027833333, 0.027766667, 0.027666667, 0.027666667, 0.0276, 0.0276, 0.0275, 0.027466666, 0.027433334, 0.027433334, 0.027366666, 0.027366666, 0.0273, 0.027233334, 0.027133333, 0.027066667, 0.027033333, 0.0269, 0.0269, 0.026833333, 0.026733333, 0.0267, 0.026633333, 0.026333334, 0.026066666, 0.026066666, 0.025666667, 0.0247}'::real[],
	'{0.025343372}'::real[],
	NULL::real[],
	NULL::real[],
	NULL::real[],
	E'{"19","34","2","61","62","7","32","13","52","4","38","55","56","28","49","1","37","16","67","50","31","64","22","68","20","58","26","46","14","10","25","8","43","40","44"}'::int4[],
	NULL::int4[],
	NULL::int4[],
	NULL::int4[],
	NULL::int4[]);

--
-- Table: store_sales, Attribute: ss_promo_sk
--
-- DELETE FROM pg_statistic WHERE starelid='cte_bug.store_sales'::regclass AND staattnum=9;
INSERT INTO pg_statistic VALUES (
	'cte_bug.store_sales'::regclass,
	9::smallint,
	False::boolean,
	0.04413333::real,
	4::integer,
	357.0::real,
	1::smallint,
	2::smallint,
	3::smallint,
	0::smallint,
	0::smallint,
	96::oid,
	97::oid,
	97::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	'{0.0036666666, 0.0036666666, 0.0036333334, 0.0036333334, 0.0035333333, 0.0035, 0.0033666666, 0.0033666666}'::real[],
	NULL::real[],
	'{0.009066247}'::real[],
	NULL::real[],
	NULL::real[],
	E'{"37","139","81","228","350","223","293","304"}'::int4[],
	E'{"1","4","7","11","14","18","21","25","28","32","35","40","43","47","50","54","57","61","64","68","71","75","78","83","86","89","93","96","100","103","107","110","114","118","121","124","128","132","136","140","143","147","150","153","157","160","163","166","170","174","177","180","183","187","190","194","197","201","204","208","211","214","218","221","226","230","234","237","241","245","249","252","256","259","262","266","270","274","277","280","284","288","291","296","300","303","308","311","315","319","322","326","329","332","336","339","343","346","351","354","357"}'::int4[],
	NULL::int4[],
	NULL::int4[],
	NULL::int4[]);

--
-- Table: store_sales, Attribute: ss_ticket_number
--
-- DELETE FROM pg_statistic WHERE starelid='cte_bug.store_sales'::regclass AND staattnum=10;
INSERT INTO pg_statistic VALUES (
	'cte_bug.store_sales'::regclass,
	10::smallint,
	False::boolean,
	0.0::real,
	8::integer,
	135837840.0::real,
	2::smallint,
	3::smallint,
	0::smallint,
	0::smallint,
	0::smallint,
	412::oid,
	412::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	NULL::real[],
	'{0.06078869}'::real[],
	NULL::real[],
	NULL::real[],
	NULL::real[],
	E'{"813","1375913","2727072","3857212","5016369","6156850","7386542","8690070","9789582","11044399","12265523","13510184","14717978","15785339","16975428","18197314","19442297","20772509","21903183","23100195","24331207","25452464","26678265","27922018","29143270","30353305","31671176","32953618","34109365","35253703","36385377","37447554","38717221","39999948","41154706","42417922","43652601","44968753","46342094","47459202","48642561","49683656","50998261","52288399","53487733","54680281","55833024","57017267","58237604","59472084","60696465","61996614","63179509","64401202","65651723","66926244","68011538","69269328","70341852","71528693","72651930","73713093","74849544","75928064","77033905","78162650","79324991","80585670","81761102","82828934","84032244","85334532","86494728","87598137","88860063","89922519","91069374","92276218","93495454","94722691","95959052","97146341","98399034","99566170","100766327","101898400","103223735","104414548","105602699","106825065","108029779","109174799","110420663","111591894","112817588","113975952","115181211","116399324","117672963","118888721","119999864"}'::int8[],
	NULL::int8[],
	NULL::int8[],
	NULL::int8[],
	NULL::int8[]);

--
-- Table: store_sales, Attribute: ss_quantity
--
-- DELETE FROM pg_statistic WHERE starelid='cte_bug.store_sales'::regclass AND staattnum=11;
INSERT INTO pg_statistic VALUES (
	'cte_bug.store_sales'::regclass,
	11::smallint,
	False::boolean,
	0.045466665::real,
	4::integer,
	100.0::real,
	1::smallint,
	3::smallint,
	0::smallint,
	0::smallint,
	0::smallint,
	96::oid,
	97::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	'{0.010833333, 0.010766666, 0.010766666, 0.010766666, 0.010633334, 0.0106, 0.010566667, 0.0104, 0.0104, 0.0104, 0.0104, 0.010366667, 0.010366667, 0.010366667, 0.0103, 0.010266666, 0.0101666665, 0.0101333335, 0.0101333335, 0.0101, 0.010066667, 0.010066667, 0.010066667, 0.010066667, 0.01, 0.01, 0.009966667, 0.009966667, 0.009933333, 0.0099, 0.009866667, 0.009833333, 0.0098, 0.009766666, 0.009766666, 0.009766666, 0.009766666, 0.009733333, 0.009733333, 0.009733333, 0.009733333, 0.0097, 0.0097, 0.009666666, 0.009666666, 0.009666666, 0.009633333, 0.0096, 0.0096, 0.0096, 0.0096, 0.009566667, 0.009533334, 0.009533334, 0.0095, 0.0095, 0.009466667, 0.009433334, 0.0094, 0.009366667, 0.009366667, 0.009366667, 0.009366667, 0.009366667, 0.009366667, 0.009366667, 0.009333333, 0.009266667, 0.0092, 0.0092, 0.0092, 0.009133333, 0.009133333, 0.009133333, 0.009133333, 0.0091, 0.0091, 0.0091, 0.0091, 0.0090666665, 0.0090666665, 0.0090333335, 0.009, 0.008966667, 0.008933334, 0.0089, 0.0089, 0.008833333, 0.008666666, 0.008633333, 0.0086, 0.008566666, 0.0084, 0.0084, 0.008366667, 0.008366667, 0.008233333, 0.008233333, 0.0081, 0.0079333335}'::real[],
	'{0.0045824465}'::real[],
	NULL::real[],
	NULL::real[],
	NULL::real[],
	E'{"29","7","10","21","27","92","86","5","55","71","81","33","79","93","6","99","40","39","57","32","53","73","84","97","85","90","38","91","12","69","76","41","23","22","37","45","63","44","61","88","100","20","98","26","49","67","14","18","78","87","95","3","31","46","15","36","58","42","64","11","13","50","59","60","68","70","19","54","25","35","66","24","52","56","83","4","16","47","74","80","96","34","51","75","82","30","77","65","28","1","48","9","2","72","43","94","17","89","62","8"}'::int4[],
	NULL::int4[],
	NULL::int4[],
	NULL::int4[],
	NULL::int4[]);

--
-- Table: store_sales, Attribute: ss_wholesale_cost
--
-- DELETE FROM pg_statistic WHERE starelid='cte_bug.store_sales'::regclass AND staattnum=12;
INSERT INTO pg_statistic VALUES (
	'cte_bug.store_sales'::regclass,
	12::smallint,
	False::boolean,
	0.0435::real,
	6::integer,
	9909.0::real,
	1::smallint,
	2::smallint,
	3::smallint,
	0::smallint,
	0::smallint,
	1752::oid,
	1754::oid,
	1754::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	'{0.00033333333, 0.00033333333, 0.00033333333, 0.00033333333, 0.00033333333, 0.00033333333}'::real[],
	NULL::real[],
	'{-0.004772207}'::real[],
	NULL::real[],
	NULL::real[],
	E'{"32.94","43.22","71.68","76.26","91.35","96.72"}'::numeric[],
	E'{"1.00","1.97","3.01","4.10","5.06","6.04","7.00","8.01","9.01","9.96","10.88","11.91","12.82","13.84","14.95","15.91","16.85","17.91","18.92","19.94","20.89","22.01","23.08","24.06","25.02","26.03","26.96","27.89","28.91","29.93","30.90","31.93","32.96","33.91","34.98","35.96","36.94","37.90","38.81","39.70","40.61","41.67","42.74","43.73","44.66","45.63","46.59","47.53","48.55","49.54","50.48","51.56","52.62","53.42","54.39","55.28","56.33","57.36","58.37","59.23","60.15","61.15","62.19","63.21","64.16","65.22","66.31","67.27","68.29","69.31","70.23","71.24","72.24","73.25","74.22","75.12","76.09","77.07","78.06","79.07","80.05","81.10","82.08","83.09","84.06","85.12","86.17","87.18","88.23","89.18","90.13","91.20","92.19","93.19","94.14","95.07","95.98","96.91","97.92","98.92","100.00"}'::numeric[],
	NULL::numeric[],
	NULL::numeric[],
	NULL::numeric[]);

--
-- Table: store_sales, Attribute: ss_list_price
--
-- DELETE FROM pg_statistic WHERE starelid='cte_bug.store_sales'::regclass AND staattnum=13;
INSERT INTO pg_statistic VALUES (
	'cte_bug.store_sales'::regclass,
	13::smallint,
	False::boolean,
	0.0447::real,
	6::integer,
	16808.0::real,
	1::smallint,
	2::smallint,
	3::smallint,
	0::smallint,
	0::smallint,
	1752::oid,
	1754::oid,
	1754::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	'{0.00033333333, 0.0003, 0.0003, 0.0003, 0.00026666667, 0.00026666667, 0.00026666667, 0.00026666667, 0.00026666667, 0.00026666667, 0.00026666667, 0.00026666667, 0.00026666667, 0.00026666667}'::real[],
	NULL::real[],
	'{-0.00067186705}'::real[],
	NULL::real[],
	NULL::real[],
	E'{"27.24","7.02","96.36","105.63","17.20","33.52","39.04","40.68","48.96","59.81","68.69","76.24","87.92","99.16"}'::numeric[],
	E'{"1.00","2.89","4.28","5.87","7.42","8.80","10.10","11.63","12.99","14.42","15.98","17.40","18.87","20.21","21.63","23.04","24.51","25.90","27.36","28.81","30.36","31.81","33.34","34.72","36.09","37.52","38.95","40.43","41.88","43.30","44.81","46.40","47.73","49.18","50.55","52.07","53.40","54.73","56.17","57.55","58.87","60.19","61.77","63.07","64.57","66.02","67.39","68.92","70.40","72.03","73.45","74.87","76.26","77.73","79.03","80.35","81.92","83.32","84.72","86.21","87.59","88.96","90.45","91.78","93.16","94.66","96.19","97.64","98.92","100.52","101.88","103.28","104.72","106.11","107.69","109.21","110.85","112.64","114.53","116.30","118.00","119.87","121.88","123.90","126.04","128.34","130.66","133.07","135.58","137.97","140.53","143.39","146.70","149.95","153.65","157.50","161.61","166.70","172.67","180.23","199.18"}'::numeric[],
	NULL::numeric[],
	NULL::numeric[],
	NULL::numeric[]);

--
-- Table: store_sales, Attribute: ss_sales_price
--
-- DELETE FROM pg_statistic WHERE starelid='cte_bug.store_sales'::regclass AND staattnum=14;
INSERT INTO pg_statistic VALUES (
	'cte_bug.store_sales'::regclass,
	14::smallint,
	False::boolean,
	0.043866668::real,
	6::integer,
	11406.0::real,
	1::smallint,
	2::smallint,
	3::smallint,
	0::smallint,
	0::smallint,
	1752::oid,
	1754::oid,
	1754::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	'{0.008633333, 0.00063333334, 0.0006, 0.00056666665, 0.00056666665, 0.00056666665, 0.00053333334, 0.00053333334, 0.0005, 0.0005, 0.0005, 0.00046666668, 0.00046666668, 0.00046666668, 0.00046666668, 0.00046666668, 0.00046666668, 0.00046666668, 0.00046666668, 0.00046666668, 0.00046666668, 0.00046666668, 0.00043333333, 0.00043333333, 0.00043333333, 0.00043333333, 0.00043333333, 0.00043333333, 0.00043333333, 0.00043333333, 0.00043333333, 0.00043333333, 0.00043333333, 0.00043333333, 0.00043333333, 0.00043333333, 0.0004, 0.0004, 0.0004, 0.0004, 0.0004, 0.0004, 0.0004, 0.0004, 0.0004, 0.0004, 0.0004, 0.0004, 0.0004, 0.0004, 0.0004, 0.0004, 0.0004, 0.0004, 0.0004, 0.0004, 0.0004, 0.0004, 0.0004, 0.0004, 0.0004, 0.0004, 0.0004, 0.0004, 0.0004, 0.0004, 0.0004, 0.0004, 0.0004, 0.0004, 0.0004, 0.0004, 0.0004, 0.0004, 0.0004, 0.00036666667, 0.00036666667, 0.00036666667, 0.00036666667, 0.00036666667, 0.00036666667, 0.00036666667, 0.00036666667, 0.00036666667, 0.00036666667, 0.00036666667, 0.00036666667, 0.00036666667, 0.00036666667, 0.00036666667, 0.00036666667, 0.00036666667, 0.00036666667, 0.00036666667, 0.00036666667, 0.00036666667, 0.00036666667, 0.00036666667, 0.00036666667, 0.00036666667}'::real[],
	NULL::real[],
	'{0.00037045707}'::real[],
	NULL::real[],
	NULL::real[],
	E'{"0.00","5.56","1.37","1.33","1.74","3.53","1.67","3.48","0.51","5.54","6.41","0.82","1.50","1.72","1.83","1.99","2.02","2.25","3.32","3.84","5.66","12.86","0.97","0.98","1.28","1.62","1.65","1.90","2.00","3.08","4.40","5.92","8.40","8.74","11.16","15.51","0.44","0.57","0.65","0.68","0.75","0.94","1.17","1.20","1.22","1.29","1.32","1.35","1.36","2.08","2.14","2.50","2.78","2.82","2.83","3.09","3.64","3.73","3.82","4.51","4.55","5.22","5.43","6.63","6.69","6.90","7.61","7.63","8.09","9.03","9.09","9.44","9.63","11.42","15.16","0.11","0.21","0.60","0.63","0.66","0.93","1.06","1.10","1.16","1.18","1.25","1.42","1.47","1.76","2.09","2.16","2.18","2.60","2.68","2.74","3.11","3.18","3.26","3.27","3.31"}'::numeric[],
	E'{"0.01","0.50","1.00","1.49","2.01","2.44","2.89","3.38","3.80","4.27","4.73","5.13","5.65","6.12","6.56","7.05","7.48","8.02","8.51","9.01","9.57","10.13","10.69","11.20","11.77","12.34","12.88","13.44","14.06","14.61","15.26","15.88","16.54","17.11","17.71","18.35","19.04","19.73","20.46","21.14","21.86","22.67","23.36","24.08","24.87","25.77","26.56","27.30","28.12","28.99","29.85","30.74","31.71","32.63","33.58","34.48","35.42","36.45","37.56","38.53","39.65","40.76","41.86","43.05","44.18","45.37","46.57","47.77","49.08","50.38","51.70","53.17","54.47","55.96","57.34","58.84","60.52","62.13","63.82","65.63","67.20","69.11","71.20","73.12","75.38","77.56","79.86","82.27","85.02","87.88","90.64","93.54","97.22","101.21","105.84","110.61","116.50","123.46","132.30","145.64","193.12"}'::numeric[],
	NULL::numeric[],
	NULL::numeric[],
	NULL::numeric[]);

--
-- Table: store_sales, Attribute: ss_ext_discount_amt
--
-- DELETE FROM pg_statistic WHERE starelid='cte_bug.store_sales'::regclass AND staattnum=15;
INSERT INTO pg_statistic VALUES (
	'cte_bug.store_sales'::regclass,
	15::smallint,
	False::boolean,
	0.045333333::real,
	3::integer,
	6760.0::real,
	1::smallint,
	2::smallint,
	3::smallint,
	0::smallint,
	0::smallint,
	1752::oid,
	1754::oid,
	1754::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	'{0.7661}'::real[],
	NULL::real[],
	'{0.6438912}'::real[],
	NULL::real[],
	NULL::real[],
	E'{"0.00"}'::numeric[],
	E'{"0.04","1.34","3.72","6.09","8.37","10.99","14.04","16.95","19.44","22.34","24.81","28.43","32.34","35.35","39.39","44.25","49.00","54.54","59.98","65.63","71.63","78.32","84.28","90.37","96.62","104.19","111.32","118.10","126.42","136.18","143.96","153.17","162.16","171.42","182.95","192.33","201.85","211.36","222.74","236.09","252.09","266.81","278.33","292.76","310.46","324.68","340.74","359.05","380.26","398.53","416.30","434.21","455.13","474.14","491.53","516.67","538.59","556.85","586.81","610.95","636.53","667.62","692.38","723.55","756.71","796.95","834.72","868.00","906.95","948.96","993.70","1033.87","1089.17","1132.81","1184.95","1228.29","1281.16","1347.98","1408.84","1480.92","1572.04","1643.21","1731.83","1830.90","1925.89","2022.22","2130.03","2281.99","2428.33","2573.26","2717.71","2913.34","3146.60","3438.96","3740.97","4126.94","4666.03","5274.03","6096.32","7082.14","12922.81"}'::numeric[],
	NULL::numeric[],
	NULL::numeric[],
	NULL::numeric[]);

--
-- Table: store_sales, Attribute: ss_ext_sales_price
--
-- DELETE FROM pg_statistic WHERE starelid='cte_bug.store_sales'::regclass AND staattnum=16;
INSERT INTO pg_statistic VALUES (
	'cte_bug.store_sales'::regclass,
	16::smallint,
	False::boolean,
	0.045333333::real,
	6::integer,
	119811.0::real,
	1::smallint,
	2::smallint,
	3::smallint,
	0::smallint,
	0::smallint,
	1752::oid,
	1754::oid,
	1754::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	'{0.0087, 0.00026666667, 0.00023333334, 0.0002, 0.0002}'::real[],
	NULL::real[],
	'{-0.00332316}'::real[],
	NULL::real[],
	NULL::real[],
	E'{"0.00","25.20","10.92","36.96","469.20"}'::numeric[],
	E'{"0.27","8.61","16.80","24.36","33.50","42.56","50.49","61.05","71.40","82.38","93.84","106.14","118.17","131.32","144.00","157.78","172.64","188.16","203.84","220.80","236.84","255.08","271.45","288.48","308.34","327.28","345.73","367.84","387.02","408.00","432.63","453.75","480.22","504.24","530.66","558.80","588.24","616.30","644.76","674.50","704.16","734.58","766.92","802.75","838.24","871.04","910.52","945.76","983.29","1024.86","1063.30","1102.08","1147.38","1190.70","1237.47","1284.12","1331.00","1387.50","1440.24","1503.28","1554.80","1610.95","1676.49","1742.20","1808.00","1867.45","1933.08","2001.48","2087.67","2170.21","2251.53","2342.40","2428.20","2526.36","2617.12","2724.48","2827.24","2944.32","3046.35","3173.12","3294.25","3442.56","3588.09","3734.64","3902.60","4072.80","4240.57","4428.00","4642.32","4906.57","5147.50","5431.50","5743.00","6100.32","6469.35","6901.32","7510.60","8168.50","9050.41","10628.04","17738.88"}'::numeric[],
	NULL::numeric[],
	NULL::numeric[],
	NULL::numeric[]);

--
-- Table: store_sales, Attribute: ss_ext_wholesale_cost
--
-- DELETE FROM pg_statistic WHERE starelid='cte_bug.store_sales'::regclass AND staattnum=17;
INSERT INTO pg_statistic VALUES (
	'cte_bug.store_sales'::regclass,
	17::smallint,
	False::boolean,
	0.044966668::real,
	6::integer,
	207111.0::real,
	2::smallint,
	3::smallint,
	0::smallint,
	0::smallint,
	0::smallint,
	1754::oid,
	1754::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	NULL::real[],
	'{-0.0051949597}'::real[],
	NULL::real[],
	NULL::real[],
	NULL::real[],
	E'{"1.08","33.20","56.42","79.36","97.98","121.59","144.80","167.70","191.00","218.46","244.03","270.55","299.76","327.80","355.32","384.30","414.52","442.20","475.83","508.95","544.25","578.25","612.36","650.43","688.35","727.92","765.38","803.60","837.20","874.56","916.09","960.94","1003.45","1044.66","1092.25","1140.48","1185.60","1238.61","1289.29","1337.70","1390.55","1444.40","1496.49","1548.16","1605.87","1662.12","1713.60","1764.63","1817.76","1875.30","1937.05","1999.50","2060.11","2128.60","2196.21","2259.76","2326.32","2396.25","2465.40","2539.24","2614.56","2685.00","2761.38","2840.04","2925.35","3015.87","3097.05","3179.52","3263.77","3343.96","3431.34","3528.32","3624.37","3716.85","3814.10","3920.27","4022.88","4127.23","4243.63","4356.48","4476.15","4597.05","4738.23","4865.40","5000.80","5143.91","5291.12","5444.00","5603.10","5773.32","5947.83","6145.55","6364.80","6594.77","6830.75","7087.20","7404.30","7745.95","8124.04","8664.15","9992.00"}'::numeric[],
	NULL::numeric[],
	NULL::numeric[],
	NULL::numeric[],
	NULL::numeric[]);

--
-- Table: store_sales, Attribute: ss_ext_list_price
--
-- DELETE FROM pg_statistic WHERE starelid='cte_bug.store_sales'::regclass AND staattnum=18;
INSERT INTO pg_statistic VALUES (
	'cte_bug.store_sales'::regclass,
	18::smallint,
	False::boolean,
	0.044166666::real,
	7::integer,
	281020.0::real,
	2::smallint,
	3::smallint,
	0::smallint,
	0::smallint,
	0::smallint,
	1754::oid,
	1754::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	NULL::real[],
	'{-0.005409235}'::real[],
	NULL::real[],
	NULL::real[],
	NULL::real[],
	E'{"1.37","48.18","81.41","111.00","143.56","176.12","208.48","244.00","277.10","312.00","354.24","397.50","432.81","473.83","514.15","559.74","605.17","653.04","703.08","749.70","793.80","843.72","897.00","950.04","1005.84","1059.20","1111.86","1168.53","1220.78","1273.14","1335.78","1395.80","1464.48","1525.11","1588.62","1662.60","1733.62","1808.00","1876.80","1943.17","2017.47","2095.83","2176.23","2255.84","2332.80","2416.80","2500.54","2583.36","2660.40","2751.20","2841.54","2936.19","3028.06","3126.61","3219.72","3316.14","3407.04","3508.20","3613.50","3714.04","3820.32","3927.70","4045.88","4164.11","4286.55","4404.60","4526.64","4639.62","4772.80","4907.30","5047.20","5174.40","5313.60","5456.64","5608.32","5770.80","5925.96","6098.88","6277.32","6458.50","6637.23","6822.90","7017.40","7216.44","7423.52","7672.96","7894.12","8146.60","8427.82","8709.95","8969.40","9303.91","9642.60","10018.92","10424.34","10892.97","11484.75","12242.88","13182.40","14456.88","19415.00"}'::numeric[],
	NULL::numeric[],
	NULL::numeric[],
	NULL::numeric[],
	NULL::numeric[]);

--
-- Table: store_sales, Attribute: ss_ext_tax
--
-- DELETE FROM pg_statistic WHERE starelid='cte_bug.store_sales'::regclass AND staattnum=19;
INSERT INTO pg_statistic VALUES (
	'cte_bug.store_sales'::regclass,
	19::smallint,
	False::boolean,
	0.0449::real,
	6::integer,
	18471.0::real,
	1::smallint,
	2::smallint,
	3::smallint,
	0::smallint,
	0::smallint,
	1752::oid,
	1754::oid,
	1754::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	'{0.10456666, 0.0009, 0.00073333335, 0.00073333335, 0.0007, 0.0007, 0.00066666666, 0.00066666666, 0.00066666666, 0.00063333334, 0.00063333334, 0.00063333334, 0.00063333334, 0.00063333334, 0.0006, 0.0006, 0.0006, 0.00056666665, 0.00056666665, 0.00056666665, 0.00056666665, 0.00056666665, 0.00053333334, 0.00053333334, 0.00053333334, 0.00053333334, 0.00053333334, 0.00053333334, 0.00053333334, 0.00053333334, 0.0005, 0.0005, 0.0005, 0.0005, 0.0005, 0.0005, 0.0005, 0.0005, 0.0005, 0.0005, 0.0005, 0.0005, 0.00046666668, 0.00046666668, 0.00046666668, 0.00046666668, 0.00046666668, 0.00046666668, 0.00046666668, 0.00046666668, 0.00046666668, 0.00046666668, 0.00046666668, 0.00046666668, 0.00046666668, 0.00046666668, 0.00046666668, 0.00046666668, 0.00046666668, 0.00043333333, 0.00043333333, 0.00043333333, 0.00043333333, 0.00043333333, 0.00043333333, 0.00043333333, 0.00043333333, 0.00043333333, 0.00043333333, 0.00043333333, 0.00043333333, 0.00043333333, 0.00043333333, 0.0004, 0.0004, 0.0004, 0.0004, 0.0004, 0.0004, 0.0004, 0.0004, 0.0004, 0.0004, 0.0004, 0.0004, 0.0004, 0.0004, 0.0004, 0.0004, 0.0004, 0.0004, 0.0004, 0.0004, 0.0004, 0.0004, 0.0004, 0.0004, 0.0004, 0.0004, 0.0004}'::real[],
	NULL::real[],
	'{0.014879999}'::real[],
	NULL::real[],
	NULL::real[],
	E'{"0.00","0.01","0.17","0.65","0.13","0.29","0.05","0.08","0.36","0.10","0.27","0.28","0.40","0.45","0.19","0.30","2.24","0.15","0.18","0.21","0.41","3.94","0.14","0.46","0.47","0.52","0.88","1.04","1.51","1.66","0.02","0.12","0.16","0.20","0.25","0.34","0.77","0.96","0.98","1.29","1.92","3.80","0.11","0.32","0.42","0.43","0.51","0.68","0.99","1.05","1.22","1.31","1.36","1.54","1.80","1.90","2.04","2.10","3.32","0.09","0.22","0.44","0.50","0.58","0.66","0.94","1.13","1.15","1.68","2.74","3.78","4.08","8.96","0.04","0.07","0.23","0.33","0.37","0.59","0.69","0.78","0.81","0.82","0.85","0.91","1.08","1.12","1.18","1.26","1.32","1.35","1.50","1.59","1.64","1.76","1.79","2.26","2.83","3.35","3.43"}'::numeric[],
	E'{"0.03","0.74","1.20","1.60","2.01","2.39","2.72","3.10","3.50","3.90","4.30","4.72","5.16","5.57","6.04","6.50","7.03","7.58","8.18","8.74","9.36","9.98","10.62","11.27","12.01","12.69","13.39","14.17","14.94","15.73","16.52","17.47","18.36","19.22","20.18","21.05","22.15","23.17","24.17","25.34","26.50","27.68","28.91","30.07","31.25","32.68","34.20","35.59","37.26","38.92","40.45","42.33","44.13","45.79","47.82","49.94","51.93","54.20","56.44","58.78","61.05","63.96","66.70","69.47","72.63","75.56","78.68","82.11","85.62","89.37","92.68","96.79","100.78","105.67","110.09","114.97","120.82","125.97","132.10","138.64","145.86","152.49","159.87","168.58","177.12","186.59","196.23","208.25","220.55","235.45","251.96","267.62","288.25","309.58","335.43","364.88","405.05","457.89","534.84","648.41","1390.48"}'::numeric[],
	NULL::numeric[],
	NULL::numeric[],
	NULL::numeric[]);

--
-- Table: store_sales, Attribute: ss_coupon_amt
--
-- DELETE FROM pg_statistic WHERE starelid='cte_bug.store_sales'::regclass AND staattnum=20;
INSERT INTO pg_statistic VALUES (
	'cte_bug.store_sales'::regclass,
	20::smallint,
	False::boolean,
	0.045333333::real,
	3::integer,
	6760.0::real,
	1::smallint,
	2::smallint,
	3::smallint,
	0::smallint,
	0::smallint,
	1752::oid,
	1754::oid,
	1754::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	'{0.7661}'::real[],
	NULL::real[],
	'{0.6438912}'::real[],
	NULL::real[],
	NULL::real[],
	E'{"0.00"}'::numeric[],
	E'{"0.04","1.34","3.72","6.09","8.37","10.99","14.04","16.95","19.44","22.34","24.81","28.43","32.34","35.35","39.39","44.25","49.00","54.54","59.98","65.63","71.63","78.32","84.28","90.37","96.62","104.19","111.32","118.10","126.42","136.18","143.96","153.17","162.16","171.42","182.95","192.33","201.85","211.36","222.74","236.09","252.09","266.81","278.33","292.76","310.46","324.68","340.74","359.05","380.26","398.53","416.30","434.21","455.13","474.14","491.53","516.67","538.59","556.85","586.81","610.95","636.53","667.62","692.38","723.55","756.71","796.95","834.72","868.00","906.95","948.96","993.70","1033.87","1089.17","1132.81","1184.95","1228.29","1281.16","1347.98","1408.84","1480.92","1572.04","1643.21","1731.83","1830.90","1925.89","2022.22","2130.03","2281.99","2428.33","2573.26","2717.71","2913.34","3146.60","3438.96","3740.97","4126.94","4666.03","5274.03","6096.32","7082.14","12922.81"}'::numeric[],
	NULL::numeric[],
	NULL::numeric[],
	NULL::numeric[]);

--
-- Table: store_sales, Attribute: ss_net_paid
--
-- DELETE FROM pg_statistic WHERE starelid='cte_bug.store_sales'::regclass AND staattnum=21;
INSERT INTO pg_statistic VALUES (
	'cte_bug.store_sales'::regclass,
	21::smallint,
	False::boolean,
	0.045033332::real,
	6::integer,
	130231.0::real,
	1::smallint,
	2::smallint,
	3::smallint,
	0::smallint,
	0::smallint,
	1752::oid,
	1754::oid,
	1754::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	'{0.010733333}'::real[],
	NULL::real[],
	'{-0.003218649}'::real[],
	NULL::real[],
	NULL::real[],
	E'{"0.00"}'::numeric[],
	E'{"0.04","5.14","10.18","16.30","21.88","28.60","36.24","44.10","51.49","60.06","68.70","77.69","87.72","97.58","108.51","119.68","131.13","142.74","154.70","167.30","180.87","194.66","210.49","225.68","242.00","258.44","273.14","288.96","307.68","325.44","342.37","363.00","380.16","400.43","423.00","444.15","468.72","492.00","519.68","544.00","572.40","600.96","629.30","656.70","686.28","718.06","749.00","782.88","817.80","854.72","893.10","930.83","967.59","1008.80","1050.42","1093.00","1135.33","1181.62","1229.40","1280.52","1326.28","1383.48","1439.88","1500.29","1556.02","1618.37","1682.20","1751.04","1818.15","1884.06","1955.88","2033.45","2126.88","2215.62","2305.35","2400.96","2495.65","2607.80","2725.97","2838.84","2961.00","3081.25","3215.94","3363.36","3525.20","3685.50","3870.99","4065.12","4264.36","4472.25","4715.82","5020.00","5297.49","5657.44","6056.08","6494.87","7012.56","7766.28","8648.46","10058.40","17738.88"}'::numeric[],
	NULL::numeric[],
	NULL::numeric[],
	NULL::numeric[]);

--
-- Table: store_sales, Attribute: ss_net_paid_inc_tax
--
-- DELETE FROM pg_statistic WHERE starelid='cte_bug.store_sales'::regclass AND staattnum=22;
INSERT INTO pg_statistic VALUES (
	'cte_bug.store_sales'::regclass,
	22::smallint,
	False::boolean,
	0.04476667::real,
	6::integer,
	216373.0::real,
	1::smallint,
	2::smallint,
	3::smallint,
	0::smallint,
	0::smallint,
	1752::oid,
	1754::oid,
	1754::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	'{0.010666667, 0.0002}'::real[],
	NULL::real[],
	'{-0.0055286656}'::real[],
	NULL::real[],
	NULL::real[],
	E'{"0.00","32.50"}'::numeric[],
	E'{"0.04","5.34","10.64","16.84","22.78","29.76","37.54","45.88","53.55","63.01","71.53","81.22","91.04","101.48","113.40","124.84","136.79","148.47","160.74","174.09","189.27","202.53","219.10","235.75","252.63","270.30","284.97","302.27","321.39","339.01","359.73","379.11","399.51","420.01","443.22","467.14","491.68","517.50","543.21","569.64","598.99","628.01","657.79","689.15","718.44","753.62","785.05","818.47","856.64","895.72","934.06","972.23","1014.07","1056.37","1098.32","1143.65","1191.49","1240.73","1292.04","1341.11","1393.32","1450.62","1510.08","1566.98","1629.98","1689.68","1763.67","1840.23","1903.93","1975.54","2049.22","2126.92","2226.03","2316.79","2410.56","2506.52","2616.49","2727.26","2849.57","2970.38","3085.20","3217.68","3363.97","3517.49","3689.45","3858.39","4052.56","4254.30","4465.59","4690.92","4962.17","5234.70","5542.48","5925.61","6319.65","6827.42","7372.02","8128.19","9079.02","10544.85","18803.21"}'::numeric[],
	NULL::numeric[],
	NULL::numeric[],
	NULL::numeric[]);

--
-- Table: store_sales, Attribute: ss_net_profit
--
-- DELETE FROM pg_statistic WHERE starelid='cte_bug.store_sales'::regclass AND staattnum=23;
INSERT INTO pg_statistic VALUES (
	'cte_bug.store_sales'::regclass,
	23::smallint,
	False::boolean,
	0.043933332::real,
	6::integer,
	164410.0::real,
	1::smallint,
	2::smallint,
	3::smallint,
	0::smallint,
	0::smallint,
	1752::oid,
	1754::oid,
	1754::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	'{0.00036666667, 0.0002}'::real[],
	NULL::real[],
	'{0.0071685645}'::real[],
	NULL::real[],
	NULL::real[],
	E'{"0.00","-86.40"}'::numeric[],
	E'{"-9756.88","-6515.41","-5668.87","-5123.34","-4724.97","-4368.00","-4064.06","-3808.62","-3576.47","-3373.46","-3174.31","-3012.09","-2859.44","-2725.70","-2593.63","-2458.24","-2337.30","-2214.00","-2107.00","-2001.96","-1904.70","-1816.40","-1727.86","-1642.55","-1561.33","-1494.48","-1420.30","-1356.48","-1294.21","-1224.96","-1162.20","-1101.58","-1048.46","-992.62","-942.72","-898.95","-853.76","-806.47","-765.12","-727.68","-693.00","-655.50","-623.42","-589.44","-557.05","-526.40","-495.72","-462.77","-435.73","-406.64","-378.40","-353.30","-328.97","-301.50","-278.13","-255.75","-233.87","-214.09","-195.96","-178.34","-162.31","-147.92","-132.24","-117.78","-104.00","-89.56","-76.54","-65.12","-54.20","-43.00","-32.46","-23.28","-14.28","-7.00","0.39","9.24","20.91","35.88","53.56","74.70","99.36","127.98","158.51","193.25","233.24","281.96","341.00","404.32","478.86","563.93","661.20","773.80","891.78","1050.06","1209.00","1410.30","1700.48","2034.00","2537.48","3429.36","8262.72"}'::numeric[],
	NULL::numeric[],
	NULL::numeric[],
	NULL::numeric[]);

--
-- Table: web_sales, Attribute: ws_sold_date_sk
--
-- DELETE FROM pg_statistic WHERE starelid='cte_bug.web_sales'::regclass AND staattnum=1;
INSERT INTO pg_statistic VALUES (
	'cte_bug.web_sales'::regclass,
	1::smallint,
	False::boolean,
	0.00036666667::real,
	4::integer,
	1823.0::real,
	1::smallint,
	2::smallint,
	3::smallint,
	0::smallint,
	0::smallint,
	96::oid,
	97::oid,
	97::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	'{0.0016, 0.0016, 0.0015333333, 0.0015, 0.0015, 0.0015, 0.0014666667, 0.0014666667, 0.0014333334, 0.0014, 0.0014, 0.0014, 0.0014, 0.0014, 0.0014, 0.0013666666, 0.0013666666, 0.0013666666, 0.0013666666, 0.0013666666, 0.0013666666, 0.0013666666, 0.0013333333, 0.0013333333, 0.0013333333, 0.0013333333, 0.0013333333, 0.0013333333, 0.0013333333, 0.0013333333, 0.0013, 0.0013, 0.0013, 0.0013, 0.0013, 0.0013, 0.0013, 0.0013, 0.0013, 0.0013, 0.0013, 0.0013, 0.0012666667, 0.0012666667, 0.0012666667, 0.0012666667, 0.0012666667, 0.0012666667, 0.0012666667, 0.0012666667, 0.0012666667, 0.0012666667, 0.0012666667, 0.0012333334, 0.0012333334, 0.0012333334, 0.0012333334, 0.0012333334, 0.0012333334, 0.0012333334, 0.0012333334, 0.0012333334, 0.0012333334, 0.0012333334, 0.0012333334, 0.0012333334, 0.0012333334, 0.0012333334, 0.0012333334, 0.0012333334, 0.0012, 0.0012, 0.0012, 0.0012, 0.0012, 0.0012, 0.0012, 0.0012, 0.0012, 0.0012, 0.0012, 0.0012, 0.0012, 0.0012, 0.0012, 0.0012, 0.0012, 0.0012, 0.0012, 0.0012, 0.0011666666, 0.0011666666, 0.0011666666, 0.0011666666, 0.0011666666, 0.0011666666, 0.0011666666, 0.0011666666, 0.0011666666, 0.0011666666}'::real[],
	NULL::real[],
	'{-0.0030525422}'::real[],
	NULL::real[],
	NULL::real[],
	E'{"2451524","2452255","2452593","2451546","2452243","2452632","2451179","2451491","2451545","2451176","2451487","2451860","2451866","2452271","2452626","2451154","2451534","2451536","2452228","2452236","2452592","2452603","2451135","2451495","2451871","2451887","2451901","2451910","2452267","2452605","2451053","2451121","2451177","2451537","2451539","2452220","2452229","2452241","2452261","2452617","2452621","2452630","2451122","2451142","2451143","2451144","2451169","2451174","2451490","2452225","2452234","2452245","2452582","2451150","2451156","2451410","2451505","2451510","2451527","2451543","2451867","2451873","2451881","2452219","2452227","2452248","2452262","2452265","2452272","2452624","2451124","2451129","2451145","2451159","2451161","2451165","2451512","2451532","2451544","2451858","2452247","2452249","2452252","2452266","2452277","2452585","2452589","2452591","2452610","2452612","2451094","2451134","2451139","2451155","2451157","2451166","2451171","2451181","2451521","2451522"}'::int4[],
	E'{"2450816","2450841","2450870","2450898","2450927","2450953","2450978","2451005","2451030","2451041","2451052","2451067","2451078","2451090","2451104","2451115","2451127","2451140","2451158","2451173","2451198","2451225","2451253","2451279","2451305","2451336","2451363","2451388","2451405","2451418","2451431","2451444","2451456","2451468","2451480","2451494","2451504","2451516","2451529","2451542","2451574","2451599","2451625","2451654","2451681","2451709","2451736","2451761","2451773","2451785","2451798","2451811","2451824","2451835","2451849","2451857","2451872","2451883","2451893","2451902","2451912","2451938","2451969","2451995","2452023","2452052","2452082","2452108","2452130","2452142","2452153","2452165","2452176","2452188","2452200","2452213","2452226","2452240","2452256","2452270","2452289","2452315","2452345","2452373","2452401","2452429","2452455","2452480","2452497","2452509","2452522","2452534","2452547","2452559","2452571","2452584","2452597","2452607","2452619","2452633","2452642"}'::int4[],
	NULL::int4[],
	NULL::int4[],
	NULL::int4[]);

--
-- Table: web_sales, Attribute: ws_sold_time_sk
--
-- DELETE FROM pg_statistic WHERE starelid='cte_bug.web_sales'::regclass AND staattnum=2;
INSERT INTO pg_statistic VALUES (
	'cte_bug.web_sales'::regclass,
	2::smallint,
	False::boolean,
	0.0004::real,
	4::integer,
	72739.0::real,
	1::smallint,
	2::smallint,
	3::smallint,
	0::smallint,
	0::smallint,
	96::oid,
	97::oid,
	97::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	'{0.0002}'::real[],
	NULL::real[],
	'{-0.004440563}'::real[],
	NULL::real[],
	NULL::real[],
	E'{"61224"}'::int4[],
	E'{"0","3453","5358","7271","8890","10655","12362","14210","15455","16483","17601","18731","20003","21248","22548","23694","24821","25793","26731","27644","28537","29469","30431","31334","32383","33161","33815","34512","35196","35909","36662","37414","38129","38856","39594","40332","41139","41826","42634","43309","43883","44435","44958","45430","45979","46461","47039","47768","48454","49168","49903","50684","51931","53162","54270","55196","56147","57080","57913","58603","59313","60034","60759","61432","62068","62610","63196","63765","64286","64834","65399","65944","66456","66942","67447","67990","68472","68929","69389","69801","70217","70674","71131","71595","72062","72552","73092","73643","74121","74642","75146","75708","76439","77167","77837","78531","79155","80293","81570","82687","86389"}'::int4[],
	NULL::int4[],
	NULL::int4[],
	NULL::int4[]);

--
-- Table: web_sales, Attribute: ws_ship_date_sk
--
-- DELETE FROM pg_statistic WHERE starelid='cte_bug.web_sales'::regclass AND staattnum=3;
INSERT INTO pg_statistic VALUES (
	'cte_bug.web_sales'::regclass,
	3::smallint,
	False::boolean,
	0.00033333333::real,
	4::integer,
	1933.0::real,
	1::smallint,
	2::smallint,
	3::smallint,
	0::smallint,
	0::smallint,
	96::oid,
	97::oid,
	97::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	'{0.0015333333, 0.0014666667, 0.0013333333, 0.0013, 0.0012666667, 0.0012333334, 0.0012333334, 0.0012, 0.0012, 0.0012, 0.0012, 0.0012, 0.0011666666, 0.0011666666, 0.0011666666, 0.0011333333, 0.0011333333, 0.0011333333, 0.0011333333, 0.0011333333, 0.0011333333, 0.0011333333, 0.0011, 0.0011, 0.0011, 0.0011, 0.0011, 0.0011, 0.0011, 0.0011, 0.0011, 0.0010666667, 0.0010666667, 0.0010666667, 0.0010666667, 0.0010666667, 0.0010666667, 0.0010666667, 0.0010666667, 0.0010666667, 0.0010666667, 0.0010666667, 0.0010666667, 0.0010666667, 0.0010333334, 0.0010333334, 0.0010333334, 0.0010333334, 0.0010333334, 0.0010333334, 0.0010333334, 0.0010333334, 0.0010333334, 0.0010333334, 0.0010333334, 0.0010333334, 0.0010333334, 0.0010333334, 0.0010333334, 0.0010333334, 0.0010333334, 0.0010333334, 0.0010333334, 0.0010333334, 0.0010333334, 0.0010333334, 0.0010333334, 0.0010333334, 0.0010333334, 0.001, 0.001, 0.001, 0.001, 0.001, 0.001, 0.001, 0.001, 0.001, 0.001, 0.001, 0.001, 0.001, 0.001, 0.001, 0.001, 0.001, 0.001, 0.001, 0.001, 0.001, 0.001, 0.001, 0.001, 0.0009666667, 0.0009666667, 0.0009666667, 0.0009666667, 0.0009666667, 0.0009666667, 0.0009666667}'::real[],
	NULL::real[],
	'{-0.0028389376}'::real[],
	NULL::real[],
	NULL::real[],
	E'{"2451902","2451188","2451164","2451549","2452285","2451156","2451195","2451918","2451956","2452264","2452636","2452641","2451197","2451870","2452241","2451538","2451539","2452279","2452292","2452608","2452626","2452633","2451146","2451167","2451178","2451537","2451547","2451905","2451923","2452250","2452263","2451133","2451201","2451557","2451563","2451607","2451612","2451975","2451986","2452226","2452273","2452281","2452358","2452657","2451171","2451181","2451489","2451518","2451550","2451569","2451935","2451964","2451965","2451966","2451974","2452219","2452251","2452259","2452260","2452261","2452271","2452280","2452300","2452301","2452302","2452317","2452604","2452652","2452665","2451147","2451161","2451177","2451183","2451187","2451189","2451196","2451229","2451545","2451565","2451573","2451575","2451601","2451904","2451951","2451952","2452265","2452288","2452342","2452629","2452635","2452637","2452639","2452696","2451173","2451174","2451184","2451192","2451202","2451212","2451242"}'::int4[],
	E'{"2450823","2450897","2450934","2450961","2450989","2451015","2451043","2451061","2451080","2451098","2451114","2451127","2451140","2451154","2451169","2451191","2451209","2451222","2451235","2451248","2451263","2451281","2451306","2451332","2451361","2451390","2451416","2451438","2451454","2451470","2451485","2451500","2451513","2451526","2451540","2451555","2451568","2451582","2451595","2451611","2451627","2451646","2451668","2451693","2451722","2451751","2451778","2451800","2451819","2451836","2451850","2451863","2451876","2451888","2451900","2451914","2451927","2451940","2451953","2451971","2451990","2452009","2452037","2452064","2452091","2452121","2452146","2452167","2452186","2452203","2452218","2452232","2452246","2452262","2452278","2452295","2452310","2452322","2452335","2452350","2452367","2452385","2452412","2452441","2452470","2452495","2452518","2452538","2452555","2452572","2452587","2452600","2452614","2452625","2452644","2452658","2452670","2452682","2452699","2452719","2452762"}'::int4[],
	NULL::int4[],
	NULL::int4[],
	NULL::int4[]);

--
-- Table: web_sales, Attribute: ws_item_sk
--
-- DELETE FROM pg_statistic WHERE starelid='cte_bug.web_sales'::regclass AND staattnum=4;
INSERT INTO pg_statistic VALUES (
	'cte_bug.web_sales'::regclass,
	4::smallint,
	False::boolean,
	0.0::real,
	4::integer,
	25062.0::real,
	1::smallint,
	2::smallint,
	3::smallint,
	0::smallint,
	0::smallint,
	96::oid,
	97::oid,
	97::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	'{0.00023333334, 0.00023333334, 0.00023333334, 0.00023333334, 0.00023333334, 0.00023333334, 0.00023333334, 0.00023333334, 0.00023333334, 0.00023333334, 0.00023333334, 0.00023333334, 0.00023333334, 0.00023333334, 0.00023333334}'::real[],
	NULL::real[],
	'{0.0007646001}'::real[],
	NULL::real[],
	NULL::real[],
	E'{"1309","3001","4099","5413","8575","10591","14557","17821","19507","20473","22969","24061","25279","25891","27685"}'::int4[],
	E'{"3","292","595","880","1169","1423","1683","1956","2282","2539","2800","3062","3343","3608","3865","4153","4415","4682","4951","5245","5505","5762","6071","6367","6655","6949","7232","7535","7831","8117","8386","8665","8928","9193","9470","9724","10001","10264","10546","10828","11122","11402","11708","11998","12271","12547","12823","13105","13399","13675","13977","14251","14527","14821","15112","15404","15715","16020","16304","16561","16846","17098","17353","17617","17892","18163","18434","18732","19039","19317","19609","19870","20149","20450","20749","21043","21305","21579","21881","22171","22460","22750","23029","23276","23536","23794","24084","24373","24664","24932","25236","25519","25793","26068","26349","26599","26899","27169","27447","27703","28000"}'::int4[],
	NULL::int4[],
	NULL::int4[],
	NULL::int4[]);

--
-- Table: web_sales, Attribute: ws_bill_customer_sk
--
-- DELETE FROM pg_statistic WHERE starelid='cte_bug.web_sales'::regclass AND staattnum=5;
INSERT INTO pg_statistic VALUES (
	'cte_bug.web_sales'::regclass,
	5::smallint,
	False::boolean,
	0.0005::real,
	4::integer,
	2118340.0::real,
	2::smallint,
	3::smallint,
	0::smallint,
	0::smallint,
	0::smallint,
	97::oid,
	97::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	NULL::real[],
	'{0.0010770214}'::real[],
	NULL::real[],
	NULL::real[],
	NULL::real[],
	E'{"116","19276","39962","61407","85373","106315","126227","147006","167677","187188","207807","226047","246827","269215","286863","307913","329088","349611","369725","390185","410503","432212","454393","475658","496403","519145","539722","559383","580534","601568","623450","645203","665659","686268","708568","730472","751325","772013","790899","811688","835234","859298","877602","896824","916526","937382","958518","980789","1000789","1022926","1044299","1064876","1087128","1107756","1128946","1150087","1170021","1189394","1210544","1229439","1250913","1270764","1292927","1312446","1334616","1355684","1377519","1397078","1417253","1439187","1460077","1482246","1504744","1525557","1548534","1571550","1593101","1612385","1632941","1652821","1671352","1691730","1714302","1736611","1757116","1778569","1799839","1820986","1843004","1862949","1886606","1909295","1929403","1952006","1972435","1993425","2015871","2037659","2058030","2078476","2099961"}'::int4[],
	NULL::int4[],
	NULL::int4[],
	NULL::int4[],
	NULL::int4[]);

--
-- Table: web_sales, Attribute: ws_bill_cdemo_sk
--
-- DELETE FROM pg_statistic WHERE starelid='cte_bug.web_sales'::regclass AND staattnum=6;
INSERT INTO pg_statistic VALUES (
	'cte_bug.web_sales'::regclass,
	6::smallint,
	False::boolean,
	0.00036666667::real,
	4::integer,
	1963935.0::real,
	2::smallint,
	3::smallint,
	0::smallint,
	0::smallint,
	0::smallint,
	97::oid,
	97::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	NULL::real[],
	'{0.008824349}'::real[],
	NULL::real[],
	NULL::real[],
	NULL::real[],
	E'{"40","19928","41222","60907","80036","96922","115963","135857","155159","175803","193832","214446","233794","253659","271138","289757","310150","329469","349317","369098","389298","407426","427564","448714","467530","487468","506816","526861","546227","565019","583873","602430","621270","641783","661497","681303","700310","718758","737636","756383","774238","793788","810674","829641","848804","867427","886648","906085","926348","946633","965764","984667","1004930","1025614","1045598","1064240","1084145","1103410","1122730","1141053","1162951","1181651","1200809","1219112","1239681","1256860","1276450","1295516","1315122","1334158","1352740","1370260","1387732","1406685","1424904","1445396","1462767","1481762","1502206","1522443","1540631","1559362","1578315","1599220","1619332","1635595","1652747","1672436","1691569","1710869","1731393","1750861","1769679","1790304","1809446","1827792","1845980","1861792","1881771","1900390","1920682"}'::int4[],
	NULL::int4[],
	NULL::int4[],
	NULL::int4[],
	NULL::int4[]);

--
-- Table: web_sales, Attribute: ws_bill_hdemo_sk
--
-- DELETE FROM pg_statistic WHERE starelid='cte_bug.web_sales'::regclass AND staattnum=7;
INSERT INTO pg_statistic VALUES (
	'cte_bug.web_sales'::regclass,
	7::smallint,
	False::boolean,
	0.0004::real,
	4::integer,
	7218.0::real,
	1::smallint,
	2::smallint,
	3::smallint,
	0::smallint,
	0::smallint,
	96::oid,
	97::oid,
	97::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	'{0.00043333333, 0.00043333333, 0.00043333333, 0.0004, 0.0004, 0.0004, 0.0004, 0.0004, 0.0004, 0.0004}'::real[],
	NULL::real[],
	'{0.0033058901}'::real[],
	NULL::real[],
	NULL::real[],
	E'{"3781","4004","5987","61","843","1723","3981","4638","5516","6255"}'::int4[],
	E'{"1","74","151","220","297","366","438","505","572","641","716","798","874","948","1023","1092","1157","1226","1295","1359","1442","1517","1582","1651","1718","1792","1861","1935","2012","2083","2154","2230","2301","2372","2434","2504","2576","2652","2724","2792","2863","2944","3013","3076","3151","3224","3294","3362","3441","3513","3581","3647","3726","3800","3878","3953","4022","4092","4172","4242","4311","4397","4466","4540","4612","4679","4750","4824","4901","4972","5042","5119","5194","5265","5338","5410","5486","5560","5636","5707","5777","5842","5916","5982","6054","6128","6200","6267","6337","6411","6487","6563","6634","6700","6776","6846","6915","6987","7054","7128","7200"}'::int4[],
	NULL::int4[],
	NULL::int4[],
	NULL::int4[]);

--
-- Table: web_sales, Attribute: ws_bill_addr_sk
--
-- DELETE FROM pg_statistic WHERE starelid='cte_bug.web_sales'::regclass AND staattnum=8;
INSERT INTO pg_statistic VALUES (
	'cte_bug.web_sales'::regclass,
	8::smallint,
	False::boolean,
	0.00043333333::real,
	4::integer,
	1052357.0::real,
	2::smallint,
	3::smallint,
	0::smallint,
	0::smallint,
	0::smallint,
	97::oid,
	97::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	NULL::real[],
	'{0.0035874592}'::real[],
	NULL::real[],
	NULL::real[],
	NULL::real[],
	E'{"85","9940","19201","29062","39466","49793","60322","70621","81217","91226","101312","111021","121563","131884","142160","151553","162420","173225","183462","193550","204825","215954","226612","236859","247641","257974","268112","278524","289530","301367","312534","323713","334146","344753","354965","366093","376805","387572","399555","409543","419685","430267","441452","451787","461680","472204","482625","492470","503572","513596","523426","533872","543655","554311","565216","576083","586017","596802","607700","617077","626774","637067","646810","658305","668298","678578","689888","699650","709454","719802","731419","742750","753186","764659","775390","785925","795925","806880","817378","827664","838917","850289","860509","871551","882170","892610","902269","911835","922478","933542","944712","955041","966086","976281","986032","996335","1007079","1017272","1027779","1039161","1049977"}'::int4[],
	NULL::int4[],
	NULL::int4[],
	NULL::int4[],
	NULL::int4[]);

--
-- Table: web_sales, Attribute: ws_ship_customer_sk
--
-- DELETE FROM pg_statistic WHERE starelid='cte_bug.web_sales'::regclass AND staattnum=9;
INSERT INTO pg_statistic VALUES (
	'cte_bug.web_sales'::regclass,
	9::smallint,
	False::boolean,
	0.00046666668::real,
	4::integer,
	2017265.0::real,
	2::smallint,
	3::smallint,
	0::smallint,
	0::smallint,
	0::smallint,
	97::oid,
	97::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	NULL::real[],
	'{-0.0043712403}'::real[],
	NULL::real[],
	NULL::real[],
	NULL::real[],
	E'{"132","20637","42180","65350","82849","103817","124318","145423","166944","187687","208737","230590","251949","273686","293387","311515","330800","353486","374240","393971","413452","433019","454488","476432","498011","518833","539886","561032","582486","602670","621844","644001","664886","685045","704748","725254","746493","768247","789025","809330","830506","851780","873145","893841","916134","939717","961138","980901","1000505","1021471","1041002","1061792","1082164","1105084","1127234","1147741","1168328","1188890","1211298","1233312","1255587","1277390","1297125","1318745","1339393","1359133","1378504","1398143","1419107","1441128","1464282","1484205","1506371","1531184","1554208","1572948","1594637","1616564","1637230","1658766","1679115","1701388","1723406","1741784","1760843","1783047","1803735","1827219","1849463","1870419","1891620","1911234","1930891","1951173","1973089","1994829","2016556","2037533","2057883","2079450","2099912"}'::int4[],
	NULL::int4[],
	NULL::int4[],
	NULL::int4[],
	NULL::int4[]);

--
-- Table: web_sales, Attribute: ws_ship_cdemo_sk
--
-- DELETE FROM pg_statistic WHERE starelid='cte_bug.web_sales'::regclass AND staattnum=10;
INSERT INTO pg_statistic VALUES (
	'cte_bug.web_sales'::regclass,
	10::smallint,
	False::boolean,
	0.0004::real,
	4::integer,
	1696478.0::real,
	2::smallint,
	3::smallint,
	0::smallint,
	0::smallint,
	0::smallint,
	97::oid,
	97::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	NULL::real[],
	'{-0.002879893}'::real[],
	NULL::real[],
	NULL::real[],
	NULL::real[],
	E'{"58","18440","37296","56770","75852","94757","113452","130704","151005","170386","188845","208340","228843","248582","267435","285987","306015","323298","341709","360888","379541","400412","418373","438856","457414","475601","496722","517639","535895","554469","574493","594102","613853","632766","651384","668989","687375","706348","726205","744691","763517","783257","803277","822267","842706","862226","882815","901593","921645","941476","960097","979312","997703","1019580","1040057","1057255","1076091","1095006","1112147","1132100","1151343","1170491","1188950","1206650","1225084","1243585","1261069","1279776","1298943","1318050","1338726","1357162","1375169","1394096","1413110","1433576","1450829","1470097","1488671","1509355","1530203","1549360","1567854","1587699","1610090","1629275","1648075","1668590","1688396","1707068","1725533","1743917","1762980","1780125","1799583","1819263","1838350","1859271","1880426","1901289","1920663"}'::int4[],
	NULL::int4[],
	NULL::int4[],
	NULL::int4[],
	NULL::int4[]);

--
-- Table: web_sales, Attribute: ws_ship_hdemo_sk
--
-- DELETE FROM pg_statistic WHERE starelid='cte_bug.web_sales'::regclass AND staattnum=11;
INSERT INTO pg_statistic VALUES (
	'cte_bug.web_sales'::regclass,
	11::smallint,
	False::boolean,
	0.00033333333::real,
	4::integer,
	7200.0::real,
	1::smallint,
	2::smallint,
	3::smallint,
	0::smallint,
	0::smallint,
	96::oid,
	97::oid,
	97::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	'{0.00043333333, 0.0004, 0.0004, 0.0004, 0.0004, 0.0004, 0.0004, 0.0004, 0.0004, 0.0004, 0.0004, 0.0004}'::real[],
	NULL::real[],
	'{0.007957798}'::real[],
	NULL::real[],
	NULL::real[],
	E'{"1687","262","626","3196","3456","3820","5684","5796","5891","5987","6436","6562"}'::int4[],
	E'{"1","76","145","212","285","352","423","496","571","640","719","798","867","940","1016","1091","1163","1231","1304","1381","1455","1523","1597","1669","1746","1814","1887","1962","2035","2099","2168","2246","2310","2377","2455","2530","2597","2668","2741","2817","2892","2966","3033","3103","3170","3247","3325","3396","3465","3541","3609","3678","3755","3821","3889","3959","4025","4088","4162","4242","4318","4386","4454","4527","4593","4658","4728","4800","4870","4940","5010","5077","5153","5225","5302","5373","5446","5510","5580","5652","5726","5799","5877","5947","6019","6099","6174","6247","6329","6399","6475","6552","6625","6697","6761","6831","6904","6984","7057","7129","7200"}'::int4[],
	NULL::int4[],
	NULL::int4[],
	NULL::int4[]);

--
-- Table: web_sales, Attribute: ws_ship_addr_sk
--
-- DELETE FROM pg_statistic WHERE starelid='cte_bug.web_sales'::regclass AND staattnum=12;
INSERT INTO pg_statistic VALUES (
	'cte_bug.web_sales'::regclass,
	12::smallint,
	False::boolean,
	0.00036666667::real,
	4::integer,
	1114224.0::real,
	2::smallint,
	3::smallint,
	0::smallint,
	0::smallint,
	0::smallint,
	97::oid,
	97::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	NULL::real[],
	'{-0.001420308}'::real[],
	NULL::real[],
	NULL::real[],
	NULL::real[],
	E'{"26","11309","20636","30995","41857","51685","61820","71405","82282","92581","103069","114250","123704","133498","144567","155732","164935","175559","186082","196700","207930","217367","227511","237781","248348","259679","269692","279097","289318","300619","311485","321268","331270","341882","352231","361830","372945","383382","393987","405239","415555","427032","438461","448473","459112","470359","481524","492717","502982","513710","524165","534333","544663","554875","565192","576567","586168","598386","608959","620175","629750","639772","650199","660474","671797","681920","692252","703223","713224","723069","733136","744467","754388","765063","775010","786658","796244","807059","817848","828650","839270","851099","861790","871850","883229","894821","905183","915481","925975","936732","947814","958254","967993","977342","988420","998860","1009253","1019245","1029913","1039907","1049935"}'::int4[],
	NULL::int4[],
	NULL::int4[],
	NULL::int4[],
	NULL::int4[]);

--
-- Table: web_sales, Attribute: ws_web_page_sk
--
-- DELETE FROM pg_statistic WHERE starelid='cte_bug.web_sales'::regclass AND staattnum=13;
INSERT INTO pg_statistic VALUES (
	'cte_bug.web_sales'::regclass,
	13::smallint,
	False::boolean,
	0.00026666667::real,
	4::integer,
	172.0::real,
	1::smallint,
	2::smallint,
	3::smallint,
	0::smallint,
	0::smallint,
	96::oid,
	97::oid,
	97::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	'{0.012433333, 0.012266667, 0.012233334, 0.0122, 0.012133333, 0.012133333, 0.0121, 0.011833333, 0.0117, 0.011633334, 0.0116, 0.0114, 0.0114, 0.011366666, 0.011366666, 0.011166667, 0.0111, 0.0111, 0.0111, 0.011066667, 0.011, 0.011, 0.010966667, 0.010866666, 0.010866666, 0.0108, 0.010733333, 0.010666667, 0.010366667, 0.0101333335, 0.008866667, 0.0079, 0.0078, 0.0075333333, 0.0075, 0.0074333334, 0.0074, 0.0072333333, 0.0071666664, 0.0071333335, 0.0071333335, 0.0071, 0.0070666666, 0.0070666666, 0.007033333, 0.007033333, 0.007, 0.0069, 0.0069, 0.0068333335, 0.0068, 0.0067, 0.0067, 0.006666667, 0.0066333334, 0.0064666667, 0.0063, 0.0061666667, 0.0061, 0.0056666667, 0.005433333, 0.0054, 0.0052, 0.0052, 0.0051666666, 0.005133333, 0.0051, 0.0051, 0.0050666668, 0.0050666668, 0.0050333333, 0.0050333333, 0.0049666665, 0.0049666665, 0.0049333335, 0.0049333335, 0.0049333335, 0.0049333335, 0.0049, 0.004833333, 0.004833333, 0.004833333, 0.0048, 0.0048, 0.004766667, 0.0047, 0.0047, 0.0047, 0.0046666665, 0.0046666665, 0.0046666665, 0.0046333335, 0.0046333335, 0.0046333335, 0.0045666667, 0.0045666667, 0.0045666667, 0.0045666667, 0.0045666667, 0.0045666667}'::real[],
	NULL::real[],
	'{0.0037705055}'::real[],
	NULL::real[],
	NULL::real[],
	E'{"31","25","103","145","7","163","109","49","151","115","157","55","97","73","169","37","61","79","127","172","91","121","139","1","85","13","67","43","19","133","122","44","116","152","74","68","128","80","26","38","164","2","98","170","62","86","134","32","146","50","92","56","104","20","158","8","140","110","14","130","129","57","89","136","29","87","113","135","112","142","75","99","17","106","35","64","69","125","33","15","46","82","88","155","141","11","39","154","45","83","117","9","77","160","27","59","63","81","94","118"}'::int4[],
	E'{"3","3","4","5","6","10","12","16","18","21","22","23","24","28","30","34","40","40","41","47","47","51","52","52","53","54","58","65","65","70","71","71","76","76","84","93","93","95","100","100","101","105","107","107","111","114","119","120","123","124","126","131","132","137","138","143","144","147","148","149","150","153","156","159","161","162","165","166","167","167","171","171"}'::int4[],
	NULL::int4[],
	NULL::int4[],
	NULL::int4[]);

--
-- Table: web_sales, Attribute: ws_web_site_sk
--
-- DELETE FROM pg_statistic WHERE starelid='cte_bug.web_sales'::regclass AND staattnum=14;
INSERT INTO pg_statistic VALUES (
	'cte_bug.web_sales'::regclass,
	14::smallint,
	False::boolean,
	0.0003::real,
	4::integer,
	32.0::real,
	1::smallint,
	3::smallint,
	0::smallint,
	0::smallint,
	0::smallint,
	96::oid,
	97::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	'{0.060833335, 0.059366666, 0.058766667, 0.0585, 0.0584, 0.0576, 0.0549, 0.036, 0.0357, 0.034733333, 0.034666665, 0.034433335, 0.025, 0.0247, 0.024433333, 0.0242, 0.0242, 0.024066666, 0.024033334, 0.023866666, 0.023566667, 0.023533333, 0.023266668, 0.023166666, 0.0231, 0.022766666, 0.022666667, 0.013133333, 0.012033333, 0.011566667, 0.011566667, 0.010933333}'::real[],
	'{0.04239957}'::real[],
	NULL::real[],
	NULL::real[],
	NULL::real[],
	E'{"31","7","32","19","1","25","13","2","14","8","26","20","28","22","4","21","23","27","3","9","16","10","17","15","11","5","29","6","24","12","30","18"}'::int4[],
	NULL::int4[],
	NULL::int4[],
	NULL::int4[],
	NULL::int4[]);

--
-- Table: web_sales, Attribute: ws_ship_mode_sk
--
-- DELETE FROM pg_statistic WHERE starelid='cte_bug.web_sales'::regclass AND staattnum=15;
INSERT INTO pg_statistic VALUES (
	'cte_bug.web_sales'::regclass,
	15::smallint,
	False::boolean,
	0.00036666667::real,
	4::integer,
	20.0::real,
	1::smallint,
	3::smallint,
	0::smallint,
	0::smallint,
	0::smallint,
	96::oid,
	97::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	'{0.0519, 0.05183333, 0.050966665, 0.050866667, 0.050633334, 0.050566666, 0.050533332, 0.050333332, 0.049933333, 0.049933333, 0.049933333, 0.049833335, 0.049833335, 0.0496, 0.049433332, 0.049233332, 0.0488, 0.0488, 0.048466668, 0.0482}'::real[],
	'{0.056176268}'::real[],
	NULL::real[],
	NULL::real[],
	NULL::real[],
	E'{"11","4","17","19","16","5","8","12","6","7","18","2","10","15","9","20","1","3","13","14"}'::int4[],
	NULL::int4[],
	NULL::int4[],
	NULL::int4[],
	NULL::int4[]);

--
-- Table: web_sales, Attribute: ws_warehouse_sk
--
-- DELETE FROM pg_statistic WHERE starelid='cte_bug.web_sales'::regclass AND staattnum=16;
INSERT INTO pg_statistic VALUES (
	'cte_bug.web_sales'::regclass,
	16::smallint,
	False::boolean,
	0.00036666667::real,
	4::integer,
	5.0::real,
	1::smallint,
	3::smallint,
	0::smallint,
	0::smallint,
	0::smallint,
	96::oid,
	97::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	'{0.20156667, 0.20043333, 0.19946666, 0.19943333, 0.19873333}'::real[],
	'{0.19803289}'::real[],
	NULL::real[],
	NULL::real[],
	NULL::real[],
	E'{"3","4","1","5","2"}'::int4[],
	NULL::int4[],
	NULL::int4[],
	NULL::int4[],
	NULL::int4[]);

--
-- Table: web_sales, Attribute: ws_promo_sk
--
-- DELETE FROM pg_statistic WHERE starelid='cte_bug.web_sales'::regclass AND staattnum=17;
INSERT INTO pg_statistic VALUES (
	'cte_bug.web_sales'::regclass,
	17::smallint,
	False::boolean,
	0.0003::real,
	4::integer,
	357.0::real,
	1::smallint,
	2::smallint,
	3::smallint,
	0::smallint,
	0::smallint,
	96::oid,
	97::oid,
	97::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	'{0.0037666666, 0.0037, 0.0035333333, 0.0035333333, 0.0035}'::real[],
	NULL::real[],
	'{0.005415437}'::real[],
	NULL::real[],
	NULL::real[],
	E'{"169","252","72","118","335"}'::int4[],
	E'{"1","4","7","11","15","19","22","26","29","33","36","39","43","47","51","54","58","61","65","68","71","76","79","83","86","89","93","96","100","103","107","110","114","117","122","125","129","132","136","139","143","146","150","153","157","161","164","168","173","176","180","183","187","190","193","197","200","204","208","211","215","218","222","225","229","233","236","240","244","247","251","255","259","262","266","269","273","276","279","283","287","290","294","297","300","304","308","311","315","318","322","325","329","332","337","340","344","347","351","354","357"}'::int4[],
	NULL::int4[],
	NULL::int4[],
	NULL::int4[]);

--
-- Table: web_sales, Attribute: ws_order_number
--
-- DELETE FROM pg_statistic WHERE starelid='cte_bug.web_sales'::regclass AND staattnum=18;
INSERT INTO pg_statistic VALUES (
	'cte_bug.web_sales'::regclass,
	18::smallint,
	False::boolean,
	0.0::real,
	8::integer,
	31567772.0::real,
	2::smallint,
	3::smallint,
	0::smallint,
	0::smallint,
	0::smallint,
	412::oid,
	412::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	NULL::real[],
	'{0.06999581}'::real[],
	NULL::real[],
	NULL::real[],
	NULL::real[],
	E'{"121","317641","648578","945597","1288393","1573610","1860492","2157681","2459791","2761260","3028976","3321440","3616466","3914756","4237831","4544558","4833953","5074651","5373885","5669291","6007647","6291071","6582751","6873797","7171514","7488211","7794326","8082653","8395928","8704266","9030833","9316184","9652205","9924686","10207036","10507877","10826653","11124316","11413852","11700753","11996463","12275214","12553434","12843505","13133657","13432219","13722004","14024091","14320895","14620641","14953137","15244476","15539965","15844103","16163890","16449629","16748023","17025956","17343876","17630387","17939341","18239124","18538613","18878347","19178480","19467260","19779045","20065269","20381326","20712670","20992673","21280690","21613312","21911183","22191172","22482391","22796790","23114702","23395001","23716661","24042960","24340548","24628420","24894268","25221472","25528346","25827723","26138822","26441040","26745092","27033424","27319988","27614893","27924604","28205457","28532777","28829016","29120974","29422245","29678171","29997708"}'::int8[],
	NULL::int8[],
	NULL::int8[],
	NULL::int8[],
	NULL::int8[]);

--
-- Table: web_sales, Attribute: ws_quantity
--
-- DELETE FROM pg_statistic WHERE starelid='cte_bug.web_sales'::regclass AND staattnum=19;
INSERT INTO pg_statistic VALUES (
	'cte_bug.web_sales'::regclass,
	19::smallint,
	False::boolean,
	0.0003::real,
	4::integer,
	100.0::real,
	1::smallint,
	3::smallint,
	0::smallint,
	0::smallint,
	0::smallint,
	96::oid,
	97::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	'{0.0117, 0.011633334, 0.0113, 0.0111, 0.011066667, 0.010933333, 0.010933333, 0.0109, 0.0109, 0.0109, 0.010833333, 0.0108, 0.010766666, 0.0107, 0.010666667, 0.010566667, 0.010466667, 0.010466667, 0.0104, 0.010366667, 0.010366667, 0.010333333, 0.0103, 0.010266666, 0.010266666, 0.010266666, 0.010233333, 0.010233333, 0.010233333, 0.0102, 0.0101666665, 0.0101666665, 0.0101333335, 0.0101333335, 0.0101333335, 0.010066667, 0.010066667, 0.010066667, 0.010066667, 0.010066667, 0.010066667, 0.010066667, 0.010033334, 0.010033334, 0.010033334, 0.010033334, 0.010033334, 0.010033334, 0.01, 0.01, 0.01, 0.009966667, 0.009966667, 0.009933333, 0.0099, 0.0099, 0.009866667, 0.009866667, 0.009866667, 0.009833333, 0.0098, 0.0098, 0.009766666, 0.009766666, 0.009766666, 0.009733333, 0.009733333, 0.0097, 0.0097, 0.0097, 0.0097, 0.009666666, 0.009633333, 0.0096, 0.0096, 0.0096, 0.009566667, 0.009533334, 0.009533334, 0.009533334, 0.009533334, 0.0095, 0.0095, 0.009466667, 0.009466667, 0.0094, 0.0094, 0.0094, 0.009366667, 0.009366667, 0.009366667, 0.009333333, 0.009266667, 0.009233333, 0.0092, 0.009133333, 0.009133333, 0.009, 0.008933334, 0.008666666}'::real[],
	'{0.014699079}'::real[],
	NULL::real[],
	NULL::real[],
	NULL::real[],
	E'{"55","56","49","74","6","69","80","42","53","76","82","93","23","10","41","7","26","61","70","73","90","68","40","58","72","78","39","59","71","2","21","34","17","54","84","8","22","24","38","60","75","77","29","33","47","95","96","98","18","52","66","3","64","15","31","83","5","57","87","13","36","67","9","37","79","4","46","1","45","91","92","11","97","30","50","81","43","16","44","48","85","25","100","12","63","20","32","86","19","27","99","35","89","62","65","28","51","14","88","94"}'::int4[],
	NULL::int4[],
	NULL::int4[],
	NULL::int4[],
	NULL::int4[]);

--
-- Table: web_sales, Attribute: ws_wholesale_cost
--
-- DELETE FROM pg_statistic WHERE starelid='cte_bug.web_sales'::regclass AND staattnum=20;
INSERT INTO pg_statistic VALUES (
	'cte_bug.web_sales'::regclass,
	20::smallint,
	False::boolean,
	0.0004::real,
	6::integer,
	9928.0::real,
	1::smallint,
	2::smallint,
	3::smallint,
	0::smallint,
	0::smallint,
	1752::oid,
	1754::oid,
	1754::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	'{0.00033333333, 0.00033333333, 0.00033333333, 0.00033333333, 0.00033333333, 0.00033333333, 0.00033333333, 0.00033333333, 0.00033333333, 0.00033333333}'::real[],
	NULL::real[],
	'{-0.007165513}'::real[],
	NULL::real[],
	NULL::real[],
	E'{"25.53","30.19","51.31","53.30","55.63","60.00","60.37","72.31","83.48","87.79"}'::numeric[],
	E'{"1.00","2.00","3.01","3.99","5.01","5.98","6.90","7.81","8.78","9.77","10.82","11.75","12.73","13.64","14.64","15.70","16.68","17.70","18.55","19.52","20.51","21.47","22.49","23.60","24.61","25.59","26.60","27.68","28.73","29.69","30.74","31.63","32.62","33.59","34.69","35.74","36.73","37.59","38.60","39.62","40.68","41.65","42.59","43.49","44.44","45.41","46.35","47.34","48.33","49.28","50.23","51.18","52.17","53.17","54.21","55.20","56.12","57.20","58.17","59.12","60.16","61.11","62.13","63.13","64.04","65.08","66.03","66.99","67.92","69.02","69.96","71.00","71.88","72.93","73.95","74.98","76.06","77.09","78.09","79.05","79.99","81.05","82.06","83.02","84.06","85.19","86.28","87.20","88.17","89.18","90.18","91.03","92.00","92.96","94.05","94.97","95.97","96.93","97.94","98.99","100.00"}'::numeric[],
	NULL::numeric[],
	NULL::numeric[],
	NULL::numeric[]);

--
-- Table: web_sales, Attribute: ws_list_price
--
-- DELETE FROM pg_statistic WHERE starelid='cte_bug.web_sales'::regclass AND staattnum=21;
INSERT INTO pg_statistic VALUES (
	'cte_bug.web_sales'::regclass,
	21::smallint,
	False::boolean,
	0.00026666667::real,
	6::integer,
	23431.0::real,
	1::smallint,
	2::smallint,
	3::smallint,
	0::smallint,
	0::smallint,
	1752::oid,
	1754::oid,
	1754::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	'{0.00033333333, 0.0003, 0.00026666667}'::real[],
	NULL::real[],
	'{-0.011709301}'::real[],
	NULL::real[],
	NULL::real[],
	E'{"12.66","82.70","21.34"}'::numeric[],
	E'{"1.11","3.63","5.56","7.34","9.01","10.72","12.60","14.50","16.23","17.89","19.83","21.55","23.30","25.17","26.92","28.63","30.25","31.84","33.78","35.57","37.56","39.36","41.23","42.89","44.61","46.36","48.07","50.03","51.95","53.80","55.48","57.36","59.16","60.92","62.83","64.68","66.48","68.14","69.79","71.63","73.63","75.51","77.07","78.85","80.80","82.62","84.47","86.12","88.00","89.88","91.57","93.44","95.26","97.24","99.03","100.89","102.79","104.74","106.47","108.50","110.28","112.13","114.15","116.34","118.35","120.77","122.81","124.89","127.28","129.50","131.92","134.57","136.88","139.32","142.07","144.61","147.18","150.06","152.87","155.83","158.87","161.99","165.25","168.54","172.12","175.74","179.09","183.39","187.67","192.09","197.29","201.65","207.10","212.73","218.60","225.49","232.53","240.48","251.38","266.95","296.85"}'::numeric[],
	NULL::numeric[],
	NULL::numeric[],
	NULL::numeric[]);

--
-- Table: web_sales, Attribute: ws_sales_price
--
-- DELETE FROM pg_statistic WHERE starelid='cte_bug.web_sales'::regclass AND staattnum=22;
INSERT INTO pg_statistic VALUES (
	'cte_bug.web_sales'::regclass,
	22::smallint,
	False::boolean,
	0.00036666667::real,
	6::integer,
	14531.0::real,
	1::smallint,
	2::smallint,
	3::smallint,
	0::smallint,
	0::smallint,
	1752::oid,
	1754::oid,
	1754::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	'{0.0104, 0.00046666668, 0.00046666668, 0.00046666668, 0.00046666668, 0.00046666668, 0.00043333333, 0.00043333333, 0.00043333333, 0.00043333333, 0.00043333333, 0.00043333333, 0.0004, 0.0004, 0.0004, 0.0004, 0.0004, 0.0004, 0.0004, 0.0004, 0.0004, 0.0004, 0.0004, 0.0004, 0.0004, 0.0004, 0.0004, 0.0004, 0.0004, 0.00036666667, 0.00036666667, 0.00036666667, 0.00036666667, 0.00036666667, 0.00036666667, 0.00036666667, 0.00036666667, 0.00036666667, 0.00036666667, 0.00036666667, 0.00036666667, 0.00036666667, 0.00036666667, 0.00036666667, 0.00036666667, 0.00036666667, 0.00036666667, 0.00036666667, 0.00036666667, 0.00036666667, 0.00036666667, 0.00036666667, 0.00036666667, 0.00036666667, 0.00036666667, 0.00033333333, 0.00033333333, 0.00033333333, 0.00033333333, 0.00033333333, 0.00033333333, 0.00033333333, 0.00033333333, 0.00033333333, 0.00033333333, 0.00033333333, 0.00033333333, 0.00033333333, 0.00033333333, 0.00033333333, 0.00033333333, 0.00033333333, 0.00033333333, 0.00033333333, 0.00033333333, 0.00033333333, 0.00033333333, 0.00033333333, 0.00033333333, 0.00033333333, 0.00033333333, 0.00033333333, 0.00033333333, 0.00033333333, 0.00033333333, 0.00033333333, 0.00033333333, 0.00033333333, 0.00033333333, 0.00033333333, 0.00033333333, 0.00033333333, 0.00033333333, 0.00033333333, 0.00033333333, 0.00033333333, 0.00033333333, 0.00033333333, 0.00033333333, 0.00033333333}'::real[],
	NULL::real[],
	'{-0.0046185157}'::real[],
	NULL::real[],
	NULL::real[],
	E'{"0.00","2.06","2.70","3.03","3.83","3.87","0.80","0.94","3.04","4.70","5.59","11.09","0.70","0.89","1.17","1.28","1.33","1.55","1.81","2.13","2.80","3.51","3.68","3.75","3.77","4.11","4.41","6.03","15.17","0.41","0.72","0.86","1.12","1.21","1.44","1.66","2.24","2.46","2.92","3.38","3.40","3.43","4.23","4.64","4.98","5.48","5.60","5.68","6.12","6.37","6.49","7.58","8.47","11.40","15.80","0.30","0.66","0.71","1.14","1.18","1.32","1.49","1.53","1.70","1.89","2.03","2.05","2.22","2.37","2.43","2.52","2.53","2.78","2.82","2.87","2.88","3.25","3.61","3.67","3.71","3.94","3.95","4.17","4.22","4.59","4.82","4.91","5.07","5.19","5.24","5.64","5.70","6.11","6.53","6.57","6.95","7.00","7.73","7.84","7.99"}'::numeric[],
	E'{"0.01","0.60","1.11","1.64","2.20","2.74","3.31","3.91","4.48","5.09","5.63","6.17","6.81","7.45","8.09","8.66","9.30","9.96","10.52","11.12","11.75","12.44","13.09","13.82","14.54","15.25","15.99","16.72","17.50","18.22","19.04","19.87","20.70","21.43","22.29","23.23","24.09","25.02","25.92","26.85","27.82","28.75","29.71","30.71","31.75","32.78","33.78","34.80","35.93","37.02","38.14","39.29","40.43","41.60","42.81","44.07","45.33","46.59","47.86","49.28","50.73","52.22","53.69","55.11","56.86","58.38","60.03","61.68","63.22","65.10","66.78","68.62","70.35","72.19","74.30","76.43","78.67","81.00","83.19","85.26","87.83","90.51","93.08","95.96","98.78","102.09","105.42","109.06","112.87","116.26","120.47","125.09","130.89","136.72","143.90","151.28","160.21","170.24","184.92","205.80","292.81"}'::numeric[],
	NULL::numeric[],
	NULL::numeric[],
	NULL::numeric[]);

--
-- Table: web_sales, Attribute: ws_ext_discount_amt
--
-- DELETE FROM pg_statistic WHERE starelid='cte_bug.web_sales'::regclass AND staattnum=23;
INSERT INTO pg_statistic VALUES (
	'cte_bug.web_sales'::regclass,
	23::smallint,
	False::boolean,
	0.00023333334::real,
	6::integer,
	143461.0::real,
	1::smallint,
	2::smallint,
	3::smallint,
	0::smallint,
	0::smallint,
	1752::oid,
	1754::oid,
	1754::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	'{0.008966667, 0.0002, 0.0002}'::real[],
	NULL::real[],
	'{-0.0031384944}'::real[],
	NULL::real[],
	NULL::real[],
	E'{"0.00","47.60","333.00"}'::numeric[],
	E'{"0.11","11.18","20.44","31.11","41.86","52.90","64.50","75.24","88.16","100.08","113.88","127.60","141.90","158.13","176.97","194.72","211.91","230.04","247.80","270.20","292.80","314.96","336.26","358.72","384.84","412.08","437.64","462.84","489.06","516.88","547.36","574.47","607.20","636.57","669.28","702.30","740.88","777.49","815.40","855.00","891.48","929.25","969.54","1014.26","1058.88","1106.04","1154.78","1202.30","1251.81","1305.20","1355.86","1412.46","1469.43","1528.56","1594.50","1659.35","1728.00","1792.70","1862.70","1933.38","2005.02","2079.36","2157.40","2242.56","2326.52","2413.80","2503.90","2604.75","2707.31","2815.80","2923.56","3023.16","3143.36","3267.04","3403.08","3542.86","3676.34","3816.00","3975.42","4122.84","4289.29","4480.63","4684.35","4859.40","5062.32","5306.40","5536.80","5796.00","6095.70","6444.80","6772.48","7170.48","7599.34","8105.58","8650.46","9275.07","10054.69","11049.02","12248.28","14374.90","25989.60"}'::numeric[],
	NULL::numeric[],
	NULL::numeric[],
	NULL::numeric[]);

--
-- Table: web_sales, Attribute: ws_ext_sales_price
--
-- DELETE FROM pg_statistic WHERE starelid='cte_bug.web_sales'::regclass AND staattnum=24;
INSERT INTO pg_statistic VALUES (
	'cte_bug.web_sales'::regclass,
	24::smallint,
	False::boolean,
	0.00033333333::real,
	6::integer,
	145118.0::real,
	1::smallint,
	2::smallint,
	3::smallint,
	0::smallint,
	0::smallint,
	1752::oid,
	1754::oid,
	1754::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	'{0.0104, 0.0002, 0.0002}'::real[],
	NULL::real[],
	'{-3.1174983e-05}'::real[],
	NULL::real[],
	NULL::real[],
	E'{"0.00","42.12","43.56"}'::numeric[],
	E'{"0.15","11.55","21.09","31.38","41.40","52.47","63.68","75.30","87.36","102.00","115.28","130.23","143.98","161.58","177.08","195.03","212.64","231.77","250.56","270.60","291.64","313.31","336.44","359.92","382.11","408.00","433.65","460.80","487.06","515.16","541.38","569.88","600.62","631.67","663.75","696.96","733.08","770.99","809.00","848.70","887.04","930.69","971.28","1008.54","1054.04","1100.44","1151.97","1201.56","1252.10","1308.53","1366.40","1428.45","1484.64","1537.62","1599.40","1660.56","1730.20","1807.20","1873.92","1944.84","2025.30","2097.54","2180.70","2264.22","2351.00","2447.28","2539.20","2636.70","2733.72","2834.16","2948.78","3067.62","3174.24","3287.79","3403.35","3526.74","3663.90","3815.55","3966.90","4120.92","4294.76","4473.13","4690.56","4881.66","5088.60","5302.54","5533.92","5818.56","6094.50","6392.75","6752.16","7092.90","7519.73","7968.00","8494.71","9116.10","9877.56","10865.55","12247.37","14678.04","25048.32"}'::numeric[],
	NULL::numeric[],
	NULL::numeric[],
	NULL::numeric[]);

--
-- Table: web_sales, Attribute: ws_ext_wholesale_cost
--
-- DELETE FROM pg_statistic WHERE starelid='cte_bug.web_sales'::regclass AND staattnum=25;
INSERT INTO pg_statistic VALUES (
	'cte_bug.web_sales'::regclass,
	25::smallint,
	False::boolean,
	6.666667e-05::real,
	6::integer,
	196312.0::real,
	2::smallint,
	3::smallint,
	0::smallint,
	0::smallint,
	0::smallint,
	1754::oid,
	1754::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	NULL::real[],
	'{-0.0027050157}'::real[],
	NULL::real[],
	NULL::real[],
	NULL::real[],
	E'{"1.93","35.43","56.70","76.82","96.60","119.36","142.52","164.70","188.88","216.20","242.60","269.10","293.82","318.16","349.23","379.35","408.51","439.04","467.04","497.98","532.65","566.68","603.20","640.29","677.73","713.32","749.62","785.46","825.12","866.00","906.84","950.49","991.20","1035.40","1086.15","1131.19","1176.84","1221.96","1272.15","1321.00","1365.67","1417.72","1474.75","1528.74","1580.92","1635.13","1688.70","1744.40","1798.35","1854.56","1916.24","1981.60","2043.72","2105.60","2172.12","2249.94","2313.36","2380.16","2454.80","2530.80","2606.81","2680.56","2757.50","2832.83","2910.27","2989.11","3068.24","3147.96","3230.25","3327.77","3416.49","3506.28","3596.71","3686.50","3790.16","3884.76","3987.72","4089.55","4193.84","4304.25","4429.44","4544.01","4671.15","4795.30","4936.88","5074.40","5225.22","5382.29","5548.20","5722.02","5905.20","6111.16","6317.28","6532.92","6787.56","7078.66","7384.50","7726.50","8117.00","8641.90","9964.00"}'::numeric[],
	NULL::numeric[],
	NULL::numeric[],
	NULL::numeric[],
	NULL::numeric[]);

--
-- Table: web_sales, Attribute: ws_ext_list_price
--
-- DELETE FROM pg_statistic WHERE starelid='cte_bug.web_sales'::regclass AND staattnum=26;
INSERT INTO pg_statistic VALUES (
	'cte_bug.web_sales'::regclass,
	26::smallint,
	False::boolean,
	0.00046666668::real,
	7::integer,
	373627.0::real,
	2::smallint,
	3::smallint,
	0::smallint,
	0::smallint,
	0::smallint,
	1754::oid,
	1754::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	NULL::real[],
	'{-0.0056457715}'::real[],
	NULL::real[],
	NULL::real[],
	NULL::real[],
	E'{"3.82","64.44","101.53","139.08","181.44","220.10","260.15","300.40","347.16","399.06","442.80","490.98","543.49","592.36","645.05","702.61","756.24","813.24","866.12","923.12","986.70","1051.75","1114.12","1180.48","1241.10","1306.88","1373.56","1443.48","1522.30","1594.02","1668.20","1759.04","1834.80","1908.80","1995.12","2087.00","2170.48","2274.40","2365.65","2454.60","2553.48","2645.06","2748.90","2856.00","2958.12","3067.00","3175.68","3276.80","3382.95","3504.28","3617.46","3732.75","3852.12","3975.75","4098.27","4220.40","4355.04","4483.16","4613.12","4751.70","4888.73","5040.00","5184.62","5346.00","5487.69","5630.85","5786.64","5954.61","6113.82","6291.90","6477.02","6674.57","6870.24","7079.40","7274.74","7502.88","7699.20","7915.05","8155.70","8392.50","8646.08","8912.73","9191.07","9444.42","9740.49","10052.91","10348.48","10732.50","11116.80","11510.34","11983.48","12501.83","13062.54","13645.80","14316.72","15042.00","15921.12","16966.66","18501.25","20712.09","29685.00"}'::numeric[],
	NULL::numeric[],
	NULL::numeric[],
	NULL::numeric[],
	NULL::numeric[]);

--
-- Table: web_sales, Attribute: ws_ext_tax
--
-- DELETE FROM pg_statistic WHERE starelid='cte_bug.web_sales'::regclass AND staattnum=27;
INSERT INTO pg_statistic VALUES (
	'cte_bug.web_sales'::regclass,
	27::smallint,
	False::boolean,
	0.00033333333::real,
	6::integer,
	21995.0::real,
	1::smallint,
	2::smallint,
	3::smallint,
	0::smallint,
	0::smallint,
	1752::oid,
	1754::oid,
	1754::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	'{0.10896666, 0.00066666666, 0.0006, 0.0006, 0.00056666665, 0.00056666665, 0.00056666665, 0.00056666665, 0.00056666665, 0.00053333334, 0.00053333334, 0.00053333334, 0.00053333334, 0.00053333334, 0.00053333334, 0.0005, 0.0005, 0.0005, 0.0005, 0.0005, 0.0005, 0.0005, 0.0005, 0.00046666668, 0.00046666668, 0.00046666668, 0.00046666668, 0.00046666668, 0.00046666668, 0.00046666668, 0.00046666668, 0.00046666668, 0.00046666668, 0.00046666668, 0.00046666668, 0.00046666668, 0.00046666668, 0.00043333333, 0.00043333333, 0.00043333333, 0.00043333333, 0.00043333333, 0.00043333333, 0.00043333333, 0.00043333333, 0.00043333333, 0.00043333333, 0.00043333333, 0.00043333333, 0.00043333333, 0.00043333333, 0.00043333333, 0.00043333333, 0.00043333333, 0.0004, 0.0004, 0.0004, 0.0004, 0.0004, 0.0004, 0.0004, 0.0004, 0.0004, 0.0004, 0.0004, 0.0004, 0.0004, 0.0004, 0.0004, 0.0004, 0.0004, 0.0004, 0.0004, 0.0004, 0.0004, 0.00036666667, 0.00036666667, 0.00036666667, 0.00036666667, 0.00036666667, 0.00036666667, 0.00036666667, 0.00036666667, 0.00036666667, 0.00036666667, 0.00036666667, 0.00036666667, 0.00036666667, 0.00036666667, 0.00036666667, 0.00036666667, 0.00036666667, 0.00036666667, 0.00036666667, 0.00036666667, 0.00036666667, 0.00036666667, 0.00036666667, 0.00036666667, 0.00036666667}'::real[],
	NULL::real[],
	'{0.004573461}'::real[],
	NULL::real[],
	NULL::real[],
	E'{"0.00","0.35","0.50","2.16","0.22","0.26","0.34","1.08","1.53","0.37","0.46","0.52","0.57","0.92","2.29","0.03","0.09","0.12","0.21","0.42","0.43","0.60","0.75","0.04","0.07","0.28","0.32","0.36","0.78","0.87","1.16","1.94","2.24","2.35","3.04","3.72","5.24","0.11","0.27","0.39","0.48","0.53","0.56","0.58","0.69","0.90","1.02","1.07","1.18","1.44","1.72","2.20","2.67","4.41","0.06","0.16","0.25","0.38","0.47","0.77","0.79","0.85","1.01","1.04","1.19","1.23","1.78","1.90","1.91","1.92","2.21","2.60","2.94","4.33","5.60","0.02","0.08","0.18","0.19","0.23","0.24","0.30","0.31","0.49","0.51","0.54","0.55","0.66","0.67","0.81","0.93","1.00","1.14","1.42","1.45","1.56","1.80","1.88","2.26","2.50"}'::numeric[],
	E'{"0.01","0.83","1.34","1.75","2.23","2.69","3.10","3.49","3.97","4.43","4.88","5.35","5.90","6.43","7.00","7.69","8.38","8.95","9.70","10.49","11.20","11.89","12.64","13.48","14.34","15.18","16.14","16.99","18.05","19.01","19.99","21.08","22.13","23.33","24.48","25.75","26.99","28.33","29.92","31.37","32.94","34.59","36.09","37.92","39.69","41.47","43.32","45.30","47.20","48.97","50.94","53.17","55.40","57.80","60.19","62.89","65.72","68.28","71.23","74.17","77.49","81.04","84.54","88.21","91.73","95.71","100.03","104.60","109.56","114.83","119.85","125.13","131.38","137.60","144.21","150.45","157.35","164.05","171.91","179.70","189.75","199.15","211.02","222.05","234.00","246.12","259.74","274.59","291.64","309.69","330.12","352.06","377.91","410.73","445.30","487.66","545.67","620.87","718.11","874.66","2139.82"}'::numeric[],
	NULL::numeric[],
	NULL::numeric[],
	NULL::numeric[]);

--
-- Table: web_sales, Attribute: ws_coupon_amt
--
-- DELETE FROM pg_statistic WHERE starelid='cte_bug.web_sales'::regclass AND staattnum=28;
INSERT INTO pg_statistic VALUES (
	'cte_bug.web_sales'::regclass,
	28::smallint,
	False::boolean,
	0.00043333333::real,
	3::integer,
	6981.0::real,
	1::smallint,
	2::smallint,
	3::smallint,
	0::smallint,
	0::smallint,
	1752::oid,
	1754::oid,
	1754::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	'{0.8047}'::real[],
	NULL::real[],
	'{0.6524823}'::real[],
	NULL::real[],
	NULL::real[],
	E'{"0.00"}'::numeric[],
	E'{"0.07","2.81","5.16","8.29","11.57","14.98","18.25","22.17","25.74","29.25","33.46","38.75","43.09","47.74","52.23","58.18","63.93","69.06","75.54","81.43","90.24","97.41","105.00","112.34","119.93","128.94","139.96","150.39","161.12","170.13","179.08","190.02","204.24","216.80","230.79","242.88","256.72","269.70","285.28","299.98","321.93","337.96","359.92","379.16","398.56","411.26","434.53","456.18","480.99","503.11","534.41","559.69","587.89","615.55","642.62","675.15","706.02","738.29","769.44","805.37","834.44","867.90","910.13","947.74","994.63","1042.05","1081.01","1133.06","1191.03","1244.46","1291.32","1335.46","1389.96","1451.73","1524.48","1590.31","1679.40","1745.45","1825.78","1907.96","2007.12","2097.34","2207.59","2313.09","2431.84","2586.43","2750.05","2882.04","3053.86","3220.08","3468.52","3726.47","4048.99","4401.10","4748.00","5156.34","5788.23","6391.13","7477.72","9404.42","23044.45"}'::numeric[],
	NULL::numeric[],
	NULL::numeric[],
	NULL::numeric[]);

--
-- Table: web_sales, Attribute: ws_ext_ship_cost
--
-- DELETE FROM pg_statistic WHERE starelid='cte_bug.web_sales'::regclass AND staattnum=29;
INSERT INTO pg_statistic VALUES (
	'cte_bug.web_sales'::regclass,
	29::smallint,
	False::boolean,
	0.00026666667::real,
	6::integer,
	83043.0::real,
	1::smallint,
	2::smallint,
	3::smallint,
	0::smallint,
	0::smallint,
	1752::oid,
	1754::oid,
	1754::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	'{0.020966666, 0.00026666667, 0.00023333334, 0.00023333334, 0.00023333334, 0.00023333334, 0.00023333334, 0.00023333334, 0.00023333334, 0.0002, 0.0002, 0.0002, 0.0002, 0.0002, 0.0002, 0.0002, 0.0002, 0.0002, 0.0002, 0.0002, 0.0002, 0.0002, 0.0002, 0.0002, 0.0002, 0.0002, 0.0002, 0.0002, 0.0002, 0.0002, 0.0002}'::real[],
	NULL::real[],
	'{-0.006589469}'::real[],
	NULL::real[],
	NULL::real[],
	E'{"0.00","450.80","10.92","48.40","68.40","79.20","93.60","113.40","142.80","6.72","9.18","15.84","16.80","23.10","32.40","37.80","38.22","52.80","84.00","105.30","106.26","111.60","126.00","131.04","143.04","184.80","198.00","216.72","305.76","333.00","369.60"}'::numeric[],
	E'{"0.12","6.44","12.04","17.91","23.85","29.60","36.11","42.78","49.62","56.16","63.60","71.04","78.89","87.72","96.00","104.00","113.54","123.20","133.92","145.52","155.25","165.02","176.80","189.20","201.63","213.48","226.80","240.50","253.76","267.85","282.72","298.89","316.10","330.25","346.00","363.00","380.52","400.32","419.76","438.40","457.56","477.96","499.20","523.27","545.37","568.70","592.32","616.00","643.86","672.10","699.48","723.24","752.40","785.46","813.93","846.30","879.06","914.50","948.00","983.84","1020.80","1057.50","1097.25","1140.48","1180.20","1229.80","1277.46","1323.70","1377.74","1434.40","1494.96","1555.07","1608.18","1672.32","1736.04","1799.16","1871.10","1948.48","2020.46","2099.31","2180.40","2270.40","2362.56","2458.14","2562.12","2692.80","2810.50","2931.74","3077.75","3238.31","3432.00","3613.44","3833.20","4076.73","4350.06","4641.78","5042.73","5561.25","6238.65","7336.85","12822.00"}'::numeric[],
	NULL::numeric[],
	NULL::numeric[],
	NULL::numeric[]);

--
-- Table: web_sales, Attribute: ws_net_paid
--
-- DELETE FROM pg_statistic WHERE starelid='cte_bug.web_sales'::regclass AND staattnum=30;
INSERT INTO pg_statistic VALUES (
	'cte_bug.web_sales'::regclass,
	30::smallint,
	False::boolean,
	0.00043333333::real,
	6::integer,
	155462.0::real,
	1::smallint,
	2::smallint,
	3::smallint,
	0::smallint,
	0::smallint,
	1752::oid,
	1754::oid,
	1754::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	'{0.0125, 0.0002}'::real[],
	NULL::real[],
	'{-0.0023643773}'::real[],
	NULL::real[],
	NULL::real[],
	E'{"0.00","43.56"}'::numeric[],
	E'{"0.06","6.90","13.40","20.67","28.47","35.91","44.46","53.53","63.00","73.44","83.29","95.48","107.88","119.93","132.34","144.75","160.46","175.10","190.14","206.83","222.10","239.84","258.46","275.49","294.81","315.96","336.50","358.36","379.82","403.62","429.78","456.00","481.00","507.99","534.40","561.61","589.38","619.65","651.00","683.82","717.57","755.54","794.30","830.96","872.19","911.09","954.18","992.44","1035.44","1083.95","1133.73","1186.24","1242.02","1296.12","1352.96","1411.83","1470.70","1528.24","1597.05","1662.45","1734.44","1814.25","1880.05","1954.56","2034.12","2110.08","2198.27","2289.76","2377.50","2475.02","2583.69","2686.32","2791.36","2899.80","3029.56","3156.05","3271.45","3395.25","3526.10","3668.35","3836.05","4007.85","4182.57","4382.49","4602.96","4828.07","5062.08","5308.86","5595.68","5910.43","6219.45","6596.00","7000.70","7489.02","8014.44","8603.82","9384.00","10412.72","11672.78","14217.46","24307.65"}'::numeric[],
	NULL::numeric[],
	NULL::numeric[],
	NULL::numeric[]);

--
-- Table: web_sales, Attribute: ws_net_paid_inc_tax
--
-- DELETE FROM pg_statistic WHERE starelid='cte_bug.web_sales'::regclass AND staattnum=31;
INSERT INTO pg_statistic VALUES (
	'cte_bug.web_sales'::regclass,
	31::smallint,
	False::boolean,
	0.0003::real,
	6::integer,
	246002.0::real,
	1::smallint,
	2::smallint,
	3::smallint,
	0::smallint,
	0::smallint,
	1752::oid,
	1754::oid,
	1754::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	'{0.012466666, 0.0002}'::real[],
	NULL::real[],
	'{-0.0024806631}'::real[],
	NULL::real[],
	NULL::real[],
	E'{"0.00","57.28"}'::numeric[],
	E'{"0.06","7.24","14.07","21.63","29.77","37.41","46.45","56.03","66.38","76.71","87.19","99.41","112.52","125.57","138.96","151.61","167.50","182.92","198.36","216.24","231.78","251.32","269.52","288.70","306.76","329.56","350.90","374.93","397.02","422.18","449.02","474.60","501.75","531.09","559.18","587.44","616.28","646.64","680.44","714.49","750.33","786.11","828.41","870.48","912.34","955.22","997.24","1039.57","1083.93","1132.88","1186.06","1237.31","1299.09","1355.29","1409.04","1474.72","1532.54","1592.67","1668.97","1738.60","1815.18","1890.75","1969.95","2046.13","2120.47","2201.72","2290.88","2394.32","2487.04","2585.21","2701.06","2805.79","2915.24","3036.02","3164.89","3298.38","3422.94","3549.84","3672.00","3832.97","4016.75","4182.78","4379.44","4577.43","4811.80","5061.31","5307.67","5555.95","5845.41","6173.76","6521.98","6885.89","7318.05","7810.46","8377.41","8999.04","9828.90","10854.91","12167.98","14819.83","25915.60"}'::numeric[],
	NULL::numeric[],
	NULL::numeric[],
	NULL::numeric[]);

--
-- Table: web_sales, Attribute: ws_net_paid_inc_ship
--
-- DELETE FROM pg_statistic WHERE starelid='cte_bug.web_sales'::regclass AND staattnum=32;
INSERT INTO pg_statistic VALUES (
	'cte_bug.web_sales'::regclass,
	32::smallint,
	False::boolean,
	0.0::real,
	7::integer,
	301791.0::real,
	1::smallint,
	2::smallint,
	3::smallint,
	0::smallint,
	0::smallint,
	1752::oid,
	1754::oid,
	1754::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	'{0.0004}'::real[],
	NULL::real[],
	'{-0.0042563225}'::real[],
	NULL::real[],
	NULL::real[],
	E'{"0.00"}'::numeric[],
	E'{"0.15","26.90","47.27","66.00","87.66","110.70","130.90","153.36","177.10","200.26","226.76","251.37","278.08","304.01","330.99","360.93","391.74","422.88","454.10","486.85","518.42","554.54","589.68","627.00","662.74","700.56","740.52","781.44","822.39","863.04","903.00","950.37","1000.58","1042.83","1092.42","1141.32","1193.41","1249.05","1308.51","1363.96","1424.64","1479.06","1542.09","1608.99","1672.64","1734.00","1802.29","1875.28","1951.50","2031.12","2099.65","2168.25","2252.68","2350.87","2431.91","2518.02","2615.34","2697.47","2798.23","2897.37","3002.40","3098.34","3202.68","3308.00","3423.75","3542.37","3654.66","3775.98","3909.42","4028.66","4167.30","4309.90","4464.88","4615.84","4766.58","4937.48","5114.56","5268.48","5449.86","5643.55","5859.42","6093.48","6323.40","6589.80","6865.04","7151.04","7471.75","7798.48","8167.15","8545.18","8940.05","9403.24","9923.00","10538.58","11210.00","12020.74","12946.34","14134.36","15809.06","18879.36","36064.00"}'::numeric[],
	NULL::numeric[],
	NULL::numeric[],
	NULL::numeric[]);

--
-- Table: web_sales, Attribute: ws_net_paid_inc_ship_tax
--
-- DELETE FROM pg_statistic WHERE starelid='cte_bug.web_sales'::regclass AND staattnum=33;
INSERT INTO pg_statistic VALUES (
	'cte_bug.web_sales'::regclass,
	33::smallint,
	False::boolean,
	0.0::real,
	7::integer,
	580101.0::real,
	1::smallint,
	2::smallint,
	3::smallint,
	0::smallint,
	0::smallint,
	1752::oid,
	1754::oid,
	1754::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	'{0.0004}'::real[],
	NULL::real[],
	'{-0.0042436747}'::real[],
	NULL::real[],
	NULL::real[],
	E'{"0.00"}'::numeric[],
	E'{"0.15","27.32","48.14","67.61","90.21","112.88","134.60","157.01","181.53","205.08","232.37","257.90","285.99","310.90","338.12","369.71","401.62","432.71","466.15","498.29","532.67","568.73","606.20","644.59","677.83","716.94","756.42","802.49","841.36","885.36","929.11","973.55","1022.68","1069.26","1124.40","1170.12","1225.33","1284.57","1342.88","1398.12","1459.13","1514.68","1583.89","1648.82","1714.40","1780.42","1851.90","1924.20","2000.32","2078.12","2156.48","2230.86","2317.75","2412.28","2495.80","2582.42","2677.84","2771.12","2874.03","2977.91","3073.79","3182.59","3288.45","3403.98","3512.75","3630.83","3761.65","3884.79","4011.93","4143.91","4277.61","4436.91","4583.20","4733.60","4884.95","5069.66","5251.05","5427.69","5605.76","5797.65","6025.63","6250.68","6502.49","6751.35","7071.69","7353.10","7674.41","8024.66","8431.89","8815.18","9204.22","9715.49","10211.19","10847.31","11539.04","12347.17","13354.05","14622.68","16369.05","19567.99","38203.82"}'::numeric[],
	NULL::numeric[],
	NULL::numeric[],
	NULL::numeric[]);

--
-- Table: web_sales, Attribute: ws_net_profit
--
-- DELETE FROM pg_statistic WHERE starelid='cte_bug.web_sales'::regclass AND staattnum=34;
INSERT INTO pg_statistic VALUES (
	'cte_bug.web_sales'::regclass,
	34::smallint,
	False::boolean,
	0.0::real,
	6::integer,
	206397.0::real,
	1::smallint,
	2::smallint,
	3::smallint,
	0::smallint,
	0::smallint,
	1752::oid,
	1754::oid,
	1754::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	'{0.00033333333}'::real[],
	NULL::real[],
	'{-0.009154078}'::real[],
	NULL::real[],
	NULL::real[],
	E'{"0.00"}'::numeric[],
	E'{"-9828.00","-6282.88","-5353.36","-4759.04","-4327.14","-3980.40","-3689.38","-3415.01","-3200.70","-2979.08","-2776.62","-2608.96","-2449.44","-2301.84","-2164.10","-2041.05","-1931.67","-1824.00","-1713.27","-1605.45","-1519.20","-1432.56","-1351.42","-1271.98","-1193.64","-1123.08","-1053.59","-994.98","-939.75","-874.80","-825.28","-773.60","-726.24","-683.34","-636.68","-597.28","-559.37","-523.79","-486.00","-449.68","-416.64","-383.24","-353.70","-325.01","-296.56","-271.44","-246.81","-222.48","-199.76","-177.30","-156.21","-138.71","-119.47","-102.79","-85.56","-72.32","-58.00","-46.02","-34.44","-22.62","-11.88","-1.54","7.80","20.24","33.81","49.02","66.06","87.48","111.15","138.51","166.66","197.05","235.56","273.56","316.31","364.32","415.57","472.36","537.60","604.77","677.15","764.42","854.28","951.99","1064.31","1176.00","1306.32","1449.60","1614.67","1788.28","1984.00","2213.82","2467.72","2782.77","3119.84","3508.34","4027.92","4724.10","5672.52","7345.81","15055.16"}'::numeric[],
	NULL::numeric[],
	NULL::numeric[],
	NULL::numeric[]);


-- 
-- Query text
-- 

 set search_path=cte_bug,public;
 set gp_cte_sharing = on;

 explain(costs off)
 with year_total as (
  select c_customer_id customer_id
        ,c_first_name customer_first_name
        ,c_last_name customer_last_name
        ,c_preferred_cust_flag customer_preferred_cust_flag
        ,c_birth_country customer_birth_country
        ,c_login customer_login
        ,c_email_address customer_email_address
        ,d_year dyear
        ,sum(((ss_ext_list_price-ss_ext_wholesale_cost-ss_ext_discount_amt)+ss_ext_sales_price)/2)
 year_total
        ,'s' sale_type
  from cte_bug.customer
      ,cte_bug.store_sales
      ,cte_bug.date_dim
  where c_customer_sk = ss_customer_sk
    and ss_sold_date_sk = d_date_sk
  group by c_customer_id
          ,c_first_name
          ,c_last_name
          ,c_preferred_cust_flag
          ,c_birth_country
          ,c_login
          ,c_email_address
          ,d_year
  union all
  select c_customer_id customer_id
        ,c_first_name customer_first_name
        ,c_last_name customer_last_name
        ,c_preferred_cust_flag customer_preferred_cust_flag
        ,c_birth_country customer_birth_country
        ,c_login customer_login
        ,c_email_address customer_email_address
        ,d_year dyear
        ,sum((((cs_ext_list_price-cs_ext_wholesale_cost-cs_ext_discount_amt)+cs_ext_sales_price)/2)
 ) year_total
        ,'c' sale_type
  from cte_bug.customer
      ,cte_bug.catalog_sales
      ,cte_bug.date_dim
  where c_customer_sk = cs_bill_customer_sk
    and cs_sold_date_sk = d_date_sk
  group by c_customer_id
          ,c_first_name
          ,c_last_name
          ,c_preferred_cust_flag
          ,c_birth_country
          ,c_login
          ,c_email_address
          ,d_year
 union all
  select c_customer_id customer_id
        ,c_first_name customer_first_name
        ,c_last_name customer_last_name
        ,c_preferred_cust_flag customer_preferred_cust_flag
        ,c_birth_country customer_birth_country
        ,c_login customer_login
        ,c_email_address customer_email_address
        ,d_year dyear
        ,sum((((ws_ext_list_price-ws_ext_wholesale_cost-ws_ext_discount_amt)+ws_ext_sales_price)/2)
 ) year_total
        ,'w' sale_type
  from cte_bug.customer
      ,cte_bug.web_sales
      ,cte_bug.date_dim
  where c_customer_sk = ws_bill_customer_sk
    and ws_sold_date_sk = d_date_sk
  group by c_customer_id
          ,c_first_name
          ,c_last_name
          ,c_preferred_cust_flag
          ,c_birth_country
          ,c_login
          ,c_email_address
          ,d_year
          )
   select  
                   t_s_secyear.customer_id
                  ,t_s_secyear.customer_first_name
                  ,t_s_secyear.customer_last_name
                  ,t_s_secyear.customer_login
  from year_total t_s_firstyear
      ,year_total t_s_secyear
      ,year_total t_c_firstyear
      ,year_total t_c_secyear
      ,year_total t_w_firstyear
      ,year_total t_w_secyear
  where t_s_secyear.customer_id = t_s_firstyear.customer_id
    and t_s_firstyear.customer_id = t_c_secyear.customer_id
    and t_s_firstyear.customer_id = t_c_firstyear.customer_id
    and t_s_firstyear.customer_id = t_w_firstyear.customer_id
    and t_s_firstyear.customer_id = t_w_secyear.customer_id
    and t_s_firstyear.sale_type = 's'
    and t_c_firstyear.sale_type = 'c'
    and t_w_firstyear.sale_type = 'w'
    and t_s_secyear.sale_type = 's'
    and t_c_secyear.sale_type = 'c'
    and t_w_secyear.sale_type = 'w'
    and t_s_firstyear.dyear =  2001
    and t_s_secyear.dyear = 2001+1
    and t_c_firstyear.dyear =  2001
    and t_c_secyear.dyear =  2001+1
    and t_w_firstyear.dyear = 2001
    and t_w_secyear.dyear = 2001+1
    and t_s_firstyear.year_total > 0
    and t_c_firstyear.year_total > 0
    and t_w_firstyear.year_total > 0
    and case when t_c_firstyear.year_total > 0 then
 t_c_secyear.year_total / t_c_firstyear.year_total else null end > case
 when t_s_firstyear.year_total > 0 then t_s_secyear.year_total /
 t_s_firstyear.year_total else null end
    and case when t_c_firstyear.year_total > 0 then
 t_c_secyear.year_total / t_c_firstyear.year_total else null end > case
 when t_w_firstyear.year_total > 0 then t_w_secyear.year_total /
 t_w_firstyear.year_total else null end
  order by t_s_secyear.customer_id
          ,t_s_secyear.customer_first_name
          ,t_s_secyear.customer_last_name
          ,t_s_secyear.customer_login
 limit 100;
