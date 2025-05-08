-- start_ignore
drop table if exists t1;
drop table if exists t2;
-- end_ignore

create table t1(v1 int, v2 int, v3 int);
insert into t1 values(generate_series(1, 10), generate_series(11, 20), generate_series(21, 30));
analyze t1;

create table t2(v1 int, v2 int, v3 int);
insert into t2 values(generate_series(0, 100), generate_series(100, 200), generate_series(200, 300));

-- should pruned both seq scan and shared scan
explain verbose with c1 as (select v1, v2, v3 from t1) select c11.v1 from c1 as c11 left join c1 as c22 on c11.v1=c22.v1 where c11.v1 < 5;
with c1 as (select v1, v2, v3 from t1) select c11.v1 from c1 as c11 left join c1 as c22 on c11.v1=c22.v1 where c11.v1 < 5;
explain verbose with c1 as (select v1, v2, v3 from t1) select c11.v2 from c1 as c11 left join c1 as c22 on c11.v1=c22.v1 where c11.v1 < 5;
with c1 as (select v1, v2, v3 from t1) select c11.v2 from c1 as c11 left join c1 as c22 on c11.v1=c22.v1 where c11.v1 < 5;
explain verbose with c1 as (select v1, v2, v3 from t1) select c11.v3 from c1 as c11 left join c1 as c22 on c11.v1=c22.v1 where c11.v1 < 5;
with c1 as (select v1, v2, v3 from t1) select c11.v3 from c1 as c11 left join c1 as c22 on c11.v1=c22.v1 where c11.v1 < 5;

-- * also should be pruned
explain verbose with c1 as (select * from t1) select c11.v1 from c1 as c11 left join c1 as c22 on c11.v1=c22.v1 where c11.v1 < 5;
with c1 as (select * from t1) select c11.v1 from c1 as c11 left join c1 as c22 on c11.v1=c22.v1 where c11.v1 < 5;

-- no push filter
explain verbose with c1 as (select v1, v2, v3 from t1) select c11.v3 from c1 as c11 left join c1 as c22 on c11.v1=c22.v2;
with c1 as (select v1, v2, v3 from t1) select c11.v3 from c1 as c11 left join c1 as c22 on c11.v1=c22.v2;

explain verbose with c1 as (select v1, v2, v3 from t1) select c11.v2 from c1 as c11 left join c1 as c22 on c11.v1=c22.v2;
with c1 as (select v1, v2, v3 from t1) select c11.v2 from c1 as c11 left join c1 as c22 on c11.v1=c22.v2;

-- distribution col can't pruned
explain verbose with c1 as (select v1, v2, v3 from t1) select c11.v2 from c1 as c11 left join c1 as c22 on c11.v2=c22.v2;
with c1 as (select v1, v2, v3 from t1) select c11.v2 from c1 as c11 left join c1 as c22 on c11.v2=c22.v2;
explain verbose with c1 as (select v1, v2, v3 from t1) select c11.v3 from c1 as c11 left join c1 as c22 on c11.v3=c22.v3;
with c1 as (select v1, v2, v3 from t1) select c11.v3 from c1 as c11 left join c1 as c22 on c11.v3=c22.v3;

-- groupby/order by/window function/grouping set should be contains in CTE output

-- group by 
explain verbose with c1 as (select v1, v2, v3 from t1) select sum(c11.v1) from c1 as c11 left join c1 as c22 on c11.v1=c22.v1 group by c11.v1;
with c1 as (select v1, v2, v3 from t1) select sum(c11.v1) from c1 as c11 left join c1 as c22 on c11.v1=c22.v1 group by c11.v1;

explain verbose with c1 as (select v1, v2, v3 from t1) select sum(c11.v1) from c1 as c11 left join c1 as c22 on c11.v1=c22.v1 group by c11.v2;
with c1 as (select v1, v2, v3 from t1) select sum(c11.v1) from c1 as c11 left join c1 as c22 on c11.v1=c22.v1 group by c11.v2;

explain verbose with c1 as (select v1, v2, v3 from t1) select sum(c11.v3) from c1 as c11 left join c1 as c22 on c11.v1=c22.v1 group by c11.v2;
with c1 as (select v1, v2, v3 from t1) select sum(c11.v3) from c1 as c11 left join c1 as c22 on c11.v1=c22.v1 group by c11.v2;

-- order by 
explain verbose with c1 as (select v1, v2, v3 from t1) select c11.v1 from c1 as c11 left join c1 as c22 on c11.v1=c22.v1 order by c22.v1;
with c1 as (select v1, v2, v3 from t1) select c11.v1 from c1 as c11 left join c1 as c22 on c11.v1=c22.v1 order by c22.v1;

explain verbose with c1 as (select v1, v2, v3 from t1) select c11.v1 from c1 as c11 left join c1 as c22 on c11.v1=c22.v1 order by c22.v3;
with c1 as (select v1, v2, v3 from t1) select c11.v1 from c1 as c11 left join c1 as c22 on c11.v1=c22.v1 order by c22.v3;

-- window function
explain verbose with c1 as (select v1, v2, v3 from t1) select sum(c11.v1) OVER (ORDER BY c11.v2) from c1 as c11 left join c1 as c22 on c11.v1=c22.v1;
with c1 as (select v1, v2, v3 from t1) select sum(c11.v1) OVER (ORDER BY c11.v2) from c1 as c11 left join c1 as c22 on c11.v1=c22.v1;
explain verbose with c1 as (select v1, v2, v3 from t1) select sum(c11.v2) OVER (ORDER BY c11.v3) from c1 as c11 left join c1 as c22 on c11.v1=c22.v1;
with c1 as (select v1, v2, v3 from t1) select sum(c11.v2) OVER (ORDER BY c11.v3) from c1 as c11 left join c1 as c22 on c11.v1=c22.v1;

-- grouping set 
explain verbose with c1 as (select v1, v2, v3 from t1) select sum(c11.v2) from c1 as c11 left join c1 as c22 on c11.v1=c22.v1 group by ROLLUP(c11.v1,c11.v2);
with c1 as (select v1, v2, v3 from t1) select sum(c11.v2) from c1 as c11 left join c1 as c22 on c11.v1=c22.v1 group by ROLLUP(c11.v1,c11.v2);
explain verbose with c1 as (select v1, v2, v3 from t1) select sum(c11.v2) from c1 as c11 left join c1 as c22 on c11.v1=c22.v1 group by ROLLUP(c11.v2,c11.v3);
with c1 as (select v1, v2, v3 from t1) select sum(c11.v2) OVER (ORDER BY c11.v3) from c1 as c11 left join c1 as c22 on c11.v1=c22.v1;


-- CTE producer should have right output

explain verbose with c1 as (select t1.v1 as v1, t2.v1 as t21, t2.v2 as t22, t2.v3 as t23 from t1 join t2 on t1.v1 = t2.v1) 
select c11.v1 from c1 as c11 left join c1 as c22 on c11.v1=c22.v1;

with c1 as (select t1.v1 as v1, t2.v1 as t21, t2.v2 as t22, t2.v3 as t23 from t1 join t2 on t1.v1 = t2.v1) 
select c11.v1 from c1 as c11 left join c1 as c22 on c11.v1=c22.v1;

explain verbose with c1 as (select sum(v1) as v1, sum(v2) as v2, v3 from t1 group by v3) 
select c11.v1 from c1 as c11 left join c1 as c22 on c11.v1=c22.v1;

with c1 as (select lt1.v3 as v3, lt1.v1 as lo1, rt1.v1 as ro1 from t1 lt1, t1 rt1 where lt1.v2 = rt1.v2 and lt1.v1 = rt1.v1)  
select * from t1 where  t1.v1 in (select v3 from c1) and t1.v1 in (select v3 from c1 where v3 > 0);

-- cte in cte


-- function call 


-- TPCDS cte not support reduce producter output yet

-- start_ignore
drop table if exists tpcds_store_sales;
drop table if exists tpcds_date_dim;
drop table if exists tpcds_item;
drop table if exists tpcds_web_sales;
-- end_ignore

create table tpcds_store_sales
(
    ss_sold_date_sk           integer                       ,
    ss_sold_time_sk           integer                       ,
    ss_item_sk                integer               not null,
    ss_customer_sk            integer                       ,
    ss_cdemo_sk               integer                       ,
    ss_hdemo_sk               integer                       ,
    ss_addr_sk                integer                       ,
    ss_store_sk               integer                       ,
    ss_promo_sk               integer                       ,
    ss_ticket_number          integer               not null,
    ss_quantity               integer                       ,
    ss_wholesale_cost         decimal(7,2)                  ,
    ss_list_price             decimal(7,2)                  ,
    ss_sales_price            decimal(7,2)                  ,
    ss_ext_discount_amt       decimal(7,2)                  ,
    ss_ext_sales_price        decimal(7,2)                  ,
    ss_ext_wholesale_cost     decimal(7,2)                  ,
    ss_ext_list_price         decimal(7,2)                  ,
    ss_ext_tax                decimal(7,2)                  ,
    ss_coupon_amt             decimal(7,2)                  ,
    ss_net_paid               decimal(7,2)                  ,
    ss_net_paid_inc_tax       decimal(7,2)                  ,
    ss_net_profit             decimal(7,2)                  ,
    primary key (ss_item_sk, ss_ticket_number)
);

create table tpcds_date_dim
(
    d_date_sk                 integer               not null,
    d_date_id                 char(16)              not null,
    d_date                    date                          ,
    d_month_seq               integer                       ,
    d_week_seq                integer                       ,
    d_quarter_seq             integer                       ,
    d_year                    integer                       ,
    d_dow                     integer                       ,
    d_moy                     integer                       ,
    d_dom                     integer                       ,
    d_qoy                     integer                       ,
    d_fy_year                 integer                       ,
    d_fy_quarter_seq          integer                       ,
    d_fy_week_seq             integer                       ,
    d_day_name                char(9)                       ,
    d_quarter_name            char(6)                       ,
    d_holiday                 char(1)                       ,
    d_weekend                 char(1)                       ,
    d_following_holiday       char(1)                       ,
    d_first_dom               integer                       ,
    d_last_dom                integer                       ,
    d_same_day_ly             integer                       ,
    d_same_day_lq             integer                       ,
    d_current_day             char(1)                       ,
    d_current_week            char(1)                       ,
    d_current_month           char(1)                       ,
    d_current_quarter         char(1)                       ,
    d_current_year            char(1)                       ,
    primary key (d_date_sk)
);

create table tpcds_item
(
    i_item_sk                 integer               not null,
    i_item_id                 char(16)              not null,
    i_rec_start_date          date                          ,
    i_rec_end_date            date                          ,
    i_item_desc               varchar(200)                  ,
    i_current_price           decimal(7,2)                  ,
    i_wholesale_cost          decimal(7,2)                  ,
    i_brand_id                integer                       ,
    i_brand                   char(50)                      ,
    i_class_id                integer                       ,
    i_class                   char(50)                      ,
    i_category_id             integer                       ,
    i_category                char(50)                      ,
    i_manufact_id             integer                       ,
    i_manufact                char(50)                      ,
    i_size                    char(20)                      ,
    i_formulation             char(20)                      ,
    i_color                   char(20)                      ,
    i_units                   char(10)                      ,
    i_container               char(10)                      ,
    i_manager_id              integer                       ,
    i_product_name            char(50)                      ,
    primary key (i_item_sk)
);

create table tpcds_web_sales
(
    ws_sold_date_sk           integer                       ,
    ws_sold_time_sk           integer                       ,
    ws_ship_date_sk           integer                       ,
    ws_item_sk                integer               not null,
    ws_bill_customer_sk       integer                       ,
    ws_bill_cdemo_sk          integer                       ,
    ws_bill_hdemo_sk          integer                       ,
    ws_bill_addr_sk           integer                       ,
    ws_ship_customer_sk       integer                       ,
    ws_ship_cdemo_sk          integer                       ,
    ws_ship_hdemo_sk          integer                       ,
    ws_ship_addr_sk           integer                       ,
    ws_web_page_sk            integer                       ,
    ws_web_site_sk            integer                       ,
    ws_ship_mode_sk           integer                       ,
    ws_warehouse_sk           integer                       ,
    ws_promo_sk               integer                       ,
    ws_order_number           integer               not null,
    ws_quantity               integer                       ,
    ws_wholesale_cost         decimal(7,2)                  ,
    ws_list_price             decimal(7,2)                  ,
    ws_sales_price            decimal(7,2)                  ,
    ws_ext_discount_amt       decimal(7,2)                  ,
    ws_ext_sales_price        decimal(7,2)                  ,
    ws_ext_wholesale_cost     decimal(7,2)                  ,
    ws_ext_list_price         decimal(7,2)                  ,
    ws_ext_tax                decimal(7,2)                  ,
    ws_coupon_amt             decimal(7,2)                  ,
    ws_ext_ship_cost          decimal(7,2)                  ,
    ws_net_paid               decimal(7,2)                  ,
    ws_net_paid_inc_tax       decimal(7,2)                  ,
    ws_net_paid_inc_ship      decimal(7,2)                  ,
    ws_net_paid_inc_ship_tax  decimal(7,2)                  ,
    ws_net_profit             decimal(7,2)                  ,
primary key (ws_item_sk, ws_order_number)
);

-- sql 23
explain verbose with frequent_ss_items as 
 (select substr(i_item_desc,1,30) itemdesc,i_item_sk item_sk,d_date solddate,count(*) cnt
  from tpcds_store_sales
      ,tpcds_date_dim 
      ,tpcds_item
  where ss_sold_date_sk = d_date_sk
    and ss_item_sk = i_item_sk 
    and d_year in (1999,1999+1,1999+2,1999+3)
  group by substr(i_item_desc,1,30),i_item_sk,d_date
  having count(*) >4)
select t1.v1 from t1 where t1.v1 in (select item_sk from frequent_ss_items)
    and t1.v1 in (select item_sk from frequent_ss_items where item_sk > 0);

-- sql 95
explain verbose with ws_wh as
(select ws1.ws_order_number,ws1.ws_warehouse_sk wh1,ws2.ws_warehouse_sk wh2
 from tpcds_web_sales ws1,tpcds_web_sales ws2
 where ws1.ws_order_number = ws2.ws_order_number
   and ws1.ws_warehouse_sk <> ws2.ws_warehouse_sk)
select * from t1 where t1.v1 in (select ws_order_number from ws_wh) and t1.v1 in (select ws_order_number from ws_wh where ws_order_number > 0);

explain verbose with ws_wh as
(select ws1.ws_order_number,ws1.ws_warehouse_sk wh1,ws2.ws_warehouse_sk wh2
 from tpcds_web_sales ws1,tpcds_web_sales ws2
 where ws1.ws_order_number = ws2.ws_order_number
   and ws1.ws_warehouse_sk <> ws2.ws_warehouse_sk)
select * from t1 where t1.v1 in (select wh1 from ws_wh) and t1.v1 in (select wh1 from ws_wh where ws_order_number > 0);

-- start_ignore
drop table tpcds_store_sales;
drop table tpcds_date_dim;
drop table tpcds_item;
drop table tpcds_web_sales;

drop table t1;
drop table t2;
-- end_ignore
