/* 
Week 5: Window Functions (Analytical SQL)
Goal: Master ranking, running totals, and time-based analysis.
Topics
•	OVER(), PARTITION BY, ORDER BY
•	Ranking: ROW_NUMBER(), RANK(), DENSE_RANK()
•	Cumulative totals, moving averages
Practice
•	Query: rank products by monthly revenue growth.
Mini-project: Build time-series analytics (sales by month + rank by region).
*/

use world;


-- create sales table
CREATE TABLE sales_v2 (
    order_id SERIAL PRIMARY KEY,
    product_name VARCHAR(50),
    region VARCHAR(50),
    order_date DATE,
    revenue DECIMAL(10, 2)
);


INSERT INTO sales_v2 (order_id, product_name, region, order_date, revenue)
VALUES
(1, 'Laptop', 'North', '2025-01-10', 1000.00),
(2, 'Laptop', 'North', '2025-02-10', 1200.00),
(3, 'Laptop', 'South', '2025-02-12', 1100.00),
(4, 'Phone',  'North', '2025-02-15', 800.00),
(5, 'Laptop', 'North', '2025-03-05', 1500.00),
(6, 'Phone',  'South', '2025-03-20', 700.00),
(7, 'Phone', 'North', '2025-03-25', 1200.00);


-- check sales table
select * from sales_v2;


-- 1.Basic Window Function: view total revenue with each row
-- Calculates total revenue across the whole table (no GROUP BY needed).
SELECT 
    s.*,
    t.total_revenue_all
FROM 
    sales_v2 s
JOIN (
    SELECT SUM(revenue) AS total_revenue_all FROM sales_v2
) t ON 1=1; -- is a trick to join every row with the single-row result from the subquery.


-- 2. Ranking Products by Revenue (ROW_NUMBER, RANK, DENSE_RANK)
-- Shows how ranking changes depending on ties.
SELECT 
    product_name,
    region,
    SUM(revenue) AS total_revenue,
    ROW_NUMBER() OVER(ORDER BY SUM(revenue) DESC) AS row_num,
    RANK() OVER(ORDER BY SUM(revenue) DESC) AS rank_num,
    DENSE_RANK() OVER(ORDER BY SUM(revenue) DESC) AS dense_rank_num
FROM sales_v2
GROUP BY product_name, region;


-- 3. Cumulative (Running) Total per Region
-- Shows how sales accumulate over time within each region.

SELECT 
    region,
    order_date,
    SUM(revenue) AS daily_revenue,
    SUM(SUM(revenue)) OVER(
        PARTITION BY region 
        ORDER BY order_date
    ) AS running_total -- this code is used to find running total value 
FROM sales_v2
GROUP BY region, order_date
ORDER BY region, order_date;


-- practice running total again with sales table
-- Shows how sales accumulate cnt over time within each region.
select 
region,
sale_date,
sum(quantity) as daily_sales,
sum(sum(quantity)) over (partition by region order by sale_date ) as running_total_cnt
from sales 
group by region,sale_date 
order by region,sale_date;


-- 4. Moving Average (3-Month Rolling)
-- Smooths out month-to-month fluctuations.
-- You need to first aggregate the monthly revenue, then apply the window function in an outer query.

WITH monthly_sales AS (
    SELECT 
        product_name,
        DATE_FORMAT(order_date, '%Y-%m-01') AS month,
        SUM(revenue) AS monthly_revenue
    FROM sales_v2
    GROUP BY product_name, DATE_FORMAT(order_date, '%Y-%m-01')
)
SELECT 
    product_name,
    month,
    monthly_revenue,
    AVG(monthly_revenue) OVER (
        PARTITION BY product_name 
        ORDER BY month
        ROWS BETWEEN 2 PRECEDING AND CURRENT row -- Defines the window frame:Includes the current month + the two previous months
    ) AS moving_avg_3m -- this code used to calculate the moving average
FROM monthly_sales
ORDER BY product_name, month;


-- practice with sales table again
with monthly_sales_cnt as (
select
product,
DATE_FORMAT(sale_date, '%Y-%m-01') AS month,
sum(quantity) as monthly_sale_vol
from sales
group by product,DATE_FORMAT(sale_date, '%Y-%m-01') 

)
select
product,
month,
monthly_sale_vol,
avg(monthly_sale_vol) over(
partition by product order by month
ROWS BETWEEN 2 PRECEDING AND CURRENT row
) as moving_avg_3months
from monthly_sales_cnt
order by product,month;




-- Mini Project: Time-Series Analytics
-- Goal:Calculate monthly sales per region and rank regions by performance.
-- Shows which region was top-performing each month.

WITH monthly_sales AS (
    SELECT
        region,
        DATE_FORMAT(order_date, '%Y-%m') AS month,
        SUM(revenue) AS total_revenue
    FROM sales_v2
    GROUP BY region, DATE_FORMAT(order_date, '%Y-%m')
)
SELECT
    month,
    region,
    total_revenue,
    RANK() OVER (PARTITION BY month ORDER BY total_revenue DESC) AS region_rank
FROM monthly_sales
ORDER BY month, region_rank;


-- Optional Challenge
-- Find month-over-month revenue growth for each product.
-- Calculates revenue growth (%) month by month.

WITH monthly AS (
    SELECT
        product_name,
        DATE_FORMAT(order_date, '%Y-%m') AS month,
        SUM(revenue) AS total_revenue
    FROM sales_v2
    GROUP BY product_name, DATE_FORMAT(order_date, '%Y-%m')
)
SELECT
    product_name,
    month,
    total_revenue,
    LAG(total_revenue) OVER (PARTITION BY product_name ORDER BY month) AS prev_month,
    ROUND(
        (total_revenue - LAG(total_revenue) OVER (PARTITION BY product_name ORDER BY month)) 
        / NULLIF(LAG(total_revenue) OVER (PARTITION BY product_name ORDER BY month), 0) * 100,
        2
    ) AS growth_percent
FROM monthly
ORDER BY product_name, month;

/*
total_revenue - LAG(...) -> Difference between current and previous month revenue.
NULLIF(LAG(...), 0) -> Prevents division by zero. If previous revenue is 0, returns NULL.
* 100 -> Converts the ratio into a percentage.
ROUND(..., 2) -> Rounds the result to 2 decimal places.
*/





