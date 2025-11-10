/*
Project - Clean & Aggregate Raw Sales Data using Subqueries and CTEs
Goal :
Use subqueries and Common Table Expressions (CTEs) to:
Clean raw sales data
Aggregate sales per customer and region
Find Top 3 customers by revenue per region
*/

use world;

-- Create tables
CREATE TABLE customers (
    customer_id INT PRIMARY KEY,
    customer_name VARCHAR(100),
    region VARCHAR(50)
);


CREATE TABLE orders (
    order_id INT PRIMARY KEY,
    customer_id INT,
    order_date DATE,
    total_amount DECIMAL(10,2),
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);


-- Insert sample data
INSERT INTO customers VALUES
(1, 'Alice', 'North'),
(2, 'Bob', 'South'),
(3, 'Charlie', 'North'),
(4, 'Diana', 'East'),
(5, 'Edward', 'South'),
(6, 'Fiona', 'East');


INSERT INTO orders VALUES
(101, 1, '2025-09-01', 500.00),
(102, 2, '2025-09-03', 250.00),
(103, 3, '2025-09-05', 800.00),
(104, 1, '2025-09-10', 300.00),
(105, 5, '2025-09-11', 1000.00),
(106, 4, '2025-09-12', 700.00),
(107, 6, '2025-09-14', 200.00),
(108, 3, '2025-09-15', 100.00),
(109, 2, '2025-09-17', 400.00),
(110, 5, '2025-09-20', 300.00);


-- Check tables
select * from customers;
select * from orders;


-- 1.Subquery in SELECT
-- Find each customer’s total orders and compare to average order amount
select c.customer_name,
sum(o.total_amount) as cus_total_spent,
(select avg(total_amount) from orders) as avg_order_amount
from customers c
join orders o on c.customer_id = o.customer_id 
Group by c.customer_name
Order by cus_total_spent desc;


-- 2️ Subquery in FROM
-- Get top-spending customers overall:
select customer_name,total_spent
from (
select c.customer_name,
sum(total_amount) as total_spent
from customers c
join orders o on c.customer_id =o.customer_id
group by c.customer_name
) as total
order by total_spent desc
limit 5;


-- 3.Subquery in WHERE
-- Find customers who spent above average:

select distinct c.customer_name,
o.total_amount
from customers c
join orders o on c.customer_id = o.customer_id
where o.total_amount > (select avg(total_amount) from orders) ;


-- Common Table Expressions (CTEs)
-- Simple CTE : Calculate total revenue per region
with region_sales as(  
SELECT 
c.region,
SUM(o.total_amount) AS total_revenue
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id
GROUP BY c.region
)
select * from region_sales;


-- Multi-Step CTE (Data Cleaning + Aggregation)
-- Goal: Find top 3 customers by revenue per region.
/*
cleaned_orders: filters and joins raw data.
customer_revenue: aggregates sales per customer per region.
ranked_customers: uses a window function to rank customers by revenue.
Final SELECT: returns top 3 per region.
*/


with cleaned_orders as (
select 
o.order_id,
o.customer_id,
o.total_amount,
c.customer_name,
c.region
from customers c
join orders o on c.customer_id = o.customer_id
where o.total_amount is not null
),
customer_revengue as (
select customer_name,
region,
sum(total_amount) as total_revenue
from  cleaned_orders 
group by region,customer_name
),
ranked_customer as (
select region,
total_revenue,
customer_name,
rank () over(partition by region order by total_revenue desc) as rnk 
from customer_revengue
)
select region, customer_name, total_revenue,rnk
from ranked_customer
where rnk <= 3
order by region, total_revenue desc;




-- Mini Challenge
-- a CTE pipeline that:
-- Removes null or zero sales rows
-- Aggregates total monthly revenue
-- Finds which region had the highest sales per month


with clean_orders as (
select o.order_id,
o.customer_id,
o.order_date,
o.total_amount,
c.region
from orders o
join customers c on o.customer_id = c.customer_id 
where o.total_amount is not null and o.total_amount > 0
),
monthly_revenue as (
select 
region,
sum(total_amount) as total_revenue,
DATE_FORMAT(order_date,'%Y-%m') as order_month
from clean_orders
group by region,DATE_FORMAT(order_date,'%Y-%m')
),
region_ranked as (
select 
region,
order_month,
total_revenue,
rank ()over (partition by order_month order by total_revenue desc ) as rnk
from monthly_revenue
)
select 
region,
order_month,
total_revenue,
rnk
from region_ranked
-- where rnk =1
where rnk between 1 and 3 -- checking top 3
order by order_month;


