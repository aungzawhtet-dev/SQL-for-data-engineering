/*
Goal: Learn to combine data across tables (key DE skill).
Topics
•	INNER JOIN, LEFT JOIN, RIGHT JOIN, FULL OUTER JOIN
•	Join conditions and aliasing
•	Self joins (e.g., manager–employee relationships)
•	Query: customer names with total orders; orders without assigned shippers.
Mini-project: Create a reporting view combining 3–4 tables.
 */

-- Practice Scenario: Online Store Database
-- work with four tables:
-- customers — customer info
-- orders — customer orders
-- shippers — shipping companies
-- employees — staff handling orders


-- Customers table
CREATE TABLE customers_v2 (
    customer_id SERIAL PRIMARY KEY,
    customer_name VARCHAR(100),
    country VARCHAR(50)
);


-- Orders table
CREATE TABLE orders_v2 (
    order_id SERIAL PRIMARY KEY,
    customer_id INT,
    employee_id INT,
    shipper_id INT,
    order_date DATE,
    total_amount DECIMAL(10,2)
);


-- Employees table
CREATE TABLE employees_v2 (
    employee_id SERIAL PRIMARY KEY,
    employee_name VARCHAR(100),
    manager_id INT
);


-- Shippers table
CREATE TABLE shippers (
    shipper_id SERIAL PRIMARY KEY,
    shipper_name VARCHAR(100)
);



-- Insert Sample Data
INSERT INTO customers_v2 (customer_name, country) VALUES
('Alice', 'Thailand'),
('Bob', 'Myanmar'),
('Charlie', 'Singapore'),
('Daisy', 'Thailand');


-- Employees
INSERT INTO employees_v2 (employee_name, manager_id) VALUES
('John', NULL),     -- Manager
('Sara', 1),        -- Reports to John
('Mike', 1);        -- Reports to John


-- Shippers
INSERT INTO shippers (shipper_name) VALUES
('DHL'),
('FedEx'),
('Thailand Post');


-- Orders
INSERT INTO orders_v2 (customer_id, employee_id, shipper_id, order_date, total_amount) VALUES
(1, 2, 1, '2025-10-01', 250.00),
(2, 3, 2, '2025-10-02', 400.00),
(3, 2, NULL, '2025-10-03', 150.00), -- missing shipper
(4, 2, 3, '2025-10-05', 600.00);


-- checking tables
select * from shippers;
select * from orders_v2;
select * from employees_v2;
select * from customers_v2;


-- Practice Queries
-- INNER JOIN
-- Show all orders with customer and shipper details.
select  
o.order_id,
o.total_amount,
s.shipper_name,
c.customer_name
from orders_v2 o
inner join customers_v2 c on o.customer_id = c.customer_id
inner join shippers s on o.shipper_id = s.shipper_id;


-- LEFT JOIN
-- Show all orders, even if they don’t have a shipper assigned.
select  
o.order_id,
o.total_amount,
s.shipper_name,
c.customer_name
from orders_v2 o
left join customers_v2 c on o.customer_id = c.customer_id
left join shippers s on o.shipper_id = s.shipper_id;


-- RIGHT JOIN
-- Show all shippers, even if they have no orders yet.
select  
o.order_id,
o.total_amount,
s.shipper_name,
c.customer_name
from orders_v2 o
left join customers_v2 c on o.customer_id = c.customer_id
Right join shippers s on o.shipper_id = s.shipper_id;


-- FULL OUTER JOIN (Full Outer Join is not support in MySQL)
-- Combine customers and employees (hypothetically if we want to see all names).

SELECT
    c.*,
    e.*
FROM
    customers_v2 c
LEFT JOIN
    employees_v2 e ON c.customer_id = e.employee_id --  Cust_Table return 4 people

UNION ALL

SELECT
    c.*,
    e.*
FROM
    customers_v2 c
RIGHT JOIN
    employees_v2 e ON c.customer_id = e.employee_id; -- Emp table return 3 people
    
    
-- SELF JOIN
-- Show each employee with their manager’s name.
SELECT 
    e.employee_name AS employee,
    m.employee_name AS manager
FROM employees_v2 e
LEFT JOIN employees_v2 m ON e.manager_id = m.employee_id;


-- Aggregations with Joins
-- Show customer names with their total order amount.

select c.customer_name,
sum(o.total_amount) as total_order_amount
from customers_v2 c
join orders_v2 o on c.customer_id = o.customer_id
group by c.customer_name
order by total_order_amount desc;


-- Show orders without assigned shippers.
select c.customer_name,
o.total_amount,
o.order_id,
s.shipper_id
from orders_v2 o
left join shippers s on o.shipper_id = s.shipper_id
join customers_v2 c on o.customer_id  = c.customer_id
where s.shipper_id is Null;


-- Mini Project: Reporting View
-- Create a combined reporting view of orders, customers, employees, and shippers.

CREATE OR REPLACE VIEW order_report AS
SELECT 
    o.order_id,
    c.customer_name,
    e.employee_name AS handled_by,
    s.shipper_name,
    o.order_date,
    o.total_amount
FROM orders_v2 o
LEFT JOIN customers_v2 c ON o.customer_id = c.customer_id
LEFT JOIN employees_v2 e ON o.employee_id = e.employee_id
LEFT JOIN shippers s ON o.shipper_id = s.shipper_id;

-- calling order_report view table
select * from order_report;




