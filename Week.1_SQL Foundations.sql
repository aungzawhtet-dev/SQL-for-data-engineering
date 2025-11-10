/*
Week 1: SQL Foundations Goal: Build solid basics for querying structured data.
 Topics 
 • CREATE TABLE, data types 
 • Basic SELECT, WHERE, ORDER BY, LIMIT 
 • Filtering with operators: =, <, >, BETWEEN, IN, LIKE • NULL handling (IS NULL, COALESCE)
 •	Write queries like: top 5 highest prices, list all customers in Bangkok.
 */



use world;

-- Create tables
CREATE TABLE customers (
    customer_id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    city VARCHAR(50),
    email VARCHAR(100)
);


CREATE TABLE products (
    product_id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    category VARCHAR(50),
    price DECIMAL(10, 2)
);


CREATE TABLE orders (
    order_id SERIAL PRIMARY KEY,
    customer_id INT REFERENCES customers(customer_id),
    product_id INT REFERENCES products(product_id),
    quantity INT,
    order_date DATE
);



-- Insert Sample Data

-- Customers
INSERT INTO customers (name, city, email) VALUES
('Aung Zaw Htet', 'Bangkok', 'aung@example.com'),
('Nyi Nyi', 'Chiang Mai', 'nyi@example.com'),
('Soe Win', 'Bangkok', 'soe@example.com'),
('May Thandar', 'Phuket', 'may@example.com'),
('Mya Mya', 'Yangon', 'mya@example.com');

-- Products
INSERT INTO products (name, category, price) VALUES
('Laptop', 'Electronics', 850.00),
('Mouse', 'Electronics', 25.50),
('Coffee Mug', 'Kitchen', 9.99),
('Desk Chair', 'Furniture', 120.00),
('Notebook', 'Stationery', 3.75);

-- Orders
INSERT INTO orders (customer_id, product_id, quantity, order_date) VALUES
(1, 1, 1, '2025-10-01'),
(1, 2, 2, '2025-10-02'),
(2, 3, 5, '2025-10-05'),
(3, 1, 1, '2025-10-07'),
(4, 4, 1, '2025-10-08'),
(5, 5, 10, '2025-10-09');


-- View or Checking the data in the tables
select * from orders;
select * from customers;
select * from products;


-- Top five most expensive products
select name , price from products
order by price desc
limit 5;

-- Customers living in Bangkok
select name from customers 
where  city = "Bangkok";


-- Order made after Oct3
select * from orders
where order_date > "2025-10-03";


-- Products priced between 10 and 200
select * from products p 
where price between 10 and 200;


-- Customers NOT in Bangkok
select * from customers c 
where city <> "Bangkok";


-- Join tables (who ordered what)
SELECT 
    c.name AS customer_name,
    p.name AS product_name,
    o.quantity,
    o.order_date
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
JOIN products p ON o.product_id = p.product_id;


-- Handle NULLs (using Coalesce)
SELECT 
    name, 
    COALESCE(email, 'No Email Provided') AS email
FROM customers;


/*
Mini-Project Task
Goal: Design a small schema for mini “Customer–Orders–Products” system
Add a discount column to products
Add a total_amount field in orders (quantity × price) using a calculated query
Create a query that shows total spent by each customer
*/

SELECT 
    c.name,
    SUM(p.price * o.quantity) AS total_spent
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
JOIN products p ON o.product_id = p.product_id
GROUP BY c.name
ORDER BY total_spent DESC;

