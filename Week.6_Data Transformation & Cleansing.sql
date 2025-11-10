/*
Scenario: “Messy Customer Data”
raw data from multiple systems in a table called stg_customers.
It contains inconsistent names, missing countries, and messy date formats.
transform it into a clean dim_customers table ready for analytics.
*/


-- Step 1: Create and Insert Messy Staging Data
CREATE TABLE stg_customers (
    customer_id SERIAL PRIMARY KEY,
    full_name VARCHAR(100),
    email VARCHAR(100),
    phone VARCHAR(20),
    country_code VARCHAR(10),
    signup_date VARCHAR(20)
);


INSERT INTO stg_customers (full_name, email, phone, country_code, signup_date)
VALUES
(' Mr. John Doe ', 'JOHN.DOE@GMAIL.COM ', ' 0987654321 ', 'us', '2024/12/01'),
('jane smith', ' jane.smith@gmail.com', ' 0912345678 ', 'USA', '12-31-2023'),
('DAVID TAN', 'DAVID@HOTMAIL.COM', 'null', 'SGP', '2023-07-25'),
('mary ', 'mary@yahoo.com', NULL, '   ', '2023-13-01'),  -- invalid month
('Aung Zaw Htet', 'aungzaw@gmail.com', '099999999', 'mmr', '2023-05-01'),
('Mr. John Doe', 'john.doe@gmail.com', '0987654321', 'us', '2024/12/01'); -- duplicate



-- check stg_customers
select * from stg_customers;


-- check below query result
  SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY email ORDER BY signup_date DESC) AS rn
    FROM stg_customers;



-- Step 2: Transform and Clean the Data Procedures
/*
Trim extra spaces
Standardize names (Title Case)
Lowercase emails
Clean phone numbers
Normalize country codes (us → USA, sgp → SGP, mmr → MMR)
Fix or remove invalid dates
Remove duplicates (keep latest signup_date)
*/




-- Step 1: REGEXP_REPLACE(full_name, 'Mr\\.\\s*', '') ->> Removes the "Mr." prefix and any spaces after it.
-- Step 2: TRIM(...) ->> Removes leading/trailing spaces.
-- Step 3: REPLACE(..., ' ', '","') and CONCAT('["', ..., '"]') ->> Converts the cleaned name into a fake JSON array of words.
-- Step 4: JSON_TABLE(...) AS jt ->> Expands each word in the JSON array into rows
-- Step 5: CONCAT(UCASE(LEFT(word, 1)), LCASE(SUBSTRING(word, 2))) ->> Converts each word into Title Case manually
-- Step 6: GROUP_CONCAT(... SEPARATOR ' ') ->> Combines the rows back into one string.

/*
Final Result for Example
If full_name = ' Mr. JOHN DOE ':
Remove “Mr.” → ' JOHN DOE '
Trim → 'JOHN DOE'
Convert to JSON → '["JOHN","DOE"]'
Expand → rows JOHN and DOE
Capitalize → John, Doe
Concatenate → John Doe
 */



WITH cleaned AS (
    SELECT
        customer_id,
        -- Clean and format customer_name: Remove 'Mr. ' and apply INITCAP logic - which capitalizes the first letter of each word and lowercases the rest.       
        (
            SELECT GROUP_CONCAT(  
                CONCAT(UCASE(LEFT(word, 1)), LCASE(SUBSTRING(word, 2)))
                SEPARATOR ' '
            )
            FROM JSON_TABLE( 
                -- *** FIX: CONCAT('["', ... , '"]') added to create valid JSON array *** -- Result → '["JOHN","DOE"]'
                CONCAT('["', REPLACE(TRIM(REGEXP_REPLACE(full_name, 'Mr\\.\\s*', '')), ' ', '","'), '"]'),
                '$[*]' COLUMNS (word VARCHAR(255) PATH '$')
            ) AS jt
        ) AS customer_name,
        
        -- Normalize email and phone
        LOWER(TRIM(email)) AS email,
        NULLIF(TRIM(phone), 'null') AS phone,

        -- Normalize country codes
        CASE 
            WHEN LOWER(TRIM(country_code)) IN ('us', 'usa') THEN 'USA'
            WHEN LOWER(TRIM(country_code)) IN ('sg', 'sgp') THEN 'SGP'
            WHEN LOWER(TRIM(country_code)) IN ('mm', 'mmr') THEN 'MMR'
            ELSE 'UNKNOWN'
        END AS country_code,

        -- Convert to DATE safely
        -- COALESCE(...) ->> Returns the first non-null result from the list.If all formats fail, returns NULL.
        COALESCE(
            STR_TO_DATE(signup_date, '%Y-%m-%d'), -- YYYY-MM-DD
            STR_TO_DATE(signup_date, '%Y/%m/%d'), -- YYYY/MM/DD
            STR_TO_DATE(signup_date, '%m-%d-%Y')  -- MM-DD-YYYY
        ) AS signup_date
    FROM stg_customers
),

-- Remove duplicates: keep most recent signup date
deduped AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY email ORDER BY signup_date DESC) AS rn
    FROM cleaned
)

SELECT 
    customer_id,
    customer_name,
    email,
    phone,
    country_code,
    signup_date
FROM deduped
WHERE rn = 1
  AND signup_date IS NOT NULL;




-- Step 3: Create the Clean Dimension Table

CREATE TABLE dim_customers AS
WITH cleaned AS (
    SELECT
        customer_id,
        CONCAT(
            UPPER(LEFT(TRIM(REPLACE(full_name, 'Mr. ', '')), 1)),
            LOWER(SUBSTRING(TRIM(REPLACE(full_name, 'Mr. ', '')), 2))
        ) AS customer_name,
        LOWER(TRIM(email)) AS email,
        NULLIF(TRIM(phone), 'null') AS phone,
        CASE 
            WHEN LOWER(TRIM(country_code)) IN ('us', 'usa') THEN 'USA'
            WHEN LOWER(TRIM(country_code)) IN ('sg', 'sgp') THEN 'SGP'
            WHEN LOWER(TRIM(country_code)) IN ('mm', 'mmr') THEN 'MMR'
            ELSE 'UNKNOWN'
        END AS country_code,
        signup_date
    FROM stg_customers
),
-- this apprach is more safe & robust for Production ETL pipelines
validated AS (
    SELECT *,
        CASE
            WHEN signup_date REGEXP '^\\d{4}-((0[1-9])|(1[0-2]))-((0[1-9])|([12][0-9])|(3[01]))$'
                THEN STR_TO_DATE(signup_date, '%Y-%m-%d')
            WHEN signup_date REGEXP '^\\d{4}/((0[1-9])|(1[0-2]))/((0[1-9])|([12][0-9])|(3[01]))$'
                THEN STR_TO_DATE(signup_date, '%Y/%m/%d')
            WHEN signup_date REGEXP '^((0[1-9])|(1[0-2]))-((0[1-9])|([12][0-9])|(3[01]))-\\d{4}$'
                THEN STR_TO_DATE(signup_date, '%m-%d-%Y')
            ELSE NULL
        END AS parsed_date
    FROM cleaned
),
deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY email ORDER BY parsed_date DESC) AS rn
    FROM validated
)
SELECT 
    customer_id,
    customer_name,
    email,
    phone,
    country_code,
    parsed_date AS signup_date
FROM deduped
WHERE rn = 1 AND parsed_date IS NOT NULL;



-- Step 4: Verify Result
SELECT * FROM dim_customers;
