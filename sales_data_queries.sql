--How many stores does the business have and in which countries?
SELECT
	country_code AS country,
	COUNT(store_code) AS total_no_stores
FROM
	dim_store_details
GROUP BY
	country
ORDER BY
	total_no_stores DESC;


--Which locations currently have the most stores?
SELECT
	locality,
	COUNT(store_code) AS total_no_stores
FROM
	dim_store_details
GROUP BY
	locality
HAVING
	COUNT(store_code) >= 10
ORDER BY
	total_no_stores DESC;


--Which months produced the largest amount of sales?
SELECT
	SUM(product_price * product_quantity) AS total_sales,
    "month"
FROM
    orders_table AS orders
JOIN
    dim_products AS prods ON prods.product_code = orders.product_code
JOIN
    dim_date_times AS dt ON dt.date_uuid = orders.date_uuid
GROUP BY
    "month"
ORDER BY
    total_sales DESC;


--How many sales are coming from online?
SELECT
    COUNT(*) AS number_of_sales,
    SUM(orders.product_quantity) AS product_quantity_count,
    CASE
        WHEN stores.store_type = 'Web Portal'
        THEN 'Web'
        ELSE 'Offline'
        END AS "location"
FROM
    orders_table AS orders
JOIN
    dim_store_details AS stores
    ON stores.store_code = orders.store_code
GROUP BY
    CASE
        WHEN stores.store_type = 'Web Portal'
        THEN 'Web'
        ELSE 'Offline'
        END;


--What percentage of sales come through each type of store?
SELECT
    stores.store_type AS store_type,
    SUM(prods.product_price * orders.product_quantity) AS total_sales,
    ROUND((COUNT(*) / CAST((SELECT COUNT(*) FROM orders_table) AS NUMERIC)) * 100, 2) AS "percentage_total(%)"
FROM
    orders_table AS orders
JOIN
    dim_store_details AS stores
    ON stores.store_code = orders.store_code
JOIN
    dim_products AS prods
    ON prods.product_code = orders.product_code
GROUP BY
    stores.store_type
ORDER BY
    total_sales DESC;


--Which month in each year produced the highest cost of sales?
SELECT
    SUM(prods.product_price * orders.product_quantity) AS total_sales,
    dt."year",
    dt."month"
FROM
    orders_table AS orders
JOIN
    dim_products AS prods
    ON prods.product_code = orders.product_code
JOIN
    dim_date_times AS dt
    ON dt.date_uuid = orders.date_uuid
GROUP BY
    dt."year",
    dt."month"
ORDER BY
    total_sales DESC LIMIT 10;


--What is the staff headcount?
SELECT
    SUM(stores.staff_numbers) AS total_staff_numbers,
    stores.country_code
FROM
    dim_store_details AS stores
GROUP BY
    stores.country_code
ORDER BY
    total_staff_numbers DESC;


--Which German store type is selling the most?
SELECT
    SUM(prods.product_price * orders.product_quantity) AS total_sales,
    stores.store_type AS store_type,
    stores.country_code AS country_code
FROM
    orders_table AS orders
JOIN
    dim_store_details AS stores
    ON stores.store_code = orders.store_code
JOIN
    dim_products AS prods
    ON prods.product_code = orders.product_code
WHERE
    stores.country_code = 'DE'
GROUP BY
    stores.store_type,
    stores.country_code
ORDER BY
    total_sales ASC;


--How quickly is the company making sales?
WITH cte AS (
    SELECT
        dt."year" AS "year",
        CAST(dt."year" || '-' || dt."month" || '-' || dt."day" || ' ' || dt.timestamp AS TIMESTAMP) AS "datetime",
        LEAD(
            CAST(dt."year" || '-' || dt."month" || '-' || dt."day" || ' ' || dt.timestamp AS TIMESTAMP), 1
            )
            OVER (
                ORDER BY CAST(dt."year" || '-' || dt."month" || '-' || dt."day" || ' ' || dt.timestamp AS TIMESTAMP) ASC
                )
            - CAST(dt."year" || '-' || dt."month" || '-' || dt."day" || ' ' || dt.timestamp AS TIMESTAMP) AS interval
    FROM
        dim_date_times AS dt
    ORDER BY
        "datetime"
)
SELECT
    cte."year",
    AVG(cte.interval) AS actual_time_taken
FROM
    cte
GROUP BY
    cte."year"
ORDER BY
    actual_time_taken DESC;