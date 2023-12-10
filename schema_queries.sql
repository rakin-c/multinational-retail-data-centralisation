--Cast columns of orders_table to correct data types.
ALTER TABLE orders_table
    ALTER COLUMN date_uuid
    TYPE UUID USING date_uuid::uuid;

ALTER TABLE orders_table
    ALTER COLUMN user_uuid
    TYPE UUID USING user_uuid::uuid;

SELECT MAX(LENGTH(CAST(card_number AS TEXT))) FROM orders_table;  --Maximum card length is 19. Column originally of type BIGINT so must cast to TEXT.
ALTER TABLE orders_table
    ALTER COLUMN card_number
    TYPE VARCHAR(19);

SELECT MAX(LENGTH(store_code)) FROM orders_table;  --Maximum store code length is 12.
ALTER TABLE orders_table
    ALTER COLUMN store_code
    TYPE VARCHAR(12);

SELECT MAX(LENGTH(product_code)) FROM orders_table;  --Maximum product code length is 11.
ALTER TABLE orders_table
    ALTER COLUMN product_code
    TYPE VARCHAR(11);

ALTER TABLE orders_table
    ALTER COLUMN product_quantity
    TYPE SMALLINT;



--Cast columns of dim_users to correct data types.
ALTER TABLE dim_users
    ALTER COLUMN first_name
    TYPE VARCHAR(255);

ALTER TABLE dim_users
    ALTER COLUMN last_name
    TYPE VARCHAR(255);

ALTER TABLE dim_users
    ALTER COLUMN date_of_birth
    TYPE DATE;

ALTER TABLE dim_users
    ALTER COLUMN country_code
    TYPE VARCHAR(2);  --Country codes are two characters long.

ALTER TABLE dim_users
    ALTER COLUMN user_uuid
    TYPE UUID USING user_uuid::uuid;

ALTER TABLE dim_users
    ALTER COLUMN join_date
    TYPE DATE;



--Update dim_store_details
ALTER TABLE dim_store_details
    ALTER COLUMN longitude
    TYPE FLOAT;

ALTER TABLE dim_store_details
    ALTER COLUMN locality
    TYPE VARCHAR(255);

SELECT MAX(LENGTH(store_code)) FROM dim_store_details;  --Max length 12.
ALTER TABLE dim_store_details
    ALTER COLUMN store_code
    TYPE VARCHAR(12);

ALTER TABLE dim_store_details
    ALTER COLUMN staff_numbers
    TYPE SMALLINT;

ALTER TABLE dim_store_details
    ALTER COLUMN opening_date
    TYPE DATE;

ALTER TABLE dim_store_details
    ALTER COLUMN store_type
    TYPE VARCHAR(255);
ALTER TABLE dim_store_details
    ALTER COLUMN store_type DROP NOT NULL;  --Nullable column.

ALTER TABLE dim_store_details
    ALTER COLUMN latitude
    TYPE FLOAT;

ALTER TABLE dim_store_details
    ALTER COLUMN country_code
    TYPE VARCHAR(2);  --Country codes are two characters long.

ALTER TABLE dim_store_details
    ALTER COLUMN continent
    TYPE VARCHAR(255);

UPDATE dim_store_details
    SET "locality" = 'N/A',
        "address" = 'N/A',
    WHERE "store_code" LIKE 'WEB%';



--Add weight_class column to dim_products.
ALTER TABLE dim_products
	ADD COLUMN weight_class VARCHAR(14);
UPDATE dim_products
	SET weight_class = 'Light'
		WHERE weight < 2;
UPDATE dim_products
	SET weight_class = 'Mid_Sized'
		WHERE weight >= 2 AND weight < 40;
UPDATE dim_products
	SET weight_class = 'Heavy'
		WHERE weight >= 40 AND weight < 140;
UPDATE dim_products
	SET weight_class = 'Truck_Required'
		WHERE weight >= 140;



--Cast columns of dim_products to correct data types.
ALTER TABLE dim_products
	ALTER COLUMN product_price
	TYPE FLOAT;

ALTER TABLE dim_products
	ALTER COLUMN weight
	TYPE FLOAT;

ALTER TABLE dim_products
	ALTER COLUMN date_added
	TYPE DATE USING date_added::date;

SELECT MAX(LENGTH(product_code)) FROM dim_products;  --Max length 11.
ALTER TABLE dim_products
	ALTER COLUMN product_code
	TYPE VARCHAR(11);

ALTER TABLE dim_products
	ALTER COLUMN uuid
	TYPE UUID USING uuid::uuid;

ALTER TABLE dim_products
	RENAME removed TO still_available;

UPDATE dim_products
	SET still_available = 'TRUE'
	WHERE still_available = 'Still_avaliable';  --Entries in this column were misspelled.
UPDATE dim_products
	SET still_available = 'FALSE'
	WHERE still_available = 'Removed';

ALTER TABLE dim_products
	ALTER COLUMN still_available
	TYPE BOOL USING still_available::boolean;

SELECT MAX(LENGTH("EAN")) FROM dim_products;  --Max length 17. Error if column name not in quotes.
ALTER TABLE dim_products
    ALTER COLUMN "EAN"
    TYPE VARCHAR(17);



--Cast columns of dim_date_times to correct data types.
ALTER TABLE dim_date_times
    ALTER COLUMN "month"
    TYPE VARCHAR(2);

ALTER TABLE dim_date_times
    ALTER COLUMN "year"
    TYPE VARCHAR(4);

ALTER TABLE dim_date_times
    ALTER COLUMN "day"
    TYPE VARCHAR(2);

SELECT MAX(LENGTH(time_period)) FROM dim_date_times;  --Max length 10.
ALTER TABLE dim_date_times
    ALTER COLUMN time_period
    TYPE VARCHAR(10);

ALTER TABLE dim_date_times
    ALTER COLUMN date_uuid
    TYPE UUID USING date_uuid::uuid;



--Cast columns of dim_card_details to correct data types.
SELECT MAX(LENGTH(CAST(card_number AS TEXT))) FROM dim_card_details;  --Max length 19. Column type originally BIGINT, must cast to TEXT.
ALTER TABLE dim_card_details
    ALTER COLUMN card_number
    TYPE VARCHAR(19);

ALTER TABLE dim_card_details
    ALTER COLUMN expiry_date
    TYPE VARCHAR(5);  --Expiry dates are five characters long.

ALTER TABLE dim_card_details
    ALTER COLUMN date_payment_confirmed
    TYPE DATE;



--Create primary keys.
ALTER TABLE dim_users
    ADD PRIMARY KEY (user_uuid);

ALTER TABLE dim_card_details
	ADD PRIMARY KEY (card_number);

ALTER TABLE dim_store_details
	ADD PRIMARY KEY (store_code);

ALTER TABLE dim_products
	ADD PRIMARY KEY (product_code);

ALTER TABLE dim_date_times
	ADD PRIMARY KEY (date_uuid);



--Create foreign keys in order_table.
ALTER TABLE orders_table
	ADD CONSTRAINT fk_users
	FOREIGN KEY (user_uuid) REFERENCES dim_users(user_uuid);

ALTER TABLE orders_table
	ADD CONSTRAINT fk_cards
    FOREIGN KEY (card_number) REFERENCES dim_card_details(card_number);

ALTER TABLE orders_table
	ADD CONSTRAINT fk_stores
    FOREIGN KEY (store_code) REFERENCES dim_store_details(store_code);

ALTER TABLE orders_table
	ADD CONSTRAINT fk_products
    FOREIGN KEY (product_code) REFERENCES dim_products(product_code);

ALTER TABLE orders_table
	ADD CONSTRAINT fk_datetimes
    FOREIGN KEY (date_uuid) REFERENCES dim_date_times(date_uuid);