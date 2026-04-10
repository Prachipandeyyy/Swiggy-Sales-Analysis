DROP TABLE IF EXISTS swiggy_data;

CREATE TABLE swiggy_data(
         id SERIAL PRIMARY KEY,
		 state  VARCHAR(100),
		 city VARCHAR(100),
		 order_date	DATE,
		 restaurant_name VARCHAR(150),	
		 location	VARCHAR(150),
		 category VARCHAR(200),
		 dish_name VARCHAR(150),
		 price_inr FLOAT,
		 rating	FLOAT ,
		 rating_count FLOAT

);

SELECT * FROM swiggy_data;

-- Data Validation and Cleaning

-- Checking for Null Values
SELECT * FROM swiggy_data
WHERE state IS NULL
   OR City IS NULL
   OR Order_Date IS NULL
   OR Restaurant_Name IS NULL
   OR Location IS NULL
   OR Category IS NULL
   OR Dish_Name IS NULL
   OR Price_INR IS NULL
   OR Rating IS NULL
   OR Rating_Count IS NULL;

-- There are no null values in this data

-- Checking for Blank or Empty strings
SELECT *
FROM swiggy_data
WHERE state='' 
   OR city=''
   OR restaurant_name=''
   OR location=''
   OR category=''
   OR dish_name='';

-- There are no empty strings

-- Checking for duplicate values
WITH cte as(
   SELECT *,
       ROW_NUMBER() OVER(PARTITION BY state,city,order_date,restaurant_name,location,category,dish_name,price_inr,rating,rating_count) AS row_number
   FROM swiggy_data) 
SELECT * FROM cte
where row_number>1;

-- There are total 27 duplicate rows

-- Deleting Duplicate rows
WITH duplicate_cte AS(
   SELECT *,
       ROW_NUMBER() OVER(PARTITION BY state,city,order_date,restaurant_name,location,category,dish_name,price_inr,rating,rating_count) AS row_number
   FROM swiggy_data) 
DELETE FROM swiggy_data
WHERE id IN (
     SELECT id
	 FROM duplicate_cte
	 WHERE row_number>1
);

-- Successfully removed the duplicates from the table

SELECT * FROM swiggy_data;


----- Creating schemas
-- Dimension tables

--Date table
CREATE TABLE dim_date(
       date_id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
	   full_date DATE,
	   year INT,
	   month INT,
	   month_name VARCHAR(50),
	   quarter INT,
	   day INT,
	   week INT
);

SELECT * FROM dim_date;

-- Location Table
CREATE TABLE dim_location(
      location_id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
	  state VARCHAR(100),
	  city VARCHAR(100),
	  location VARCHAR(200)
);

SELECT * FROM dim_location;

-- Restaurant table
CREATE TABLE dim_restaurant(
       restaurant_id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
	   restaurant_name VARCHAR(150)
);

SELECT * FROM dim_restaurant;

-- Category table
CREATE TABLE dim_category(
       category_id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
	   category_name VARCHAR(200)
);

SELECT * FROM dim_category;

-- Dish table
CREATE TABLE dim_dish(
       dish_id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
	   dish_name VARCHAR(200)
);

SELECT * FROM dim_dish ;

-- Facts table
CREATE TABLE facts_swiggy_orders(
        order_id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
		date_id INT ,
		location_id INT,
		restaurant_id INT ,
		category_id INT,
		dish_id INT,
		price_inr FLOAT,
		rating FLOAT,
		rating_count FLOAT,
        CONSTRAINT fk_date
		FOREIGN KEY (date_id) REFERENCES dim_date(date_id),
		CONSTRAINT fk_category
		FOREIGN KEY (category_id) REFERENCES dim_category(category_id),
		CONSTRAINT fk_dish 
		FOREIGN KEY (dish_id) REFERENCES dim_dish(dish_id),
		CONSTRAINT fk_location
		FOREIGN KEY (location_id) REFERENCES dim_location(location_id),
		CONSTRAINT fk_restaurant
		FOREIGN KEY (restaurant_id) REFERENCES dim_restaurant(restaurant_id)
		);

SELECT * FROM facts_swiggy_orders;		


----- Inserting data into dimension tables

-- Date table
INSERT INTO dim_date(full_date,year,month,month_name,quarter,day,week)
SELECT DISTINCT order_date,
                DATE_PART('year',order_date),
			    DATE_PART('month',order_date),
				TRIM(TO_CHAR(order_date',month')),
				DATE_PART('quarter',order_date),
				DATE_PART('day',order_date),
			    DATE_PART('week',order_date)
FROM swiggy_data
WHERE order_date IS NOT NULL;

SELECT * FROM dim_date;

-- Category table
INSERT INTO dim_category(category_name)
SELECT DISTINCT category
FROM swiggy_data
WHERE category IS NOT NULL;

SELECT * FROM dim_category;

-- Location table
INSERT INTO dim_location(state,city,location)
SELECT DISTINCT state,
                city,
				location
FROM swiggy_data;

SELECT * FROM dim_location;

-- Dish table
INSERT INTO dim_dish(dish_name)
SELECT DISTINCT dish_name
FROM swiggy_data;

SELECT * FROM dim_dish;

-- Restaurant table
INSERT INTO dim_restaurant(restaurant_name)
SELECT DISTINCT restaurant_name
FROM swiggy_data;

SELECT * FROM dim_restaurant;

-- Inserting data into facts table
INSERT INTO facts_swiggy_orders (date_id,location_id,restaurant_id,category_id,dish_id,price_inr,rating,rating_count)
SELECT dd.date_id,
       dl.location_id,
	   dr.restaurant_id,
	   dc.category_id,
	   dsh.dish_id,
	   s.price_inr,
	   s.rating,
	   s.rating_count
FROM swiggy_data s
JOIN dim_date dd
     ON dd.full_date=s.order_date
JOIN dim_location dl
     ON dl.state=s.state
     AND dl.city=s.city
     AND dl.location=s.location
JOIN dim_restaurant dr
     ON dr.restaurant_name=s.restaurant_name
JOIN dim_category dc
     ON dc.category_name=s.category
JOIN dim_dish dsh
     ON dsh.dish_name=s.dish_name;


SELECT * FROM facts_swiggy_orders;

----- All the data together
SELECT * FROM facts_swiggy_orders f
JOIN dim_date d ON f.date_id=d.date_id
JOIN dim_category c ON f.category_id=c.category_id
JOIN dim_dish ds ON f.dish_id=ds.dish_id
JOIN dim_location l ON f.location_id=l.location_id
JOIN dim_restaurant r ON f.restaurant_id=r.restaurant_id;


----- KPIs and Analysis

-- KPIs

-- Total Orders
SELECT COUNT(*) AS Total_Orders
FROM facts_swiggy_orders;

-- Total Revenue
SELECT SUM(price_inr) AS Total_Revenue
FROM facts_swiggy_orders;

-- Average Dish Price
SELECT AVG(price_inr) AS Average_dish_Price
FROM facts_swiggy_orders;

-- Average Rating
SELECT AVG(rating) AS average_ratings 
FROM  facts_swiggy_orders;


------- Business Analysis

---- Date based analysis

-- monthly order trend
SELECT d.year,
       d.month,
	   d.month_name,
	   count(*) AS total_orders
FROM facts_swiggy_orders f 
JOIN dim_date d 
ON d.date_id=f.date_id
GROUP BY d.year,
       d.month,
	   d.month_name
ORDER BY COUNT(*) DESC;

-- monthly revenue trend
SELECT d.year,
       d.month,
	   d.month_name,
	   SUM(price_inr) AS total_revenue
FROM facts_swiggy_orders f 
JOIN dim_date d 
ON d.date_id=f.date_id
GROUP BY d.year,
       d.month,
	   d.month_name
ORDER BY COUNT(*) DESC;

-- quarterly order trend
SELECT d.year,
       d.quarter,
	   COUNT(*) AS total_orders,
	   SUM(price_inr) AS total_revenue
FROM facts_swiggy_orders f 
JOIN dim_date d 
ON d.date_id=f.date_id
GROUP BY d.year,
         d.quarter
ORDER BY COUNT(*) DESC;

-- order trend by day of the week
SELECT d.year,
	   TO_CHAR(d.full_date,'Day') AS day_name,
	   COUNT(*) AS total_orders,
	   SUM(price_inr) AS total_revenue
FROM facts_swiggy_orders f 
JOIN dim_date d 
ON d.date_id=f.date_id
GROUP BY d.year,
	   TO_CHAR(d.full_date,'Day')
ORDER BY COUNT(*) DESC	   ;

-- Yearly order trend
SELECT d.year,
	   COUNT(*) AS total_orders,
	   SUM(price_inr) AS total_revenue
FROM facts_swiggy_orders f 
JOIN dim_date d 
ON d.date_id=f.date_id
GROUP BY d.year;


---- Location based analysis

-- Top cities by order count
SELECT l.city,
       COUNT(*) AS total_orders
FROM facts_swiggy_orders f
JOIN dim_location l
ON l.location_id=f.location_id
GROUP BY l.city
LIMIT 10;

-- Top cities by revenue
SELECT l.city,
       SUM(f.price_inr) AS total_revenue
FROM facts_swiggy_orders f
JOIN dim_location l
ON l.location_id=f.location_id
GROUP BY l.city
LIMIT 10;

-- revenuecontribution by states
SELECT l.state,
       SUM(f.price_inr) AS total_revenue
FROM facts_swiggy_orders f
JOIN dim_location l
ON l.location_id=f.location_id
GROUP BY l.state;

-- TOP 10 locations by order and revenue
SELECT l.location,
       COUNT(*) AS total_orders,
       SUM(f.price_inr) AS total_revenue
FROM facts_swiggy_orders f
JOIN dim_location l
ON l.location_id=f.location_id
GROUP BY l.location
ORDER BY COUNT(*) DESC,SUM(f.price_inr) DESC
LIMIT 10;

---- Restaurant based analysis

-- Top restaurants by order count
SELECT r.restaurant_name,
       COUNT(*) AS total_orders
FROM facts_swiggy_orders f
JOIN dim_restaurant r
ON r.restaurant_id=f.restaurant_id
GROUP BY r.restaurant_name
ORDER BY COUNT(*) DESC
LIMIT 10;

-- Top rated restaurants by average rating
SELECT r.restaurant_name,
       AVG(f.rating) AS avergae_rating 
FROM facts_swiggy_orders f
JOIN dim_restaurant r
ON r.restaurant_id=f.restaurant_id
GROUP BY r.restaurant_name
ORDER BY AVG(f.rating) DESC
LIMIT 10;



---- Food based analysis

-- Top food categories by order count
SELECT c.category_name,
       COUNT(*) AS total_orders 
FROM facts_swiggy_orders f
JOIN dim_category c
ON c.category_id=f.category_id
GROUP BY c.category_name
ORDER BY COUNT(*) DESC
LIMIT 10;

-- Top food categories (orders + average rating)
SELECT c.category_name,
       COUNT(*) AS total_orders,
	   AVG(f.rating) AS average_rating
FROM facts_swiggy_orders f
JOIN dim_category c
ON c.category_id=f.category_id
GROUP BY c.category_name
ORDER BY COUNT(*) DESC
LIMIT 10;

-- top 10 most ordered dishes
SELECT d.dish_name,
       COUNT(*) AS total_orders 
FROM facts_swiggy_orders f
JOIN dim_dish d
ON d.dish_id=f.dish_id
GROUP BY d.dish_name
ORDER BY COUNT(*) DESC
LIMIT 10;


---- rating based analysis

-- rating count distribution
SELECT rating,
       COUNT(*) AS rating_count 
FROM facts_swiggy_orders
GROUP BY rating
ORDER BY rating DESC;
