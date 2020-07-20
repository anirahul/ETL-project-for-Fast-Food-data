--ETL Project Tables

--DROP TABLE fast_food_datafiniti
--DROP TABLE fast_food_yelp
--DROP TABLE population_by_zip
--DROP TABLE pop_zip_agg
--DROP TABLE fast_food_population_base
--DROP TABLE fast_food_population_base_yelp


CREATE TABLE fast_food_datafiniti (
name VARCHAR (75) NOT NULL,
address VARCHAR (100) NOT NULL,
city VARCHAR (50) NOT NULL,
country VARCHAR (10) NOT NULL,
latitude FLOAT NOT NULL,
longitude FLOAT NOT NULL,
postal_code VARCHAR (10) NOT NULL,
province VARCHAR (20) NOT NULL,
categories TEXT NOT NULL,
primary_categories VARCHAR (100) NOT NULL,
keys VARCHAR (250) NOT NULL,
sourceURL TEXT,
websites TEXT
);
--Import fast_food_datafiniti csv file (10,000 rows of data)
SELECT * FROM fast_food_datafiniti

CREATE TABLE fast_food_yelp (
alias VARCHAR (100) NOT NULL,
name VARCHAR (75) NOT NULL,
url TEXT,
latitude FLOAT NOT NULL,
longitude FLOAT NOT NULL,
address1 VARCHAR (250),
address2 VARCHAR (250),
city VARCHAR (50) NOT NULL,
zip_code VARCHAR (50) NOT NULL,
state VARCHAR (20) NOT NULL,
phone TEXT
);
--Import fast_food_yelp csv file (150 rows of data)
SELECT * FROM fast_food_yelp;

CREATE TABLE population_by_zip (
population INTEGER,
min_age INTEGER,
max_age INTEGER,
gender VARCHAR (10),
zip_code VARCHAR (5),
geo_id VARCHAR (50)
);
--Import population_by_zip csv file (1,622,831 rows of data)
SELECT * FROM population_by_zip

-- Aggregate population data by zip code. Drop gender and age info.
CREATE TABLE pop_zip_agg as (
SELECT zip_code
	, SUM(population) population
FROM PUBLIC.population_by_zip
GROUP BY zip_code
ORDER BY zip_code);
--Table after aggregation (33,119 rows of data)
SELECT * FROM pop_zip_agg

-- Clean fast_food_datafiniti to trim zip codes longer than 5 characters
UPDATE PUBLIC.fast_food_datafiniti
SET postal_code = LEFT(postal_code, 5) 
WHERE LENGTH(postal_code) > 5;

SELECT * FROM fast_food_datafiniti

--Join to fast food datafiniti dataset and create new table
--(28 don't match because the population/zip data is census based and over 9 years old)
CREATE TABLE fast_food_population_base AS (
SELECT ff.name
	, ff.address
	, ff.city
	, ff.province "state"
	, ff.postal_code
	, ff.websites "website"
	, p.population
FROM PUBLIC.fast_food_datafiniti ff
		JOIN pop_zip_agg p ON ff.postal_code = p.zip_code
ORDER BY 1,3);

--New table output (9,972 rows of data)
SELECT * FROM fast_food_population_base;

--Join to fast food yelp dataset and create new table
CREATE TABLE fast_food_population_base_yelp AS (
SELECT y.name
	, y.address1
	, y.city
	, y.state
	, y.zip_code
	, y.url "website"
	, p.population
FROM PUBLIC.fast_food_yelp y
		JOIN pop_zip_agg p ON y.zip_code = p.zip_code
ORDER BY 1,3);

--New table output (150 rows of data)
SELECT * FROM fast_food_population_base_yelp;