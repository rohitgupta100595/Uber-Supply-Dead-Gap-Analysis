DROP SCHEMA IF EXISTS uber;
CREATE SCHEMA uber;

USE uber;

DROP TABLE IF EXISTS uber;
CREATE TABLE uber(
request_id INT,
pickup_point VARCHAR(10),
driver_id VARCHAR(30),
status VARCHAR(30),
request_timestamp VARCHAR(20),
drop_timestamp VARCHAR(20),
time_slot VARCHAR(30),
day_of_week VARCHAR(5),
request_status VARCHAR(40),
trip_duration INT
);

LOAD DATA INFILE 
'C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\Uber Request Data - Cleaned.csv'
INTO TABLE uber
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS
SET 
request_timestamp = NULLIF(request_timestamp, 'NA'),
drop_timestamp = NULLIF(drop_timestamp, 'NA');

-- Converting 'request_timestamp' and 'drop_timestamp' to datetime
UPDATE uber SET request_timestamp = STR_TO_DATE(CONCAT(request_timestamp,':00'),'%d-%m-%Y %H:%i:%s');
ALTER TABLE uber MODIFY request_timestamp DATETIME;

UPDATE uber SET drop_timestamp = STR_TO_DATE(CONCAT(drop_timestamp,':00'),'%d-%m-%Y %H:%i:%s');
ALTER TABLE uber MODIFY drop_timestamp DATETIME;

-- Insights
-- Number of users--
SELECT COUNT(request_id) AS Users FROM uber;

-- Trip Status Count --
SELECT status,COUNT(request_id) AS Count
FROM uber
GROUP BY status;

-- Number of Drivers --
SELECT COUNT(DISTINCT driver_id) AS number_of_drivers
FROM uber
WHERE status != "No Cars Available";

-- Number of users with unfulfilled requests --
SELECT COUNT(request_id) AS Users FROM uber WHERE drop_timestamp IS NULL;

-- Percentage of users with unfulfilled requests
WITH CTE1 AS (
SELECT COUNT(request_id) AS Total_users
FROM uber),
CTE2 AS (
SELECT COUNT(request_id) AS Users_unfulfilled
FROM uber WHERE drop_timestamp IS NULL)
SELECT Total_users,Users_unfulfilled, CONCAT(ROUND((CTE2.Users_unfulfilled/CTE1.Total_users)*100,2),'%') AS percentage_users_unfulfilled
FROM CTE1,CTE2;

-- Number of requests unfulfilled by pickup point --
WITH CTE1 AS (SELECT pickup_point,COUNT(request_id) AS number_of_requests
FROM uber
GROUP BY pickup_point),
CTE2 AS (
SELECT pickup_point,COUNT(request_id) AS number_of_unfulfilled_requests 
FROM uber
WHERE drop_timestamp IS NULL
GROUP BY pickup_point)
SELECT CTE1.pickup_point,CTE1.number_of_requests,CTE2.number_of_unfulfilled_requests,
CONCAT(ROUND((CTE2.number_of_unfulfilled_requests/CTE1.number_of_requests)*100,2),'%') AS percentage_users_unfulfilled
FROM CTE1
LEFT JOIN CTE2 ON CTE1.pickup_point = CTE2.pickup_point;

SELECT * FROM uber;
-- Number of unfulfilled request percentage in different time slots for Airport --
WITH CTE1 AS(
SELECT time_slot,COUNT(request_id) AS number_of_request
FROM uber
WHERE pickup_point = "Airport"
GROUP BY time_slot
),
CTE2 AS(
SELECT time_slot,COUNT(request_id) AS number_of_unfulfilled_requests
FROM uber
WHERE pickup_point = "Airport" AND drop_timestamp IS NULL
GROUP BY time_slot
)
SELECT CTE1.time_slot,CTE1.number_of_request,CTE2.number_of_unfulfilled_requests,
CONCAT(ROUND((CTE2.number_of_unfulfilled_requests/CTE1.number_of_request)*100,2),'%') AS percentage_users_unfulfilled
FROM CTE1
LEFT JOIN CTE2 ON CTE1.time_slot = CTE2.time_slot;

-- Number of unfulfilled request percentage in different time slots for City --
WITH CTE1 AS(
SELECT time_slot,COUNT(request_id) AS number_of_request
FROM uber
WHERE pickup_point = "City"
GROUP BY time_slot
),
CTE2 AS(
SELECT time_slot,COUNT(request_id) AS number_of_unfulfilled_requests
FROM uber
WHERE pickup_point = "City" AND drop_timestamp IS NULL
GROUP BY time_slot
)
SELECT CTE1.time_slot,CTE1.number_of_request,CTE2.number_of_unfulfilled_requests,
CONCAT(ROUND((CTE2.number_of_unfulfilled_requests/CTE1.number_of_request)*100,2),'%') AS percentage_users_unfulfilled
FROM CTE1
LEFT JOIN CTE2 ON CTE1.time_slot = CTE2.time_slot;

-- Number of unfulfilled requests per day of the week for Airport -- 
WITH CTE1 AS(
SELECT pickup_point,day_of_week,COUNT(request_id) AS number_of_request
FROM uber
WHERE pickup_point = "Airport"
GROUP BY pickup_point,day_of_week
),
CTE2 AS(
SELECT pickup_point,day_of_week,COUNT(request_id) AS number_of_unfulfilled_request
FROM uber
WHERE pickup_point = "Airport" AND request_status = "Request Unfulfilled"
GROUP BY pickup_point,day_of_week
)
SELECT CTE1.day_of_week,CTE1.number_of_request,CTE2.number_of_unfulfilled_request,
CONCAT(ROUND((CTE2.number_of_unfulfilled_request/CTE1.number_of_request)*100,2),'%') AS percentage_users_unfulfilled
FROM CTE1
LEFT JOIN CTE2 ON CTE1.pickup_point = CTE2.pickup_point AND CTE1.day_of_week = CTE2.day_of_week;

-- Number of unfulfilled requests per day of the week for City -- 
WITH CTE1 AS(
SELECT pickup_point,day_of_week,COUNT(request_id) AS number_of_request
FROM uber
WHERE pickup_point = "City"
GROUP BY pickup_point,day_of_week
),
CTE2 AS(
SELECT pickup_point,day_of_week,COUNT(request_id) AS number_of_unfulfilled_request
FROM uber
WHERE pickup_point = "City" AND request_status = "Request Unfulfilled"
GROUP BY pickup_point,day_of_week
)
SELECT CTE1.day_of_week,CTE1.number_of_request,CTE2.number_of_unfulfilled_request,
CONCAT(ROUND((CTE2.number_of_unfulfilled_request/CTE1.number_of_request)*100,2),'%') AS percentage_users_unfulfilled
FROM CTE1
LEFT JOIN CTE2 ON CTE1.pickup_point = CTE2.pickup_point AND CTE1.day_of_week = CTE2.day_of_week;


