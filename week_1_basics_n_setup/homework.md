## QUESTION 1
- `--iidfile string`

## QUESTION 2
- 3

## QUESTION 3 - 20530
SELECT 
	COUNT(*)
FROM 
	green_taxi_data t,
	zones zpu,
	zones zdo
WHERE
	t."PULocationID" = zpu."LocationID" AND
	t."DOLocationID" = zdo."LocationID" AND
	DATE_TRUNC('DAY', lpep_pickup_datetime) = '2019-01-15 00:00:00' AND
	DATE_TRUNC('DAY', lpep_dropoff_datetime) = '2019-01-15 00:00:00'
	
## QUESTION 4 - 2019-01-10 : 78572.47
SELECT 
	SUM(trip_distance),	
	CAST(lpep_pickup_datetime AS DATE)
FROM 
	green_taxi_data t,
	zones zpu,
	zones zdo
WHERE
	t."PULocationID" = zpu."LocationID" AND
	t."DOLocationID" = zdo."LocationID"
GROUP BY
	CAST(lpep_pickup_datetime AS DATE)
HAVING 
	CAST(lpep_pickup_datetime AS DATE) = '2019-01-18' OR
	CAST(lpep_pickup_datetime AS DATE) = '2019-01-28' OR
	CAST(lpep_pickup_datetime AS DATE) = '2019-01-15' OR
	CAST(lpep_pickup_datetime AS DATE) = '2019-01-10'
ORDER BY
	SUM(trip_distance) DESC
	
## QUESTION 5 - 2: 1282 ; 3: 254 
SELECT 
	COUNT(*)
FROM 
	green_taxi_data t,
	zones zpu,
	zones zdo
WHERE
	t."PULocationID" = zpu."LocationID" AND
	t."DOLocationID" = zdo."LocationID" AND 
	t.passenger_count = 3 AND
	CAST(lpep_pickup_datetime AS DATE) = '2019-01-01'
	
## QUESTION 6 - Long Island City/Queens Plaza
SELECT
	tip_amount,
	CONCAT(zpu."Borough", ' / ', zpu."Zone") AS "pickup_loc",
	CONCAT(zdo."Borough", ' / ', zdo."Zone") AS "dropoff_loc"
FROM 
	green_taxi_data t,
	zones zpu,
	zones zdo
WHERE
	t."PULocationID" = zpu."LocationID" AND
	t."DOLocationID" = zdo."LocationID" AND
	zpu."Zone" = 'Astoria'
ORDER BY
	tip_amount DESC
LIMIT 1