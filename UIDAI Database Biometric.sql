COPY biometric_data 
FROM 'D:\data hackathon\api_data_aadhar_biometric\api_data_aadhar_biometric_0_500000.csv'
DELIMITER ','
CSV HEADER
ESCAPE '\';

COPY biometric_data 
FROM 'D:\data hackathon\api_data_aadhar_biometric\api_data_aadhar_biometric_500000_1000000.csv'
DELIMITER ','
CSV HEADER
ESCAPE '\';

COPY biometric_data 
FROM 'D:\data hackathon\api_data_aadhar_biometric\api_data_aadhar_biometric_1000000_1500000.csv'
DELIMITER ','
CSV HEADER
ESCAPE '\';

COPY biometric_data 
FROM 'D:\data hackathon\api_data_aadhar_biometric\api_data_aadhar_biometric_1500000_1861108.csv'
DELIMITER ','
CSV HEADER
ESCAPE '\';



-- CREATING TABLE
create table api_data_aadhar_biometric_0_500000 (
	enroll_date date,
	states varchar(100),
	districts varchar(100),
	pincode int,
	age_5_17 int,
	age_17_plus int
);



-- RENAMING THE TABLE NAME TO "enrollment_data"
ALTER TABLE api_data_aadhar_biometric_0_500000 RENAME TO biometric_data;



-- SELECTING MERGED TABLE OF ALL 3 FILE INTO ONE 
SELECT * FROM biometric_data;



-- DISTINCT COLUMNS FROM TABLE
SELECT DISTINCT(states) FROM biometric_data;



-- RENAMING ALL MISSMATACHED COLUMNS
UPDATE biometric_data
SET states = CASE
    WHEN states = 'Jammu and Kashmir' THEN 'Jammu & Kashmir'
    WHEN states = 'Dadra and Nagar Haveli' THEN 'Dadra & Nagar Haveli'
    WHEN states = 'Puducherry' THEN 'Pondicherry'
    WHEN states = 'Daman and Diu' THEN 'Daman & Diu'
	WHEN states = 'The Dadra And Nagar Haveli And Daman And Diu' THEN 'Dadra And Nagar Haveli And Daman And Diu'
    WHEN states = 'Andaman and Nicobar Islands' THEN 'Andaman & Nicobar Islands'
	WHEN states = 'andhra pradesh' THEN 'Andhra Pradesh'
	WHEN states = 'Chhattisgarh' THEN 'Chhatisgarh'
	WHEN states = 'Tamilnadu' THEN 'Tamil Nadu'
	WHEN states = 'Uttaranchal' THEN 'Uttarakhand'
	WHEN states = 'odisha' THEN 'Odisha'
	WHEN states = 'Orissa' THEN 'Odisha'
	WHEN states = 'ODISHA' THEN 'Odisha'
	WHEN states = 'West  Bengal' THEN 'West Bengal'
	WHEN states = 'West Bangal' THEN 'West Bengal'
	WHEN states = 'west Bengal' THEN 'West Bengal'
	WHEN states = 'West bengal' THEN 'West Bengal'
	WHEN states = 'WEST BENGAL' THEN 'West Bengal'
	WHEN states = 'Westbengal' THEN 'West Bengal'
	WHEN states = 'WESTBENGAL' THEN 'West Bengal'
	ELSE states
END;



-- 1. TOTAL BIOMETRIC ENROLLMENTS (SYSTEM SCALE)
-- Calculates total biometric enrollments across all age groups
-- Used to understand overall system size

SELECT
    SUM(age_5_17 + age_17_plus) AS total_biometric_enrollments
FROM biometric_data;



-- 2. AGE-WISE BIOMETRIC DISTRIBUTION
-- Breaks down biometric enrollments by age group
-- Helps assess demographic coverage balance

SELECT
    SUM(age_5_17)   AS biometric_age_5_17,
    SUM(age_17_plus) AS biometric_age_17_plus
FROM biometric_data;



-- 3. AGE BALANCE RATIO (KEY GOVERNANCE INDICATOR)
-- Calculates percentage share of each age group
-- Indicates inclusivity and demographic balance

SELECT
    ROUND(
        SUM(age_5_17)::DECIMAL /
        NULLIF(SUM(age_5_17 + age_17_plus), 0) * 100,
        2
    ) AS age_5_17_percent,
    ROUND(
        SUM(age_17_plus)::DECIMAL /
        NULLIF(SUM(age_5_17 + age_17_plus), 0) * 100,
        2
    ) AS age_17_plus_percent
FROM biometric_data;



-- 4. DAILY BIOMETRIC ENROLLMENT TREND
-- Tracks biometric enrollments over time
-- Helps identify surge periods and temporal patterns

SELECT
    enroll_date,
    SUM(age_5_17 + age_17_plus) AS daily_biometric_load
FROM biometric_data
GROUP BY enroll_date
ORDER BY enroll_date;



-- 5. MONTH-WISE BIOMETRIC ENROLLMENTS
-- Aggregates biometric enrollments by month
-- Useful for monthly capacity planning

SELECT
    DATE_TRUNC('month', enroll_date) AS month,
    SUM(age_5_17 + age_17_plus) AS monthly_biometric_enrollments
FROM biometric_data
GROUP BY month
ORDER BY month;



-- 6. STATE-WISE BIOMETRIC LOAD (GEOGRAPHICAL PRESSURE)
-- Identifies states with highest biometric enrollment pressure
-- Supports regional infrastructure prioritization

SELECT
    states,
    SUM(age_5_17 + age_17_plus) AS state_biometric_load
FROM biometric_data
GROUP BY states
ORDER BY state_biometric_load DESC;



-- 7. DISTRICT-LEVEL BIOMETRIC HOTSPOTS
-- Detects districts with high biometric concentration
-- Helps locate operational pressure points

SELECT
    states,
    districts,
    SUM(age_5_17 + age_17_plus) AS district_biometric_load
FROM biometric_data
GROUP BY states, districts
ORDER BY district_biometric_load DESC;



-- 8. BIOMETRIC OPERATIONAL LOAD INDEX (WEIGHTED)
-- Calculates weighted operational load for biometric processing
-- Adult biometrics assumed slightly more resource intensive

SELECT
    states,
    ROUND(
        SUM(age_5_17 * 1.0 + age_17_plus * 1.1),
        2
    ) AS biometric_operational_load_index
FROM biometric_data
GROUP BY states
ORDER BY biometric_operational_load_index DESC;



-- 9. BIOMETRIC ENROLLMENT VOLATILITY (STABILITY METRIC)
-- Measures fluctuation in biometric enrollments
-- High volatility indicates operational instability

SELECT
    states,
    ROUND(
        STDDEV(age_5_17 + age_17_plus),
        2
    ) AS biometric_volatility
FROM biometric_data
GROUP BY states
ORDER BY biometric_volatility DESC;



-- 10. ANOMALY DETECTION – SUDDEN BIOMETRIC SURGES
-- Detects unusually high biometric enrollment days
-- Useful for alerts, audits, and investigation

SELECT
    enroll_date,
    states,
    districts,
    (age_5_17 + age_17_plus) AS biometric_total
FROM biometric_data
WHERE (age_5_17 + age_17_plus) >
      (
        SELECT AVG(age_5_17 + age_17_plus) * 2
        FROM biometric_data
      )
ORDER BY biometric_total DESC;



-- 11. QUARTER-WISE BIOMETRIC LOAD
-- Aggregates biometric enrollments by quarter
-- Helps identify seasonal operational surges

SELECT
    EXTRACT(QUARTER FROM enroll_date) AS quarter,
    SUM(age_5_17 + age_17_plus) AS quarterly_biometric_load
FROM biometric_data
GROUP BY quarter
ORDER BY quarter;



-- 12. UNDERPERFORMING DISTRICTS (COVERAGE GAPS)
-- Identifies districts performing below their state average
-- Highlights regions needing outreach or access improvement

WITH state_avg AS (
    SELECT
        states,
        AVG(age_5_17 + age_17_plus) AS avg_biometric_load
    FROM biometric_data
    GROUP BY states
)
SELECT
    b.states,
    b.districts,
    SUM(b.age_5_17 + b.age_17_plus) AS district_biometric_load
FROM biometric_data b
JOIN state_avg s
  ON b.states = s.states
GROUP BY b.states, b.districts, s.avg_biometric_load
HAVING SUM(b.age_5_17 + b.age_17_plus) < s.avg_biometric_load
ORDER BY district_biometric_load;



-- 13. LONG-TAIL DISTRICTS (BOTTOM 10%)
-- Identifies districts in bottom 10% of biometric enrollments
-- Important for equity-focused planning

WITH district_totals AS (
    SELECT
        districts,
        SUM(age_5_17 + age_17_plus) AS total_biometric
    FROM biometric_data
    GROUP BY districts
)
SELECT *
FROM district_totals
WHERE total_biometric <
      (
        SELECT PERCENTILE_CONT(0.10)
        WITHIN GROUP (ORDER BY total_biometric)
        FROM district_totals
      )
ORDER BY total_biometric;



-- 14. BIOMETRIC ENROLLMENT MOMENTUM SCORE
-- Calculates day-on-day growth velocity
-- Acts as a predictive signal for capacity scaling

SELECT
    enroll_date,
    ROUND(
        (SUM(age_5_17 + age_17_plus) -
         LAG(SUM(age_5_17 + age_17_plus))
         OVER (ORDER BY enroll_date))
        /
        NULLIF(
            LAG(SUM(age_5_17 + age_17_plus))
            OVER (ORDER BY enroll_date),
            0
        ),
        2
    ) AS biometric_momentum_score
FROM biometric_data
GROUP BY enroll_date
ORDER BY enroll_date;



-- 15. COMPOSITE BIOMETRIC RISK ZONE CLASSIFICATION
-- Classifies states into LOW / MEDIUM / HIGH biometric risk zones
-- Combines average load and volatility for decision-making

WITH metrics AS (
    SELECT
        states,
        AVG(age_5_17 + age_17_plus) AS avg_load,
        STDDEV(age_5_17 + age_17_plus) AS volatility
    FROM biometric_data
    GROUP BY states
)
SELECT
    states,
    avg_load,
    volatility,
    CASE
        WHEN avg_load > (SELECT AVG(avg_load) FROM metrics)
         AND volatility > (SELECT AVG(volatility) FROM metrics)
        THEN 'HIGH RISK'
        WHEN avg_load > (SELECT AVG(avg_load) FROM metrics)
        THEN 'MEDIUM RISK'
        ELSE 'LOW RISK'
    END AS biometric_risk_zone
FROM metrics
ORDER BY biometric_risk_zone;

