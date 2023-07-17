/*
 In this Part 1, we will cover Basic of Window function and use functions like:
  -- Aggregation functions
  -- Row_Number
  -- Rank_Number
  -- Dense_Rank
  -- Lag
  -- Lead

 we are using `WORKFORCE_DATA_ANALYTICS` free dataset.
 You can get this in the Snowflake Marketplace
 */

USE DATABASE WORKFORCE_DATA_ANALYTICS;
USE SCHEMA PUBLIC;

SELECT * FROM REVELIO_LAYOFFS LIMIT 1000;

// Let's check first MAX layoffs group by company (We can use other aggregated functions like Sum, Min, Avg, Count as well)
SELECT
    COMPANY,
    MAX(NUM_EMPLOYEES) AS MAX_EMP
FROM REVELIO_LAYOFFS
GROUP BY COMPANY
HAVING MAX_EMP IS NOT NULL
ORDER BY MAX_EMP DESC;

-- How we can get this by using Window function `Max`
SELECT
    *,
    MAX(NUM_EMPLOYEES) OVER () AS MAX_LAYOFF
FROM REVELIO_LAYOFFS;

-- Let's look at data partition by Company and finding Maximum layoffs.
SELECT
    *,
    MAX(NUM_EMPLOYEES) OVER (PARTITION BY COMPANY) AS MAX_LAYOFF
FROM REVELIO_LAYOFFS;


// Row_number //
-- Row_Number assigns a number to every row respective to every partition.
-- Using `Row_number` (Also a Window function), let's give a row number to every company's partition (in simple language `group`)
SELECT *,
       ROW_NUMBER() OVER (PARTITION BY COMPANY ORDER BY LAYOFF_DATE) AS RN -- Ordering by Layoff_Date
FROM REVELIO_LAYOFFS;

SELECT *,
       ROW_NUMBER() OVER (PARTITION BY COMPANY ORDER BY NUM_EMPLOYEES DESC ) AS RN -- Ordering by Num_Employees descending
FROM REVELIO_LAYOFFS;

-- Fetch the top 3 number of layoffs from every company
SELECT * FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY COMPANY ORDER BY NUM_EMPLOYEES DESC ) AS RN
    FROM REVELIO_LAYOFFS
              ) X
WHERE X.RN < 4;


// Rank Number //
-- Rank Number assigns a rank to thr row of every partition
-- We can get same top 3 numbers of layoffs by company too
SELECT * FROM
             (SELECT
                  *,
                  RANK() OVER (PARTITION BY COMPANY ORDER BY NUM_EMPLOYEES DESC ) AS RANK_
              FROM REVELIO_LAYOFFS) x
WHERE x.RANK_< 4;


// Dense_Rank //
-- Dense_Rank also works like Rank_number but there is one difference mentioned below.

SELECT * FROM (
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY COMPANY ORDER BY NUM_EMPLOYEES DESC ) AS DRANK_
    FROM REVELIO_LAYOFFS
              )x
WHERE x.DRANK_ < 4;

/*
 Difference between RANK and DENSE_RANK
*  RANK: a list of results could use the RANK function and show values of 1, 2, 2, 4, and 5.
 The number 3 is skipped because the rank of 2 has two same values.
** DENSE_RANK: a list of results could use the DENSE_RANK function and show values of 1, 2, 2, 3, and 4.
 The number 3 is still used, even if rank of 2 has two same values.
 */


 // Lag //
-- We use a Lag() function to access previous rows data as per defined offset value.
-- Here we finding the layoff difference and layoff rate order layoff_date
-- And in next example, difference of weeks between previous layoff and the next layoff

SELECT
    *,
    LAG(NUM_EMPLOYEES) OVER (PARTITION BY COMPANY ORDER BY LAYOFF_DATE) AS LAYOFF_PREVIOUS,
    NUM_EMPLOYEES - (LAG(NUM_EMPLOYEES) OVER (PARTITION BY COMPANY ORDER BY LAYOFF_DATE)) AS LAYOFF_DIFF,
    (DIV0(LAYOFF_DIFF,REVELIO_LAYOFFS.NUM_EMPLOYEES)*100) AS LAYOFF_RATE
FROM REVELIO_LAYOFFS;

SELECT
    *,
    LAG(LAYOFF_DATE) OVER (PARTITION BY COMPANY ORDER BY LAYOFF_DATE) AS DATE_PREVIOUS,
    DATEDIFF(MONTH, LAG(LAYOFF_DATE) OVER (PARTITION BY COMPANY ORDER BY LAYOFF_DATE), LAYOFF_DATE) AS DATE_DIFF
FROM REVELIO_LAYOFFS;

/*
 There  are number of arguments we can pass in the lag function like
 LAG(NUM_EMPLOYEES, 2, 0) OVER (PARTITION BY COMPANY ORDER BY LAYOFF_DATE) AS LAYOFF_PREVIOUS
 Now Lag() will look and shows second previous value
 */


// Lead //
-- Lead just opposite of Lag function. It shows next row value.

SELECT *,
       LEAD(NUM_EMPLOYEES) OVER (PARTITION BY COMPANY ORDER BY LAYOFF_DATE) AS LAYOFF_NEXT
FROM REVELIO_LAYOFFS