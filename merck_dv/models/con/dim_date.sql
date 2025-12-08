{{ config(
    materialized='table',
    schema='CON',
    unique_key='date_key'
) }}

-- Generate a series of dates for the Date Dimension
WITH date_series AS (
    SELECT
        DATEADD(day, seq4(), '2020-01-01'::DATE) AS date_day
    FROM
        TABLE(GENERATOR(rowcount => (365 * 6) )) -- Generate roughly 6 years of days (2020 through 2025)
),

date_attributes AS (
    SELECT
        -- Primary Key (YYYYMMDD integer)
        TO_NUMBER(TO_CHAR(date_day, 'YYYYMMDD')) AS date_key,

        -- Date Components
        date_day,
        YEAR(date_day) AS year,
        QUARTER(date_day) AS quarter_of_year,
        MONTH(date_day) AS month_of_year,
        DAYOFMONTH(date_day) AS day_of_month,
        DAYOFWEEK(date_day) AS day_of_week_num,
        DAYOFYEAR(date_day) AS day_of_year,
        WEEKOFYEAR(date_day) AS week_of_year,

        -- Descriptive Names
        DAYNAME(date_day) AS day_name,
        MONTHNAME(date_day) AS month_name,

        -- Flags
        CASE WHEN DAYOFWEEK(date_day) IN (6, 7) THEN TRUE ELSE FALSE END AS is_weekend,
        CASE WHEN YEAR(date_day) = YEAR(CURRENT_DATE()) AND MONTH(date_day) = MONTH(CURRENT_DATE()) THEN TRUE ELSE FALSE END AS is_current_month

    FROM date_series
    WHERE date_day <= DATEADD(year, 6, '2020-01-01'::DATE) -- Limit the generation range
)

SELECT * FROM date_attributes
ORDER BY date_key