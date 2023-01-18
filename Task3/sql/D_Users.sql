
CREATE OR REPLACE TABLE DWH.D_Users
PARTITION BY RANGE_BUCKET ( id, GENERATE_ARRAY(1, 5000, 100)) AS (

SELECT DISTINCT
  ROW_NUMBER() OVER(PARTITION BY id) as key,
  id,
  fname,
  lname,
  email,
  country,
  TRUE is_active,
  current_datetime() as effective_start,
  DATETIME(2100, 12, 25, 05, 30, 00) as effective_end
FROM
  DWH_STAGING.Users

WHERE id IS NOT NULL
  AND fname is not NULL
  AND lname is not NULL
  AND REGEXP_CONTAINS(email, "[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+.[A-Za-z]{2,6}")
  AND country is not NULL
  AND (subscription = "1" OR subscription = "0")
)