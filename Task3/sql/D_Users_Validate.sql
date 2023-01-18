DELETE FROM DWH_STAGING.Users

WHERE id IS NULL
  OR fname is NULL
  OR lname is NULL
  OR NOT REGEXP_CONTAINS(email, "[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+.[A-Za-z]{2,6}")
  OR country is NULL
  OR (subscription != "1" AND subscription != "0")
