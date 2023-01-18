MERGE `DWH.D_Users` D_U
USING `DWH_STAGING.Users` U
ON U.id = D_U.id
WHEN MATCHED  AND effective_end = DATETIME(2100, 12, 25, 05, 30, 00) THEN
  UPDATE SET effective_end = current_datetime()
WHEN NOT MATCHED THEN
  INSERT (key, id, fname, lname, email, country, is_active, effective_start, effective_end)
  VALUES(1, id, fname, lname, email, country,TRUE, current_datetime(), DATETIME(2100, 12, 25, 05, 30, 00))