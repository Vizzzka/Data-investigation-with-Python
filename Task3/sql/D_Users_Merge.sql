MERGE DWH.D_Users D_U
USING DWH_STAGING.Users U
ON D_U.id = U.id AND D_U.key = U.key
WHEN MATCHED THEN
  UPDATE SET D_U.effective_end = U.effective_end
WHEN NOT MATCHED THEN
  INSERT(id, fname, lname, email, country, is_active, effective_start, effective_end)
  VALUES(U.id, U.fname, U.lname, U.email, U.country, True, DATETIME(TIMESTAMP_SECONDS(U.timestamp)), U.effective_end)