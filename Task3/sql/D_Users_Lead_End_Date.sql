UPDATE DWH_STAGING.Users  U
SET effective_end = DATETIME(TIMESTAMP_SECONDS(followed_by))
FROM
(SELECT key,
  id,
  timestamp,
  effective_end,
  LEAD(timestamp)
    OVER (PARTITION BY id ORDER BY timestamp ASC) AS followed_by
FROM  DWH_STAGING.Users) AS f
WHERE U.timestamp = f.timestamp AND U.id = f.id