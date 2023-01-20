UPDATE DWH_STAGING.Users
SET effective_end = DATETIME(2100, 12, 25, 05, 30, 00)
WHERE effective_end is NULL