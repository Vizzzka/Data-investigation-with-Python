INSERT INTO `DWH_STAGING.Users`
SELECT
matched_users.id,
matched_users.fname,
matched_users.lname,
matched_users.email,
matched_users.country,
1 subscription,
"" categories,
UNIX_SECONDS(TIMESTAMP(matched_users.effective_start)),
matched_users.key,
matched_users.effective_end
FROM
(SELECT d_users.key, d_users.id, d_users.fname, d_users.lname, d_users.email, d_users.country, d_users.effective_start, d_users.effective_end
FROM DWH_STAGING.Users AS users INNER JOIN DWH.D_Users AS d_users ON users.id = d_users.id) matched_users