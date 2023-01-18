CREATE OR REPLACE TABLE DWH.D_Videos
PARTITION BY RANGE_BUCKET ( id, GENERATE_ARRAY(1, 5000, 100)) AS (

SELECT DISTINCT
  videos.id,
  name,
  url,
  TIMESTAMP_SECONDS(SAFE_CAST(creation_timestamp AS INTEGER)) creation_timestamp,
  SAFE_CAST(creator_id AS INTEGER) AS creator_id,
  D_Users.key AS creator_key,
  private
FROM
  (SELECT * FROM DWH_STAGING.Videos
WHERE id IS NOT NULL
  AND name is not NULL
  AND url is not NULL
  AND SAFE_CAST(creation_timestamp AS INTEGER) IS NOT NULL
  AND REGEXP_CONTAINS(url, "((http|https)://)(www.)?[a-zA-Z0-9@:%._\\+~#?&//=]{2,256}\\.[a-z]{2,6}\\b([-a-zA-Z0-9@:%._\\+~#?&//=]*)")
  AND (private = 1 OR private = 0)
  AND SAFE_CAST(creator_id AS INTEGER) IS NOT NULL
  ) videos
  LEFT JOIN DWH.D_Users
ON D_Users.id = SAFE_CAST(videos.creator_id AS INTEGER)

)
