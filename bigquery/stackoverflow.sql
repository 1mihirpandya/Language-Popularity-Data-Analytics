-- Getting all posts
-- Processed 751 MB in 18 seconds
SELECT
    id,
    creation_date,
    tags,
    owner_user_id
FROM
    [bigquery-public-data:stackoverflow.posts_questions]
WHERE
    DATE(creation_date) BETWEEN '2016-01-01'
    AND '2016-12-31'

-- Getting all users
-- Processed 112 MB in 38 seconds
SELECT
    id, location
FROM
    [bigquery-public-data:stackoverflow.users]
