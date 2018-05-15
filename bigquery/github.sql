-- Get all commits
-- Processed 95.7 GB in 30 seconds
SELECT
    author.date,
    commits.repo_name,
    author.tz_offset
FROM
    [bigquery-public-data:github_repos.commits] commits
WHERE
    DATE(author.date) BETWEEN '2016-01-01'
    AND '2016-12-31'

-- Get all languages
-- Processed 136 MB in 40 seconds
SELECT repo_name, language.name FROM [bigquery-public-data:github_repos.languages]

-- Investigating Ruby on 01/03/2016
-- Processed 94.1 GB in 3 seconds
SELECT
    repo_name,
    COUNT(*) as cnt
FROM
    [bigquery-public-data:github_repos.commits]
WHERE
    DATE(author.date) = "2016-01-03"
GROUP BY
    repo_name
ORDER BY
    cnt DESC
