WITH visitors AS (
    SELECT
        date,
        unique_ip_count
    FROM
        ogd_analytics.daily_unique_external_user_ip_count
), interactions AS (
    SELECT
        date,
        dataset_interactions
    FROM
        ogd_analytics.daily_external_user_dataset_interactions
)
SELECT 
    DATE(v.date),
    v.unique_ip_count,
    i.dataset_interactions
FROM
    visitors v
JOIN
    interactions i
ON
    (v.date = i.date)
ORDER BY
    date DESC