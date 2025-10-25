SELECT 'IT' AS category, municipality, occupation_name, COUNT(*) AS num_ads
FROM staging.job_ads_it
GROUP BY 1, 2, 3

UNION ALL

SELECT 'Media' AS category, municipality, occupation_name, COUNT(*) AS num_ads
FROM staging.job_ads_media
GROUP BY 1, 2, 3

UNION ALL

SELECT 'Bygg' AS category, municipality, occupation_name, COUNT(*) AS num_ads
FROM staging.job_ads_bygg
GROUP BY 1, 2, 3

ORDER BY num_ads DESC;
