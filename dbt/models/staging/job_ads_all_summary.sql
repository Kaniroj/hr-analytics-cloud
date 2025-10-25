WITH all_ads AS (

  SELECT 'IT' AS category, occupation_name
  FROM staging.job_ads_it

  UNION ALL

  SELECT 'Media' AS category, occupation_name
  FROM staging.job_ads_media

  UNION ALL

  SELECT 'Bygg' AS category, occupation_name
  FROM staging.job_ads_bygg

  UNION ALL

  SELECT 'Pedagogik' AS category, occupation_name
  FROM staging.job_ads_pedagogik

)

SELECT
  category,
  occupation_name,
  COUNT(*) AS num_ads
FROM all_ads
GROUP BY category, occupation_name
ORDER BY num_ads DESC
