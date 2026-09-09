USE SCHEMA revops.raw;

-- Distribution by stage level 
SELECT deal_stage,
       COUNT(*)                   AS n,
       COUNT(close_value)         AS with_value,
       ROUND(AVG(close_value), 0) AS avg_value
FROM sales_pipeline
GROUP BY 1
ORDER BY 2 DESC

-- Range of interest
SELECT MIN(engage_date) AS first_engage,
       MAX(close_date)  AS last_close
FROM sales_pipeline;

-- Duplocations
SELECT COUNT(*) - COUNT(DISTINCT opportunity_id) AS dupes
FROM sales_pipeline

-- Orphans primary keys
SELECT COUNT(*) AS orphan_accounts
FROM sales_pipeline p
LEFT JOIN accounts a ON p.account = a.account
WHERE p.account IS NOT NULL AND a.account IS NULL


SELECT COUNT(*) AS orphan_agents
FROM sales_pipeline p
LEFT JOIN sales_teams t ON p.sales_agent = t.sales_agent
WHERE t.sales_agent IS NULL