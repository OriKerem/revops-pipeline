CREATE OR REPLACE TABLE accounts AS
SELECT
  C1                AS account,
  C2                AS sector,
  TRY_TO_NUMBER(C3) AS year_established,
  TRY_TO_NUMBER(C4) AS revenue,
  TRY_TO_NUMBER(C5) AS employees,
  C6                AS office_location,
  C7                AS subsidiary_of
FROM accounts
WHERE C1 <> 'account';

CREATE OR REPLACE TABLE products AS
SELECT
  C1                AS product,
  C2                AS series,
  TRY_TO_NUMBER(C3) AS sales_price
FROM products
WHERE C1 <> 'product';

CREATE OR REPLACE TABLE sales_teams AS
SELECT
  C1 AS sales_agent,
  C2 AS manager,
  C3 AS regional_office
FROM sales_teams
WHERE C1 <> 'sales_agent';



SELECT 'accounts' t, COUNT(*) FROM accounts
UNION ALL SELECT 'products', COUNT(*) FROM products
UNION ALL SELECT 'sales_teams', COUNT(*) FROM sales_teams
UNION ALL SELECT 'sales_pipeline', COUNT(*) FROM sales_pipeline;