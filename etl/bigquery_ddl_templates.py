# Example BigQuery DDLs for core models

# Dimension Table: Parish
dim_parish = """
CREATE OR REPLACE TABLE `neon-project-487902.Neon_Dataset_Project.dim_parish` AS
SELECT DISTINCT
  id AS parish_id,
  name,
  address,
  parishType,
  createdAt,
  updatedAt
FROM `neon-project-487902.Neon_Dataset_Project.parish`;
"""

# Dimension Table: User
dim_user = """
CREATE OR REPLACE TABLE `neon-project-487902.Neon_Dataset_Project.dim_user` AS
SELECT DISTINCT
  id AS user_id,
  name,
  email,
  role,
  createdAt,
  updatedAt
FROM `neon-project-487902.Neon_Dataset_Project.user`;
"""

# Fact Table: Payment
fact_payment = """
CREATE OR REPLACE TABLE `neon-project-487902.Neon_Dataset_Project.fact_payment` AS
SELECT
  p.id AS payment_id,
  p.userId AS user_id,
  p.parishId AS parish_id,
  p.amount,
  p.baseAmount,
  p.serviceCharge,
  p.type,
  p.status,
  p.createdAt,
  p.updatedAt,
  u.role AS user_role,
  pa.parishType
FROM `neon-project-487902.Neon_Dataset_Project.payment` p
LEFT JOIN `neon-project-487902.Neon_Dataset_Project.user` u ON p.userId = u.id
LEFT JOIN `neon-project-487902.Neon_Dataset_Project.parish` pa ON p.parishId = pa.id;
"""

# Aggregate: Parish Daily Metrics
agg_parish_daily_metrics = """
CREATE OR REPLACE TABLE `neon-project-487902.Neon_Dataset_Project.agg_parish_daily_metrics` AS
SELECT
  parish_id,
  DATE(createdAt) AS date,
  COUNT(*) AS num_payments,
  SUM(amount) AS total_amount,
  SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) AS failed_payments
FROM `neon-project-487902.Neon_Dataset_Project.fact_payment`
GROUP BY parish_id, date;
"""

# Add more DDLs as needed for other facts, dimensions, and aggregates
