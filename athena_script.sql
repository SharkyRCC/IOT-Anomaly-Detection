-- =====================================================
-- IoT Sensor Anomaly Detection - Athena SQL Scripts
-- =====================================================
-- This file contains all the Athena SQL scripts used in the IoT anomaly detection project
-- Author: [Your Name]
-- Date: [Current Date]
-- =====================================================

-- =====================================================
-- 1. CREATE DATABASE
-- =====================================================
-- Create the main database for IoT analytics
CREATE DATABASE IF NOT EXISTS iotdb
COMMENT 'Database for IoT sensor anomaly detection analytics'
LOCATION 's3://iot-sensor-anomaly-processed-data/';

-- Use the database
USE iotdb;

-- =====================================================
-- 2. CREATE EXTERNAL TABLES
-- =====================================================

-- -----------------------------------------------------
-- 2.1 Curated Data Table
-- -----------------------------------------------------
-- Table for cleaned and normalized IoT sensor data
CREATE EXTERNAL TABLE IF NOT EXISTS iot_curated (
  timestamp   timestamp COMMENT 'Sensor reading timestamp',
  device_id   string COMMENT 'Unique device identifier',
  sensor_a    double COMMENT 'Sensor A reading value',
  sensor_b    double COMMENT 'Sensor B reading value', 
  sensor_c    double COMMENT 'Sensor C reading value',
  location    string COMMENT 'Device location'
)
COMMENT 'Curated IoT sensor data - cleaned and normalized'
PARTITIONED BY (
  dt   string COMMENT 'Date partition (YYYY-MM-DD)',
  hour string COMMENT 'Hour partition (HH)'
)
STORED AS PARQUET
LOCATION 's3://iot-sensor-anomaly-processed-data/iot/curated/'
TBLPROPERTIES (
  'projection.enabled' = 'true',
  'projection.dt.type' = 'date',
  'projection.dt.range' = '2020-01-01,NOW',
  'projection.dt.format' = 'yyyy-MM-dd',
  'projection.hour.type' = 'integer',
  'projection.hour.range' = '00,23',
  'projection.hour.digits' = '2',
  'storage.location.template' = 's3://iot-sensor-anomaly-processed-data/iot/curated/dt=${dt}/hour=${hour}/'
);

-- -----------------------------------------------------
-- 2.2 Anomalies Data Table
-- -----------------------------------------------------
-- Table for detected anomalies with statistical metrics
CREATE EXTERNAL TABLE IF NOT EXISTS iot_anomalies (
  timestamp   timestamp COMMENT 'Anomaly detection timestamp',
  device_id   string COMMENT 'Device that generated the anomaly',
  sensor_a    double COMMENT 'Sensor A value at anomaly',
  sensor_b    double COMMENT 'Sensor B value at anomaly',
  sensor_c    double COMMENT 'Sensor C value at anomaly',
  metric      string COMMENT 'Which sensor metric triggered the anomaly',
  median      double COMMENT 'Median value used in robust z-score',
  mad         double COMMENT 'Median Absolute Deviation',
  robust_z    double COMMENT 'Robust z-score value'
)
COMMENT 'Detected anomalies with statistical metrics'
PARTITIONED BY (
  dt   string COMMENT 'Date partition (YYYY-MM-DD)',
  hour string COMMENT 'Hour partition (HH)'
)
STORED AS PARQUET
LOCATION 's3://iot-sensor-anomaly-processed-data/iot/anomalies/'
TBLPROPERTIES (
  'projection.enabled' = 'true',
  'projection.dt.type' = 'date',
  'projection.dt.range' = '2020-01-01,NOW',
  'projection.dt.format' = 'yyyy-MM-dd',
  'projection.hour.type' = 'integer',
  'projection.hour.range' = '00,23',
  'projection.hour.digits' = '2',
  'storage.location.template' = 's3://iot-sensor-anomaly-processed-data/iot/anomalies/dt=${dt}/hour=${hour}/'
);

-- =====================================================
-- 3. PARTITION MANAGEMENT
-- =====================================================

-- Repair table partitions (run after new data is loaded)
MSCK REPAIR TABLE iot_curated;
MSCK REPAIR TABLE iot_anomalies;

-- Alternative: Add specific partitions manually if needed
-- ALTER TABLE iot_curated ADD PARTITION (dt='2025-08-20', hour='22');
-- ALTER TABLE iot_anomalies ADD PARTITION (dt='2025-08-20', hour='22');

-- =====================================================
-- 4. SAMPLE QUERIES FOR ANALYTICS
-- =====================================================

-- -----------------------------------------------------
-- 4.1 Basic Data Exploration
-- -----------------------------------------------------

-- View recent curated data
SELECT * 
FROM iot_curated 
WHERE dt = '2025-08-20' 
ORDER BY timestamp DESC 
LIMIT 10;

-- View recent anomalies
SELECT * 
FROM iot_anomalies 
WHERE dt = '2025-08-20' 
ORDER BY timestamp DESC 
LIMIT 10;

-- Count total records by date
SELECT dt, COUNT(*) as record_count
FROM iot_curated
GROUP BY dt
ORDER BY dt DESC;

-- -----------------------------------------------------
-- 4.2 Anomaly Analysis Queries
-- -----------------------------------------------------

-- Anomaly count by hour
SELECT dt, hour, COUNT(*) as anomaly_count
FROM iot_anomalies
WHERE dt = '2025-08-20'
GROUP BY dt, hour
ORDER BY hour;

-- Top devices with most anomalies
SELECT device_id, COUNT(*) as anomaly_count
FROM iot_anomalies
WHERE dt = '2025-08-20'
GROUP BY device_id
ORDER BY anomaly_count DESC
LIMIT 10;

-- Anomalies by sensor type
SELECT metric, COUNT(*) as anomaly_count
FROM iot_anomalies
WHERE dt = '2025-08-20'
GROUP BY metric
ORDER BY anomaly_count DESC;

-- Highest severity anomalies (by robust z-score)
SELECT device_id, metric, robust_z, timestamp
FROM iot_anomalies
WHERE dt = '2025-08-20'
  AND ABS(robust_z) > 5.0
ORDER BY ABS(robust_z) DESC
LIMIT 20;

-- -----------------------------------------------------
-- 4.3 Time Series Analysis
-- -----------------------------------------------------

-- Hourly anomaly trends
SELECT 
  dt,
  hour,
  COUNT(*) as anomaly_count,
  AVG(ABS(robust_z)) as avg_severity
FROM iot_anomalies
WHERE dt >= '2025-08-19'
GROUP BY dt, hour
ORDER BY dt, hour;

-- Daily anomaly summary
SELECT 
  dt,
  COUNT(*) as total_anomalies,
  COUNT(DISTINCT device_id) as affected_devices,
  AVG(ABS(robust_z)) as avg_severity,
  MAX(ABS(robust_z)) as max_severity
FROM iot_anomalies
GROUP BY dt
ORDER BY dt DESC;

-- -----------------------------------------------------
-- 4.4 Device Health Monitoring
-- -----------------------------------------------------

-- Device performance summary
SELECT 
  c.device_id,
  c.location,
  COUNT(c.device_id) as total_readings,
  COUNT(a.device_id) as anomaly_count,
  ROUND(COUNT(a.device_id) * 100.0 / COUNT(c.device_id), 2) as anomaly_rate_percent
FROM iot_curated c
LEFT JOIN iot_anomalies a 
  ON c.device_id = a.device_id 
  AND c.dt = a.dt 
  AND c.hour = a.hour
WHERE c.dt = '2025-08-20'
GROUP BY c.device_id, c.location
ORDER BY anomaly_rate_percent DESC;

-- Devices with no recent data (potential failures)
SELECT DISTINCT device_id, location
FROM iot_curated
WHERE dt = '2025-08-19'
  AND device_id NOT IN (
    SELECT DISTINCT device_id 
    FROM iot_curated 
    WHERE dt = '2025-08-20'
  );

-- -----------------------------------------------------
-- 4.5 Statistical Analysis
-- -----------------------------------------------------

-- Sensor value distributions
SELECT 
  dt,
  AVG(sensor_a) as avg_sensor_a,
  STDDEV(sensor_a) as stddev_sensor_a,
  MIN(sensor_a) as min_sensor_a,
  MAX(sensor_a) as max_sensor_a,
  AVG(sensor_b) as avg_sensor_b,
  STDDEV(sensor_b) as stddev_sensor_b,
  AVG(sensor_c) as avg_sensor_c,
  STDDEV(sensor_c) as stddev_sensor_c
FROM iot_curated
WHERE dt >= '2025-08-19'
GROUP BY dt
ORDER BY dt;

-- Anomaly severity distribution
SELECT 
  CASE 
    WHEN ABS(robust_z) BETWEEN 3 AND 4 THEN 'Low (3-4)'
    WHEN ABS(robust_z) BETWEEN 4 AND 6 THEN 'Medium (4-6)'
    WHEN ABS(robust_z) BETWEEN 6 AND 8 THEN 'High (6-8)'
    WHEN ABS(robust_z) > 8 THEN 'Critical (>8)'
  END as severity_level,
  COUNT(*) as anomaly_count
FROM iot_anomalies
WHERE dt = '2025-08-20'
GROUP BY 
  CASE 
    WHEN ABS(robust_z) BETWEEN 3 AND 4 THEN 'Low (3-4)'
    WHEN ABS(robust_z) BETWEEN 4 AND 6 THEN 'Medium (4-6)'
    WHEN ABS(robust_z) BETWEEN 6 AND 8 THEN 'High (6-8)'
    WHEN ABS(robust_z) > 8 THEN 'Critical (>8)'
  END
ORDER BY anomaly_count DESC;

-- =====================================================
-- 5. GRAFANA DASHBOARD QUERIES
-- =====================================================

-- -----------------------------------------------------
-- 5.1 Time Series Panels
-- -----------------------------------------------------

-- Anomaly count over time (for time series chart)
SELECT 
  timestamp,
  COUNT(*) as anomaly_count
FROM iot_anomalies
WHERE dt = '2025-08-20'
GROUP BY timestamp
ORDER BY timestamp;

-- Sensor readings over time
SELECT 
  timestamp,
  AVG(sensor_a) as avg_sensor_a,
  AVG(sensor_b) as avg_sensor_b,
  AVG(sensor_c) as avg_sensor_c
FROM iot_curated
WHERE dt = '2025-08-20'
GROUP BY timestamp
ORDER BY timestamp;

-- -----------------------------------------------------
-- 5.2 Single Stat Panels
-- -----------------------------------------------------

-- Total anomalies today
SELECT COUNT(*) as total_anomalies
FROM iot_anomalies
WHERE dt = CURRENT_DATE;

-- Active devices today
SELECT COUNT(DISTINCT device_id) as active_devices
FROM iot_curated
WHERE dt = CURRENT_DATE;

-- Average anomaly severity today
SELECT ROUND(AVG(ABS(robust_z)), 2) as avg_severity
FROM iot_anomalies
WHERE dt = CURRENT_DATE;

-- -----------------------------------------------------
-- 5.3 Table Panels
-- -----------------------------------------------------

-- Recent critical anomalies
SELECT 
  timestamp,
  device_id,
  metric,
  robust_z
FROM iot_anomalies
WHERE dt = CURRENT_DATE
  AND ABS(robust_z) > 6.0
ORDER BY timestamp DESC
LIMIT 50;

-- =====================================================
-- 6. MAINTENANCE QUERIES
-- =====================================================

-- Check table schemas
DESCRIBE iot_curated;
DESCRIBE iot_anomalies;

-- Check partition information
SHOW PARTITIONS iot_curated;
SHOW PARTITIONS iot_anomalies;

-- Table statistics
SELECT 
  'iot_curated' as table_name,
  COUNT(*) as row_count
FROM iot_curated
WHERE dt = '2025-08-20'
UNION ALL
SELECT 
  'iot_anomalies' as table_name,
  COUNT(*) as row_count
FROM iot_anomalies
WHERE dt = '2025-08-20';

-- Data quality checks
SELECT 
  dt,
  hour,
  COUNT(*) as total_records,
  COUNT(DISTINCT device_id) as unique_devices,
  SUM(CASE WHEN sensor_a IS NULL THEN 1 ELSE 0 END) as null_sensor_a,
  SUM(CASE WHEN sensor_b IS NULL THEN 1 ELSE 0 END) as null_sensor_b,
  SUM(CASE WHEN sensor_c IS NULL THEN 1 ELSE 0 END) as null_sensor_c
FROM iot_curated
WHERE dt = '2025-08-20'
GROUP BY dt, hour
ORDER BY hour;

-- =====================================================
-- 7. COST OPTIMIZATION QUERIES
-- =====================================================

-- Query to check data volume by partition (for cost estimation)
SELECT 
  dt,
  hour,
  COUNT(*) as record_count,
  COUNT(*) * 100 as estimated_bytes  -- Rough estimate
FROM iot_curated
GROUP BY dt, hour
ORDER BY dt DESC, hour DESC;

-- Most queried partitions (add this info manually based on CloudTrail)
-- This helps identify which partitions to keep in frequently accessed storage

-- =====================================================
-- END OF SCRIPT
-- =====================================================
