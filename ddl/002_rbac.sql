USE ROLE securityadmin;
CREATE ROLE IF NOT EXISTS pipeline_role;
GRANT ROLE pipeline_role TO ROLE sysadmin;

USE ROLE sysadmin;
GRANT USAGE ON WAREHOUSE weather_wh TO ROLE pipeline_role;
GRANT USAGE ON DATABASE weather_db TO ROLE pipeline_role;
GRANT USAGE ON ALL SCHEMAS IN DATABASE weather_db TO ROLE pipeline_role;

-- read + write raw, build staging/mart (+ snapshots)
GRANT SELECT,INSERT,UPDATE,DELETE ON ALL TABLES IN SCHEMA weather_db.raw TO ROLE pipeline_role;
GRANT CREATE TABLE,CREATE VIEW ON SCHEMA weather_db.staging TO ROLE pipeline_role;
GRANT CREATE TABLE,CREATE VIEW ON SCHEMA weather_db.mart TO ROLE pipeline_role;
GRANT SELECT,INSERT,UPDATE,DELETE ON FUTURE TABLES IN SCHEMA weather_db.mart TO ROLE pipeline_role;

GRANT ROLE pipeline_role TO USER <your_user>;