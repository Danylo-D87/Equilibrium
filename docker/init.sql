-- ====================================================================================
-- PostgreSQL Initialization Script
-- ====================================================================================
-- This script is automatically executed by the Docker container on the first startup.
-- It ensures that the secondary database for analytics statistics is created.
--
-- Note: The primary database ('market_data') is created automatically based on
-- the POSTGRES_DB environment variable defined in docker-compose.yml.
-- ====================================================================================

CREATE DATABASE app_stats;