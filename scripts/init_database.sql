/*****************************************************************************************
   Script Name : DataWarehouse_Setup.sql
   Purpose     : To create a fresh 'DataWarehouse' database with Bronze, Silver, and Gold 
                 schemas, following the Medallion Architecture for structured data pipelines.

   Author      : Jitendra Kumar Manasingh
   Created On  : 22/08/2025

   Description :
       - Drops the existing 'DataWarehouse' database if it exists.
       - Creates a new 'DataWarehouse' database.
       - Defines three schemas (Bronze, Silver, Gold) to separate raw, cleaned, 
         and business-ready data.

   WARNING :
       - This script will DROP the existing 'DataWarehouse' database (if any).
       - Ensure you have taken a BACKUP before running this script in production.
       - All connections to the database will be terminated immediately when dropped.

   Usage :
       - Run this script on SQL Server Management Studio (SSMS).
       - Execute from the 'master' database context.

*****************************************************************************************/

-- Step 1: Always start from the master database 
-- (CREATE/DROP DATABASE commands must be executed from 'master')
USE master;
GO

-- Step 2: Drop 'DataWarehouse' if it already exists
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
    -- Force SINGLE_USER mode to disconnect all active users immediately
    ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;

    -- Drop the existing database
    DROP DATABASE DataWarehouse;
END;
GO

-- Step 3: Create a fresh 'DataWarehouse' database
CREATE DATABASE DataWarehouse;
GO

-- Step 4: Switch context to the new 'DataWarehouse' database
USE DataWarehouse;
GO

/*****************************************************************************************
   Step 5: Create Schemas for Medallion Architecture
   - Bronze: Raw data (loaded as-is from source, minimal transformation)
   - Silver: Cleaned, validated, standardized data (ready for integration)
   - Gold: Business-ready, aggregated data (for reporting & analytics)
*****************************************************************************************/

-- Create Bronze schema (raw data layer)
CREATE SCHEMA bronze;
GO

-- Create Silver schema (cleaned/standardized data layer)
CREATE SCHEMA silver;
GO

-- Create Gold schema (business-ready analytics layer)
CREATE SCHEMA gold;
GO
