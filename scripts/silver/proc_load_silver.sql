/*****************************************************************************************
   Stored Procedure : silver.load_silver
   Script Purpose   : ETL (Extract, Transform, Load) process to populate the 'silver' schema 
                      tables from the 'bronze' schema.

   Actions Performed:
     - Truncates existing Silver tables (clears all old data).
     - Inserts transformed and cleansed data from Bronze into Silver.

   Author      : Jitendra Kumar Manasingh
   Created On  : 23-Aug-2025

   Parameters  : None
                 (This stored procedure does not accept parameters or return values.)

   WARNING     :
     - TRUNCATE TABLE removes ALL existing data.
     - Ensure SQL Server has proper read permissions on source folders if BULK INSERT is used.
*****************************************************************************************/


CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
    DECLARE 
        @start_time DATETIME, 
        @end_time DATETIME, 
        @batch_start_time DATETIME, 
        @batch_end_time DATETIME;

    BEGIN TRY
        -- ===========================================
        -- Start Overall Batch Timer
        -- ===========================================
        SET @batch_start_time = GETDATE();

        PRINT '=========================================================';
        PRINT 'Starting Silver Layer Load';
        PRINT '=========================================================';

        /*****************************************************************************************
           Load CRM Tables
        *****************************************************************************************/
        PRINT '---------------------------------------------------------';    
        PRINT 'Loading CRM Tables';
        PRINT '---------------------------------------------------------';

        -----------------------------------------------------------------------------------------
        -- Load CRM Customer Info
        -----------------------------------------------------------------------------------------
        SET @start_time = GETDATE();
        PRINT '>> Truncating: silver.crm_cust_info';
        TRUNCATE TABLE silver.crm_cust_info;

        PRINT '>> Inserting Data Into: silver.crm_cust_info';
        INSERT INTO silver.crm_cust_info (
            cst_id,
            cst_key,
            cst_firstname,
            cst_lastname,
            cst_marital_status,
            cst_gndr,
            cst_create_date
        )
        SELECT
            cst_id,
            cst_key,
            TRIM(cst_firstname) AS cst_firstname,   -- Remove extra spaces from first name
            TRIM(cst_lastname)  AS cst_lastname,    -- Remove extra spaces from last name

            -- Normalize marital status values
            CASE 
                WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
                WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
                ELSE 'n/a'
            END AS cst_marital_status, 

            -- Normalize gender values
            CA
