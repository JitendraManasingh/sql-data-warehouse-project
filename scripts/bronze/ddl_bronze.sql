/*****************************************************************************************
   Script Name : bronze.load_bronze
   Purpose     : Load raw CSV data from CRM & ERP sources into Bronze schema (staging layer).
   Author      : Jitendra Kumar Manasingh
   Created On  : 23/08/2025

   Description :
       - Truncate Bronze tables (clear old data).
       - Bulk insert fresh CSV data from source files.
       - Print progress messages and log duration for each step.
       - Provide basic error handling for debugging.

   WARNING :
       - TRUNCATE TABLE removes ALL existing data.
       - BULK INSERT requires that the file paths exist and SQL Server has access.
       - Ensure proper permissions on the data folder.
*****************************************************************************************/


/*****************************************************************************************
   Execute the Procedure
   ---------------------------------------------------------------------
   Example:
       EXEC bronze.load_bronze;
*****************************************************************************************/


CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
    -- Declare variables to track timings
    DECLARE @start_time DATETIME, @end_time DATETIME;
    DECLARE @batch_start_time DATETIME, @batch_end_time DATETIME;

    BEGIN TRY
        -- Start overall batch timer
        SET @batch_start_time = GETDATE();

        PRINT '=========================================================';
        PRINT '🚀 Starting Bronze Layer Load';
        PRINT '=========================================================';

        /*****************************************************************************************
           Load CRM Tables
        *****************************************************************************************/
        PRINT '---------------------------------------------------------';    
        PRINT '📂 Loading CRM Tables';
        PRINT '---------------------------------------------------------';

        -----------------------------------------
        -- CRM Customer Information
        -----------------------------------------
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.crm_cust_info';
        TRUNCATE TABLE bronze.crm_cust_info;

        PRINT '>> Inserting Data Into: bronze.crm_cust_info';
        BULK INSERT bronze.crm_cust_info
        FROM 'D:\SQL - MySQL Projects\The Complete SQL Bootcamp (30 Hours) Go from Zero to Hero - Baraa Khatib Salkini\SQL - Data Warehouse Projects\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
        WITH (
            FIRSTROW = 2,              -- Skip header row
            FIELDTERMINATOR = ',',     -- CSV delimiter
            ROWTERMINATOR = '\n',      -- Line break
            TABLOCK                    -- Table lock for faster load
        );

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' Seconds';


        -----------------------------------------
        -- CRM Product Information
        -----------------------------------------
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.crm_prd_info';
        TRUNCATE TABLE bronze.crm_prd_info;

        PRINT '>> Inserting Data Into: bronze.crm_prd_info';
        BULK INSERT bronze.crm_prd_info
        FROM 'D:\SQL - MySQL Projects\The Complete SQL Bootcamp (30 Hours) Go from Zero to Hero - Baraa Khatib Salkini\SQL - Data Warehouse Projects\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' Seconds';


        -----------------------------------------
        -- CRM Sales Details
        -----------------------------------------
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.crm_sales_details';
        TRUNCATE TABLE bronze.crm_sales_details;

        PRINT '>> Inserting Data Into: bronze.crm_sales_details';
        BULK INSERT bronze.crm_sales_details
        FROM 'D:\SQL - MySQL Projects\The Complete SQL Bootcamp (30 Hours) Go from Zero to Hero - Baraa Khatib Salkini\SQL - Data Warehouse Projects\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' Seconds';


        /*****************************************************************************************
           Load ERP Tables
        *****************************************************************************************/
        PRINT '---------------------------------------------------------';    
        PRINT '📂 Loading ERP Tables';
        PRINT '---------------------------------------------------------';

        -----------------------------------------
        -- ERP Customer Information
        -----------------------------------------
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.erp_cust_az12';
        TRUNCATE TABLE bronze.erp_cust_az12;

        PRINT '>> Inserting Data Into: bronze.erp_cust_az12';
        BULK INSERT bronze.erp_cust_az12
        FROM 'D:\SQL - MySQL Projects\The Complete SQL Bootcamp (30 Hours) Go from Zero to Hero - Baraa Khatib Salkini\SQL - Data Warehouse Projects\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' Seconds';


        -----------------------------------------
        -- ERP Location Information
        -----------------------------------------
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.erp_loc_a101';
        TRUNCATE TABLE bronze.erp_loc_a101;

        PRINT '>> Inserting Data Into: bronze.erp_loc_a101';
        BULK INSERT bronze.erp_loc_a101
        FROM 'D:\SQL - MySQL Projects\The Complete SQL Bootcamp (30 Hours) Go from Zero to Hero - Baraa Khatib Salkini\SQL - Data Warehouse Projects\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' Seconds';


        -----------------------------------------
        -- ERP Product Category Information
        -----------------------------------------
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.erp_px_cat_g1V2';
        TRUNCATE TABLE bronze.erp_px_cat_g1V2;

        PRINT '>> Inserting Data Into: bronze.erp_px_cat_g1V2';
        BULK INSERT bronze.erp_px_cat_g1V2
        FROM 'D:\SQL - MySQL Projects\The Complete SQL Bootcamp (30 Hours) Go from Zero to Hero - Baraa Khatib Salkini\SQL - Data Warehouse Projects\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' Seconds';


        /*****************************************************************************************
           Final Summary
        *****************************************************************************************/
        SET @batch_end_time = GETDATE();
        PRINT '=========================================================';
        PRINT '✅ Bronze Layer Load Completed Successfully';
        PRINT '   - Total Duration: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' Seconds';
        PRINT '=========================================================';

    END TRY

    BEGIN CATCH
        PRINT '=========================================================';
        PRINT '❌ ERROR OCCURRED DURING BRONZE LAYER LOAD';
        PRINT 'Error Message: ' + ERROR_MESSAGE();
        PRINT 'Error Number : ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'Error State  : ' + CAST(ERROR_STATE() AS NVARCHAR);
        PRINT '=========================================================';
    END CATCH
END;
GO
