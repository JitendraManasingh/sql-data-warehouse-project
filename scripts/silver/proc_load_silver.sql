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
            CASE 
                WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
                WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
                ELSE 'n/a'
            END AS cst_gndr, 

            cst_create_date
        FROM (
            -- Rank records per customer by creation date (latest = rank 1)
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY cst_id 
                       ORDER BY cst_create_date DESC
                   ) AS flag_last
            FROM bronze.crm_cust_info
            WHERE cst_id IS NOT NULL
        ) t
        WHERE flag_last = 1;  -- Keep only the latest record per customer

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' Seconds';


        -----------------------------------------------------------------------------------------
        -- Load CRM Product Info
        -----------------------------------------------------------------------------------------
        SET @start_time = GETDATE();
        PRINT '>> Truncating: silver.crm_prd_info';
        TRUNCATE TABLE silver.crm_prd_info;

        PRINT '>> Inserting Data Into: silver.crm_prd_info';
        INSERT INTO silver.crm_prd_info (
            prd_id,
            cat_id,
            prd_key,
            prd_nm,
            prd_cost,
            prd_line,
            prd_start_dt,
            prd_end_dt
        )
        SELECT
            prd_id,
            REPLACE(SUBSTRING(prd_key, 1,5),'-','_') AS cat_id, -- Extract and clean category ID
            SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key,        -- Remove prefix for clean product key
            prd_nm,
            ISNULL(prd_cost,0) AS prd_cost,                      -- Replace NULL with 0
            CASE UPPER(TRIM(prd_line))                           -- Normalize product line
                WHEN 'M' THEN 'Mountain'
                WHEN 'R' THEN 'Road'
                WHEN 'S' THEN 'Other Sales'
                WHEN 'T' THEN 'Touring'
                ELSE 'n/a'
            END AS prd_line,
            CAST(prd_start_dt AS DATE) AS prd_start_dt,
            CAST(LEAD(prd_start_dt) OVER (
                    PARTITION BY prd_key ORDER BY prd_start_dt
                ) - 1 AS DATE) AS prd_end_dt
        FROM bronze.crm_prd_info;

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' Seconds';


        -----------------------------------------------------------------------------------------
        -- Load CRM Sales Details
        -----------------------------------------------------------------------------------------
        SET @start_time = GETDATE();
        PRINT '>> Truncating: silver.crm_sales_details';
        TRUNCATE TABLE silver.crm_sales_details;

        PRINT '>> Inserting Data Into: silver.crm_sales_details';
        INSERT INTO silver.crm_sales_details (
            sls_ord_num,
            sls_prd_key,
            sls_cust_id,
            sls_order_dt,
            sls_ship_dt,
            sls_due_dt,
            sls_sales,
            sls_quantity,
            sls_price
        )
        SELECT
            sls_ord_num,
            sls_prd_key,
            sls_cust_id,

            -- Validate and convert Order Date (YYYYMMDD)
            CASE 
                WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
                ELSE CONVERT(DATE, CAST(sls_order_dt AS CHAR(8)), 112)
            END AS sls_order_dt,

            -- Validate and convert Ship Date
            CASE 
                WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
                ELSE CONVERT(DATE, CAST(sls_ship_dt AS CHAR(8)), 112)
            END AS sls_ship_dt, 

            -- Validate and convert Due Date
            CASE 
                WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
                ELSE CONVERT(DATE, CAST(sls_due_dt AS CHAR(8)), 112)
            END AS sls_due_dt,

            -- Handle invalid sales values
            CASE 
                WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price IS NULL OR sls_price <= 0 
                    THEN sls_quantity * ABS(sls_price)
                ELSE ABS(sls_sales)
            END AS sls_sales,

            sls_quantity,  

            -- Handle invalid price values
            CASE 
                WHEN sls_price IS NULL OR sls_price <= 0 THEN 
                    CASE 
                        WHEN sls_quantity > 0 THEN ABS(sls_sales) / sls_quantity 
                        ELSE NULL 
                    END
                ELSE ABS(sls_price)
            END AS sls_price
        FROM bronze.crm_sales_details;

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' Seconds';


        /*****************************************************************************************
           Load ERP Tables
        *****************************************************************************************/
        PRINT '---------------------------------------------------------';    
        PRINT 'Loading ERP Tables';
        PRINT '---------------------------------------------------------';

        -----------------------------------------------------------------------------------------
        -- Load ERP Customer Info
        -----------------------------------------------------------------------------------------
        SET @start_time = GETDATE();
        PRINT '>> Truncating: silver.erp_cust_az12';
        TRUNCATE TABLE silver.erp_cust_az12;

        PRINT '>> Inserting Data Into: silver.erp_cust_az12';
        INSERT INTO silver.erp_cust_az12 (
            cid,
            bdate,
            gen
        )
        SELECT 
            -- Remove 'NAS' prefix if present
            CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid)) ELSE cid END cid, 

            -- Convert future birthdates to NULL
            CASE WHEN bdate > GETDATE() THEN NULL ELSE bdate END AS bdate, 

            -- Normalize gender values
            CASE 
                WHEN UPPER(TRIM(gen)) IN ('F', 'FRMALE') THEN 'Female'
                WHEN UPPER(TRIM(gen)) IN ('M', 'MALE')  THEN 'Male'
                ELSE 'n/a'
            END AS gen
        FROM bronze.erp_cust_az12;

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' Seconds';


        -----------------------------------------------------------------------------------------
        -- Load ERP Location Info
        -----------------------------------------------------------------------------------------
        SET @start_time = GETDATE();
        PRINT '>> Truncating: silver.erp_loc_a101';
        TRUNCATE TABLE silver.erp_loc_a101;

        PRINT '>> Inserting Data Into: silver.erp_loc_a101';
        INSERT INTO silver.erp_loc_a101 (
            cid,
            cntry
        )
        SELECT
            REPLACE(cid, '-','') AS cid,  -- Remove hyphens from CID
            CASE 
                WHEN TRIM(cntry) = 'DE' THEN 'Germany'
                WHEN TRIM(cntry) IN ('US','USA') THEN 'United States'
                WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
                ELSE TRIM(cntry)
            END AS cntry
        FROM bronze.erp_loc_a101;

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' Seconds';


        -----------------------------------------------------------------------------------------
        -- Load ERP Product Category Info
        -----------------------------------------------------------------------------------------
        SET @start_time = GETDATE();
        PRINT '>> Truncating: silver.erp_px_cat_g1V2';
        TRUNCATE TABLE silver.erp_px_cat_g1V2;

        PRINT '>> Inserting Data Into: silver.erp_px_cat_g1V2';
        INSERT INTO silver.erp_px_cat_g1V2 (
            id,
            cat,
            subcat,
            maintenance
        )
        SELECT
            id,
            cat,
            subcat,
            maintenance
        FROM bronze.erp_px_cat_g1V2;  -- FIX: Use bronze as source, not silver

        -- ===========================================
        -- End Overall Batch Timer
        -- ===========================================
        SET @batch_end_time = GETDATE();
        PRINT '=========================================================';
        PRINT 'Silver Layer Load Completed Successfully';
        PRINT '   - Total Duration: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' Seconds';
        PRINT '=========================================================';

    END TRY

    BEGIN CATCH
        -- ===========================================
        -- Error Handling
        -- ===========================================
        PRINT '=========================================================';
        PRINT 'ERROR OCCURRED DURING SILVER LAYER LOAD';
        PRINT 'Error Message: ' + ERROR_MESSAGE();
        PRINT 'Error Number : ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'Error State  : ' + CAST(ERROR_STATE() AS NVARCHAR);
        PRINT '=========================================================';
    END CATCH
END;
