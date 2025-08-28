/*****************************************************************************************
    Quality Check Script: Silver Layer
    Purpose:
        This script performs data quality checks on the Silver Layer tables.
        It validates:
            - Primary Key uniqueness
            - Null values
            - Invalid/negative numbers
            - Date consistency and business rules
            - Data standardization and boundary checks

    Expectation:
        All checks should ideally return **zero rows**.
        If rows are returned, those records violate data quality standards.
*****************************************************************************************/

---------------------------------------------------------
-- 1. CRM Customer Information: Primary Key Validation
---------------------------------------------------------
-- Check for NULL or duplicate Customer IDs
-- Expectation: No rows should be returned
SELECT 
    cst_id,
    COUNT(*) AS duplicate_count
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1
   OR cst_id IS NULL;


---------------------------------------------------------
-- 2. CRM Customer Information: Data Cleanliness
---------------------------------------------------------
-- Check for unwanted spaces in First Name field
-- Expectation: No rows should be returned
SELECT 
    cst_firstname
FROM silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);


---------------------------------------------------------
-- 3. CRM Product Information: Cost Validations
---------------------------------------------------------
-- Check for NULL or Negative Product Costs
-- Expectation: No rows should be returned
SELECT 
    prd_id,
    prd_nm,
    prd_cost
FROM silver.crm_prd_info
WHERE prd_cost IS NULL 
   OR prd_cost < 0;


---------------------------------------------------------
-- 4. CRM Product Information: Standardization
---------------------------------------------------------
-- Validate consistency of Product Line values
-- Expectation: Business should validate distinct values
SELECT DISTINCT 
    prd_line
FROM silver.crm_prd_info;


---------------------------------------------------------
-- 5. CRM Product Information: Date Validations
---------------------------------------------------------
-- Check for invalid date ordering (end date before start date)
SELECT 
    prd_id,
    prd_key,
    prd_nm,
    prd_start_dt,
    prd_end_dt
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt;


---------------------------------------------------------
-- 6. CRM Sales Details: Date Validations
---------------------------------------------------------
-- Order Date must always be earlier than Ship Date or Due Date
SELECT 
    sls_ord_num,
    sls_order_dt,
    sls_ship_dt,
    sls_due_dt
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt 
   OR sls_order_dt > sls_due_dt;


---------------------------------------------------------
-- 7. CRM Sales Details: Business Rule Validations
---------------------------------------------------------
-- Rule: Sales = Quantity * Price
--       No NULLs, Zeros, or Negative values
SELECT DISTINCT
    sls_ord_num,
    sls_sales       AS old_sls_sales,
    sls_quantity,
    sls_price       AS old_sls_price,

    -- Auto-corrected values (if business allows fixes at DWH level)
    CASE 
        WHEN sls_sales IS NULL OR sls_sales <= 0 
             OR sls_sales != sls_quantity * ABS(sls_price)
        THEN sls_quantity * ABS(sls_price)
        ELSE sls_sales
    END AS derived_sales,

    CASE 
        WHEN sls_price IS NULL OR sls_price <= 0
        THEN sls_sales / NULLIF(sls_quantity, 0)
        ELSE sls_price
    END AS derived_price

FROM silver.crm_sales_details
WHERE sls_sales IS NULL OR sls_sales <= 0
   OR sls_quantity IS NULL OR sls_quantity <= 0
   OR sls_price IS NULL OR sls_price <= 0
   OR sls_sales != sls_quantity * sls_price
ORDER BY sls_ord_num;


---------------------------------------------------------
-- 8. CRM Sales Details: Example Debug Record
---------------------------------------------------------
-- Fetch a problematic record for investigation
SELECT *
FROM silver.crm_sales_details
WHERE sls_ord_num = 'SO69066';


---------------------------------------------------------
-- 9. ERP Location / Customer / Category Tables
---------------------------------------------------------
-- Example Check: Nulls or unexpected values in ERP tables
-- Add as needed based on business rules
SELECT *
FROM silver.erp_loc_a101
WHERE cid IS NULL OR CNTRY IS NULL;

SELECT *
FROM silver.erp_cust_az12
WHERE cid IS NULL OR bdate IS NULL;

SELECT *
FROM silver.erp_px_cat_g1V2
WHERE id IS NULL OR cat IS NULL;
