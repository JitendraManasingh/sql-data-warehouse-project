/*****************************************************************************************
   DDL Script Name : Create Bronze Tables
   Script Purpose  : This script creates tables in the 'bronze' schema, dropping existing tables if they already exist. Run this script to re-define the DDL structure of 'bronze' tables.

   Author      : Jitendra Kumar Manasingh
   Created On  : 23/08/2025

   Description :
       - Drops and recreates Bronze layer tables to ensure a clean staging area.
       - Stores raw data imported from CRM and ERP systems before transformation
         into Silver and Gold layers.

   WARNING :
       - Running this script will DROP existing tables in the Bronze schema.
       - All previously loaded data in these tables will be LOST.
       - Intended for ETL staging; do not store permanent records here.

*****************************************************************************************/

-- Switch to DataWarehouse database
USE DataWarehouse;
GO


/*****************************************************************************************
   CRM Source Tables (Bronze Layer)
*****************************************************************************************/

-- ================================
-- CRM Customer Information (Raw)
-- ================================
IF OBJECT_ID('bronze.crm_cust_info','U') IS NOT NULL
    DROP TABLE bronze.crm_cust_info;

CREATE TABLE bronze.crm_cust_info (
    cst_id              INT,             -- Customer ID (numeric surrogate)
    cst_key             NVARCHAR(50),    -- Customer business key from CRM
    cst_firstname       NVARCHAR(50),    -- Customer first name
    cst_lastname        NVARCHAR(50),    -- Customer last name
    cst_marital_status  NVARCHAR(50),    -- Marital status
    cst_gndr            NVARCHAR(50),    -- Gender
    cst_create_date     DATE             -- Customer creation date in CRM
);
GO


-- ================================
-- CRM Product Information (Raw)
-- ================================
IF OBJECT_ID('bronze.crm_prd_info','U') IS NOT NULL
    DROP TABLE bronze.crm_prd_info;

CREATE TABLE bronze.crm_prd_info (
    prd_id          INT,             -- Product ID (numeric surrogate)
    prd_key         NVARCHAR(50),    -- Product business key from CRM
    prd_nm          NVARCHAR(50),    -- Product name
    prd_cost        INT,             -- Product cost
    prd_line        NVARCHAR(50),    -- Product line/category
    prd_start_dt    DATETIME,        -- Product start date (valid from)
    prd_end_dt      DATETIME         -- Product end date (valid to)
);
GO


-- ================================
-- CRM Sales Details (Raw)
-- ================================
IF OBJECT_ID('bronze.crm_sales_details','U') IS NOT NULL
    DROP TABLE bronze.crm_sales_details;

CREATE TABLE bronze.crm_sales_details (
    sls_ord_num     NVARCHAR(50),    -- Sales order number
    sls_prd_key     NVARCHAR(50),    -- Product key (foreign key to product)
    sls_cust_id     INT,             -- Customer ID (foreign key to customer)
    sls_order_dt    INT,             -- Order date (consider changing to DATE)
    sls_ship_dt     INT,             -- Shipment date (consider changing to DATE)
    sls_due_dt      INT,             -- Due date (consider changing to DATE)
    sls_sales       INT,             -- Total sales amount
    sls_quantity    INT,             -- Quantity sold
    sls_price       INT              -- Price per unit
);
GO



/*****************************************************************************************
   ERP Source Tables (Bronze Layer)
*****************************************************************************************/

-- ================================
-- ERP Location Information (Raw)
-- ================================
IF OBJECT_ID('bronze.erp_loc_a101','U') IS NOT NULL
    DROP TABLE bronze.erp_loc_a101;

CREATE TABLE bronze.erp_loc_a101 (
    cid     NVARCHAR(50),    -- Customer/location ID
    CNTRY   NVARCHAR(50)     -- Country name
);
GO


-- ================================
-- ERP Customer Information (Raw)
-- ================================
IF OBJECT_ID('bronze.erp_cust_az12','U') IS NOT NULL
    DROP TABLE bronze.erp_cust_az12;

CREATE TABLE bronze.erp_cust_az12 (
    cid     VARCHAR(50),     -- Customer ID
    bdate   DATE,            -- Birth date
    gen     NVARCHAR(50)     -- Gender
);
GO


-- ================================
-- ERP Product Category Information (Raw)
-- ================================
IF OBJECT_ID('bronze.erp_px_cat_g1V2','U') IS NOT NULL
    DROP TABLE bronze.erp_px_cat_g1V2;

CREATE TABLE bronze.erp_px_cat_g1V2 (
    id           NVARCHAR(50),    -- Category ID
    cat          NVARCHAR(50),    -- Main category
    subcat       NVARCHAR(50),    -- Sub-category
    maintenance  NVARCHAR(50)     -- Maintenance information/status
);
GO
