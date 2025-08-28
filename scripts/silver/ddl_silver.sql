/********************************************************************************************
 Script Name    : Silver Layer - Table Creation (DDL)
 Script Purpose : 
    - This script creates tables under the 'silver' schema. 
    - If tables already exist, they will be dropped before re-creation. 
    - These tables serve as the refined/cleansed layer (post Bronze ingestion) for downstream usage.

 Note:
    Run this script whenever you need to re-define the DDL structure of the 'silver' tables.

 Author         : Jitendra Kumar Manasingh
********************************************************************************************/

-- ============================================
-- CRM Customer Information (Silver Layer)
-- ============================================
IF OBJECT_ID('silver.crm_cust_info','U') IS NOT NULL
    DROP TABLE silver.crm_cust_info;
GO

CREATE TABLE silver.crm_cust_info (
    cst_id              INT,             -- Surrogate Customer ID
    cst_key             NVARCHAR(50),    -- Business key from CRM
    cst_firstname       NVARCHAR(50),    -- Customer First Name
    cst_lastname        NVARCHAR(50),    -- Customer Last Name
    cst_marital_status  NVARCHAR(50),    -- Marital Status
    cst_gndr            NVARCHAR(50),    -- Gender
    cst_create_date     DATE,            -- Customer creation date in CRM
    dwh_create_date     DATETIME2 DEFAULT GETDATE() -- DWH record creation timestamp
);
GO


-- ============================================
-- CRM Product Information (Silver Layer)
-- ============================================
IF OBJECT_ID('silver.crm_prd_info','U') IS NOT NULL
    DROP TABLE silver.crm_prd_info;
GO

CREATE TABLE silver.crm_prd_info (
    prd_id          INT,             -- Surrogate Product ID
    cat_id          NVARCHAR(52),    -- Derived Category Key (split from product key)
    prd_key         NVARCHAR(50),    -- Product business key from CRM
    prd_nm          NVARCHAR(50),    -- Product Name
    prd_cost        INT,             -- Product Cost
    prd_line        NVARCHAR(50),    -- Product Line / Category
    prd_start_dt    DATE,            -- Product Start Date (valid from)
    prd_end_dt      DATE,            -- Product End Date (valid to)
    dwh_create_date DATETIME2 DEFAULT GETDATE() -- DWH record creation timestamp
);
GO


-- ============================================
-- CRM Sales Details (Silver Layer)
-- ============================================
IF OBJECT_ID('silver.crm_sales_details','U') IS NOT NULL
    DROP TABLE silver.crm_sales_details;
GO

CREATE TABLE silver.crm_sales_details (
    sls_ord_num     NVARCHAR(50),    -- Sales Order Number
    sls_prd_key     NVARCHAR(50),    -- Product Key (FK to Product)
    sls_cust_id     INT,             -- Customer ID (FK to Customer)
    sls_order_dt    DATE,            -- Order Date
    sls_ship_dt     DATE,            -- Shipment Date
    sls_due_dt      DATE,            -- Due Date
    sls_sales       INT,             -- Total Sales Amount
    sls_quantity    INT,             -- Quantity Sold
    sls_price       INT,             -- Price per Unit
    dwh_create_date DATETIME2 DEFAULT GETDATE() -- DWH record creation timestamp
);
GO


-- ============================================
-- ERP Location Information (Silver Layer)
-- ============================================
IF OBJECT_ID('silver.erp_loc_a101','U') IS NOT NULL
    DROP TABLE silver.erp_loc_a101;
GO

CREATE TABLE silver.erp_loc_a101 (
    cid             NVARCHAR(50),    -- Customer/Location ID
    cntry           NVARCHAR(50),    -- Country Name
    dwh_create_date DATETIME2 DEFAULT GETDATE() -- DWH record creation timestamp
);
GO


-- ============================================
-- ERP Customer Information (Silver Layer)
-- ============================================
IF OBJECT_ID('silver.erp_cust_az12','U') IS NOT NULL
    DROP TABLE silver.erp_cust_az12;
GO

CREATE TABLE silver.erp_cust_az12 (
    cid             VARCHAR(50),     -- Customer ID
    bdate           DATE,            -- Birth Date
    gen             NVARCHAR(50),    -- Gender
    dwh_create_date DATETIME2 DEFAULT GETDATE() -- DWH record creation timestamp
);
GO


-- ============================================
-- ERP Product Category Information (Silver Layer)
-- ============================================
IF OBJECT_ID('silver.erp_px_cat_g1V2','U') IS NOT NULL
    DROP TABLE silver.erp_px_cat_g1V2;
GO

CREATE TABLE silver.erp_px_cat_g1V2 (
    id              NVARCHAR(50),    -- Category ID
    cat             NVARCHAR(50),    -- Main Category
    subcat          NVARCHAR(50),    -- Sub-Category
    maintenance     NVARCHAR(50),    -- Maintenance Info/Status
    dwh_create_date DATETIME2 DEFAULT GETDATE() -- DWH record creation timestamp
);
GO
