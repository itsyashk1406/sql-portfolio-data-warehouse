/*
===============================================================================
DDL Script: Create Silver Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'silver' schema, dropping existing tables 
    if they already exist.
	Run this script to re-define the DDL structure of 'silver' Tables
===============================================================================
*/


IF OBJECT_ID('silver.crm_cust_info', 'U') IS NOT NULL
    DROP TABLE silver.crm_cust_info;
GO
CREATE TABLE silver.crm_cust_info (
    cust_id              INT,
    cust_key             NVARCHAR(50),
    cust_firstname       NVARCHAR(50),
    cust_lastname        NVARCHAR(50),
    cust_marital_status  NVARCHAR(50),
    cust_gender            NVARCHAR(50),
    cust_create_date     DATE ,
	dwh_create_date		DATETIME2 DEFAULT GETDATE()
);
GO

IF OBJECT_ID('silver.crm_prd_info', 'U') IS NOT NULL
    DROP TABLE silver.crm_prd_info;
GO
CREATE TABLE silver.crm_prd_info (
    prd_id       INT,
	cat_id		 NVARCHAR(50),
    prd_key      NVARCHAR(50),
    prd_nm       NVARCHAR(50),
    prd_cost     INT,
    prd_line     NVARCHAR(50),
    prd_start_date DATE,
    prd_end_date   DATE,
	dwh_create_date		DATETIME2 DEFAULT GETDATE()
);
GO

IF OBJECT_ID('silver.crm_sales_details', 'U') IS NOT NULL
    DROP TABLE silver.crm_sales_details;
GO

CREATE TABLE silver.crm_sales_details (
    sales_ord_num  NVARCHAR(50),
    sales_prd_key  NVARCHAR(50),
    sales_cust_id  INT,
    sales_order_date DATE,
    sales_ship_date  DATE,
    sales_due_date  DATE,
    sales_sales    INT,
    sales_quantity INT,
    sales_price    INT,
	dwh_create_date		DATETIME2 DEFAULT GETDATE()
);
GO

IF OBJECT_ID('silver.erp_loc_a101', 'U') IS NOT NULL
    DROP TABLE silver.erp_loc_a101;
GO
CREATE TABLE silver.erp_loc_a101 (
    cid    NVARCHAR(50),
    country  NVARCHAR(50),
	dwh_create_date		DATETIME2 DEFAULT GETDATE()
);
GO

IF OBJECT_ID('silver.erp_cust_az12', 'U') IS NOT NULL
    DROP TABLE silver.erp_cust_az12;
GO
CREATE TABLE silver.erp_cust_az12 (
    cid    NVARCHAR(50),
    bdate  DATE,
    gen    NVARCHAR(50),
	dwh_create_date		DATETIME2 DEFAULT GETDATE()
);
GO

IF OBJECT_ID('silver.erp_px_cat_g1v2', 'U') IS NOT NULL
    DROP TABLE silver.erp_px_cat_g1v2;
GO

CREATE TABLE silver.erp_px_cat_g1v2 (
    id           NVARCHAR(50),
    cat          NVARCHAR(50),
    subcat       NVARCHAR(50),
    maintenance  NVARCHAR(50),
	dwh_create_date		DATETIME2 DEFAULT GETDATE()
);
GO
