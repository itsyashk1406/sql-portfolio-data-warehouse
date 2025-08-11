/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
===============================================================================
*/

IF OBJECT_ID('gold.dim_customers','V') IS NOT NULL
	DROP VIEW gold.dim_customers
GO

CREATE VIEW gold.dim_customers AS
	SELECT ROW_NUMBER() OVER(Order by cust_id) as customer_key
		  ,ci.[cust_id] as customer_id
		  ,ci.[cust_key] as customer_number
		  ,ci.[cust_firstname] as first_name
		  ,ci.[cust_lastname] as last_name
		  ,ci.[cust_marital_status] as marital_status
		  ,CASE WHEN ci.cust_gender!='n/a' THEN ci.cust_gender
			ELSE COALESCE(ca.gen,'n/a') 
			END as cust_gender
		  ,ca.[bdate] as birthdate
		  ,cl.[country] as country
		  ,ci.[cust_create_date] as create_date
	  FROM [silver].[crm_cust_info] as ci
	  LEFT JOIN silver.erp_cust_az12 as ca	ON ci.cust_key=ca.cid
	  LEFT JOIN silver.erp_loc_a101 as cl	ON ci.cust_key=cl.cid

GO
IF OBJECT_ID('gold.dim_products','V') IS NOT NULL
	DROP VIEW gold.dim_products
GO

CREATE VIEW gold.dim_products AS
	SELECT ROW_NUMBER() OVER(ORDER BY prd_start_date , prd_key) as product_key
		  ,pi.[prd_id] as product_id
		  ,pi.[prd_key] as product_number
		  ,pi.[prd_nm] as product_name
		  ,pi.[cat_id] as category_id
		  ,pc.cat as category
		  ,pc.subcat as subcategory
		  ,pi.[prd_line] as product_line
		  ,pi.[prd_cost] as	cost
		  ,pi.[prd_start_date] as product_start_date
		  ,pc.maintenance as maintenance
	  FROM .[silver].[crm_prd_info] pi
	  LEFT JOIN silver.erp_px_cat_g1v2 as pc ON pi.cat_id=pc.id
	  WHERE prd_end_date IS NULL --Filtering historical data to keep only current info

GO
IF OBJECT_ID('gold.fact_sales','V') IS NOT NULL
	DROP VIEW gold.fact_sales
GO

CREATE VIEW gold.fact_sales AS
	SELECT
		  --Keys
		  sd.[sales_ord_num] as order_number
		  ,c.customer_key
		  ,p.product_key
		  --Dates
		  ,sd.[sales_order_date] as order_date
		  ,sd.[sales_ship_date] as ship_date
		  ,sd.[sales_due_date] as due_date
		  --Measure
		  ,sd.[sales_sales] as sales_amount
		  ,sd.[sales_quantity] as quantity
		  ,sd.[sales_price] as price
	  FROM [silver].[crm_sales_details] as sd
	  LEFT JOIN gold.dim_customers as c ON sd.sales_cust_id=c.customer_id 
	  LEFT JOIN gold.dim_products as p ON sd.sales_prd_key=p.product_number
