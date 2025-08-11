/*
===============================================================================
Quality Checks
===============================================================================
Script Purpose:
    This script performs quality checks to validate the integrity, consistency, 
    and accuracy of the Gold Layer. These checks ensure:
    - Uniqueness of surrogate keys in dimension tables.
    - Referential integrity between fact and dimension tables.
    - Validation of relationships in the data model for analytical purposes.

===============================================================================
*/

--Dimension Customers 

--Duplicates may occurr do to joining
--Check for duplicates in primary key after JOining
SELECT cust_id,COUNT(*) FROM
  silver.crm_cust_info ci
  LEFT JOIN silver.erp_cust_az12 as ca	ON ci.cust_key=ca.cid
  LEFT JOIN silver.erp_loc_a101 as cl	ON ci.cust_key=cl.cid
  GROUP BY cust_id 
  HAVING COUNT(*)>1

--Two gender columns for customers having different values
--Assume CRM as master table for the customer's data
SELECT DISTINCT
ci.cust_gender,
ca.gen,
CASE WHEN ci.cust_gender!='n/a' THEN ci.cust_gender
	ELSE COALESCE(ca.gen,'n/a') 
END as cust_gender
  FROM [silver].[crm_cust_info] as ci
  LEFT JOIN silver.erp_cust_az12 as ca	ON ci.cust_key=ca.cid
  LEFT JOIN silver.erp_loc_a101 as cl	ON ci.cust_key=cl.cid


--Dimension Products

--Check duplicats in primary key after joining
SELECT prd_key ,COUNT(*) FROM (
SELECT TOP (1000) pi.[prd_id]
      ,pi.[prd_key]
      ,pi.[prd_nm]
      ,pi.[cat_id]
	  ,pc.cat
      ,pc.subcat
      ,pi.[prd_line]
      ,pi.[prd_cost]
      ,pi.[prd_start_date]
      ,pi.[prd_end_date]
	  ,pc.maintenance
  FROM .[silver].[crm_prd_info] pi
  LEFT JOIN silver.erp_px_cat_g1v2 as pc ON pi.cat_id=pc.id
  WHERE prd_end_date IS NULL --Filtering historical data to keep only current info
  )t 
  GROUP BY prd_key
  HAVING COUNT(*) >1

--Fact Sales

--Foreign Key Integrity (Dimensions)
SELECT * FROM gold.fact_sales s
LEFT JOIN gold.dim_customers c
ON s.customer_key =c.customer_key
LEFT JOIN gold.dim_products p 
ON p.product_key=s.product_key
WHERE c.customer_key IS NULL OR p.product_key IS NULL

