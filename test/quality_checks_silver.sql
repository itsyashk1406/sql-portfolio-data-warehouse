/*
===============================================================================
Quality Checks
===============================================================================
Script Purpose:
    This script performs various quality checks for data consistency, accuracy, 
    and standardization across the 'silver' layer. It includes checks for:
    - Null or duplicate primary keys.
    - Unwanted spaces in string fields.
    - Data standardization and consistency.
    - Invalid date ranges and orders.
    - Data consistency between related fields.

Usage Notes:
    - Run these checks after data loading Silver Layer.
    - Investigate and resolve any discrepancies found during the checks.
===============================================================================
*/

--Check for nulls in primary key
SELECT prd_id FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*)>1 OR prd_id IS NULL

--CHECK for whitespaces
SELECT * FROM bronze.crm_prd_info 
WHERE prd_line!=TRIM(prd_line)

--Standardization & Consistency
SELECT DISTINCT gen FROM silver.erp_cust_az12

--Check for Invalid Date Orders (eg. Start date > End Date)
SELECT prd_key,prd_start_date,prd_end_date FROM bronze.crm_prd_info
WHERE prd_end_date<prd_start_date
OR prd_start_date IS NULL
OR prd_end_date IS NULL
;
SELECT prd_key,CAST(prd_start_date AS DATE) AS prd_start_date,prd_end_date,
CAST((LEAD(prd_start_date,1) OVER(PARTITION BY prd_key ORDER BY prd_start_date) -1) AS DATE)  as prd_end_date
FROM bronze.crm_prd_info;

SELECT *
FROM bronze.crm_sales_details
WHERE sales_order_date <=0
OR LEN(sales_order_date)!=8
OR sales_order_date<19000101
OR sales_order_date>20500101

--Check for Data Integrity
SELECT * FROM bronze.erp_px_cat_g1v2 WHERE id NOT IN (
SELECT DISTINCT cat_id FROM silver.crm_prd_info)

SELECT *,CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid))
ELSE cid 
END as cid
FROM bronze.erp_cust_az12 
WHERE CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid))
ELSE cid 
END NOT IN 
(SELECT cust_key FROM 
silver.crm_cust_info)

--Data Enrichment
SELECT * ,ISNULL(prd_cost,0) as prd_cost FROM bronze.crm_prd_info WHERE
SUBSTRING (prd_key ,7,LEN(prd_key))NOT  IN (
SELECT DISTINCT sales_prd_key FROM bronze.crm_sales_details)

SELECT DISTINCT SUBSTRING(prd_key,1,5) FROM bronze.crm_prd_info
WHERE  REPLACE(SUBSTRING(prd_key,1,5),'-','_') NOT IN (
SELECT DISTINCT id FROM bronze.erp_px_cat_g1v2)

--Errors in Buisness Logic
SELECT * FROM silver.crm_sales_details
WHERE sales_sales<=0 or sales_sales IS NULL or sales_sales != sales_quantity* sales_price
OR sales_quantity<=0 or sales_quantity IS NULL 
OR sales_price<=0 or sales_price IS NULL

SELECT * FROM bronze.erp_px_cat_g1v2
SELECT * FROM silver.crm_prd_info