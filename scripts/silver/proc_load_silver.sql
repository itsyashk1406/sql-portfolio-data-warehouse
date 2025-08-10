/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
	
	Data Cleansing Steps Performed:
		- Check for nulls/duplicates in the primary key & update with the latest one using Row_Number
		- TRIM the text columns 
		- Data Standardization & Consistency with CASE 
		- Check for correct datatype (set at DDL)
		- Invalid Date orders (eg. end_date<start_date)
		- Handle nulls/negative values
		- Check for failures in Business Logic like sales!=price*quantity
		- Data Enrichment ( deriving new columns from exisiting data)

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC Silver.load_silver;
===============================================================================
*/


CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN 
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME
	BEGIN TRY
	SET @batch_start_time = GETDATE();
		PRINT '================================================';
		PRINT 'Loading Silver Layer';
		PRINT '================================================';
		PRINT '                                                ';
		PRINT '------------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '------------------------------------------------';
		PRINT '                                                ';

		SET @start_time = GETDATE();
			PRINT '>>Truncating Table: silver.crm_cust_info';
			TRUNCATE TABLE silver.crm_cust_info

			PRINT '>>Inserting Data into: silver.crm_cust_info';
			INSERT INTO silver.crm_cust_info (cust_id , cust_key , cust_firstname , cust_lastname , cust_marital_status , cust_gender , cust_create_date)

				SELECT cust_id ,
				TRIM(cust_key) as cust_key,
				TRIM(cust_firstname) as cust_firstname,
				TRIM(cust_lastname) as cust_lastname,
				CASE WHEN TRIM(UPPER(cust_marital_status))='M' THEN 'Married'
					WHEN TRIM(UPPER(cust_marital_status))='S' THEN 'Single'
					ELSE 'n/a'
					END as cust_marital_status,
				CASE WHEN TRIM(UPPER(cust_gender))='M' THEN 'Male'
					WHEN TRIM(UPPER(cust_gender))='F' THEN 'Female'
					ELSE 'n/a'
					END as cust_gender,
				cust_create_date
				FROM 
				(
				SELECT *,ROW_NUMBER() OVER(PARTITION BY cust_id ORDER BY cust_create_date DESC) as flag_last
				FROM bronze.crm_cust_info
				WHERE cust_id IS NOT NULL
				)t WHERE flag_last=1
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

		SET @start_time = GETDATE();
			PRINT '>>Truncating Table: silver.crm_prd_info';
			TRUNCATE TABLE silver.crm_prd_info

			PRINT '>>Inserting Data into: silver.crm_prd_info';
			INSERT INTO silver.crm_prd_info (prd_id,cat_id,prd_key,prd_nm,prd_cost ,prd_line,prd_start_date,prd_end_date)
				SELECT prd_id ,
				REPLACE(SUBSTRING(prd_key,1,5),'-','_') as cat_id ,
				SUBSTRING (prd_key ,7,LEN(prd_key) ) as prd_key ,
				TRIM(prd_nm) as prd_nm ,
				ISNULL(prd_cost,0) as prd_cost,
				CASE WHEN UPPER(TRIM(prd_line)) ='M' THEN 'Mountain'
					WHEN UPPER(TRIM(prd_line))='R' THEN 'Road'
					WHEN UPPER(TRIM(prd_line))='S' THEN 'Other Sales'
					WHEN UPPER(TRIM(prd_line))='T' THEN 'Touring'
					ELSE 'n/a'
				END as prd_line,
				CAST(prd_start_date AS DATE) as prd_start_date,
				CAST(LEAD(prd_start_date,1) OVER(PARTITION BY prd_key ORDER BY prd_start_date)-1 AS DATE)  as prd_end_date
	
				FROM bronze.crm_prd_info
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

		SET @start_time = GETDATE();
			PRINT '>>Truncating Table:silver.crm_sales_details';
			TRUNCATE TABLE silver.crm_sales_details
	
			PRINT '>>Inserting Data into: silver.crm_sales_details';
			INSERT INTO silver.crm_sales_details(sales_ord_num,sales_prd_key,sales_cust_id,sales_order_date,sales_ship_date,sales_due_date
			,sales_sales,sales_quantity,sales_price)

				SELECT sales_ord_num,
				sales_prd_key,
				sales_cust_id,
				CASE WHEN sales_order_date=0 OR LEN(sales_order_date)!=8 OR sales_order_date>20500101 OR sales_order_date<19000101 THEN NULL
					ELSE CAST(CAST(sales_order_date AS VARCHAR) AS DATE) 
				END AS sales_order_date, --order_date
				CASE WHEN sales_ship_date=0 OR LEN(sales_ship_date)!=8  OR sales_ship_date>20500101 OR sales_ship_date<19000101 THEN NULL
					ELSE CAST(CAST(sales_ship_date AS VARCHAR) AS DATE) 
				END as sales_ship_date, --ship_date
				CASE WHEN sales_due_date=0 OR LEN(sales_due_date)!=8 OR sales_due_date>20500101 OR sales_due_date <19000101 THEN NULL
					ELSE CAST(CAST(sales_due_date AS VARCHAR) AS DATE) 
				END as sales_due_date, --due_date
	
				CASE WHEN sales_sales<=0 OR sales_sales IS NULL OR sales_sales!=sales_quantity*sales_price
					THEN sales_quantity*ABS(sales_price)
					ELSE sales_sales
				END as sales_sales, --sales
				CASE WHEN sales_quantity<=0 OR sales_quantity IS NULL
					THEN ABS(sales_sales)/ABS(sales_price)
					ELSE sales_quantity
				END as sales_quantity, --quantity
				CASE WHEN sales_price<=0 OR sales_price IS NULL
					THEN ABS(sales_sales)/NULLIF(ABS(sales_quantity),0)
					ELSE sales_price
				END as sales_price --price

				FROM bronze.crm_sales_details
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

		PRINT '------------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '------------------------------------------------';
		PRINT '												   ';

		SET @start_time = GETDATE();
			PRINT '>>Truncating Table: silver.erp_cust_az12';
			TRUNCATE TABLE silver.erp_cust_az12
	
			PRINT '>>Inserting Data into: silver.erp_cust_az12';
			INSERT INTO silver.erp_cust_az12(cid,bdate,gen)

				SELECT 
				CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid))
					ELSE cid 
				END as cid,
				CASE WHEN bdate >GETDATE() THEN NULL
					ELSE bdate
				END as bdate,
				CASE WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'Male'
					WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'
					ELSE 'n/a'
				END as gen
	
				FROM bronze.erp_cust_az12	
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

		SET @start_time = GETDATE();
			PRINT '>>Truncating Table: silver.erp_loc_a101';
			TRUNCATE TABLE silver.erp_loc_a101
	
			PRINT '>>Inserting Data into: ssilver.erp_loc_a101';
			INSERT INTO silver.erp_loc_a101(cid,country)

				SELECT 
				REPLACE(cid,'-','') as cid ,
				CASE WHEN TRIM(country) IN ('US','USA') THEN 'United States'
					WHEN TRIM(country)='DE' THEN 'Germany'
					WHEN TRIM(country)='' OR country IS NULL THEN 'n/a'
					ELSE TRIM(country)
					END as country
	
				FROM bronze.erp_loc_a101	
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

		SET @start_time = GETDATE();
			PRINT '>>Truncating Table: silver.erp_px_cat_g1v2';
			TRUNCATE TABLE silver.erp_px_cat_g1v2
	
			PRINT '>>Inserting Data into: silver.erp_px_cat_g1v2';
			INSERT INTO silver.erp_px_cat_g1v2(id,cat,subcat,maintenance)

				SELECT 
				id ,
				TRIM(cat) as cat,
				TRIM(subcat) as subcat,
				TRIM(maintenance) as maintenance
	
				FROM bronze.erp_px_cat_g1v2
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

	SET @batch_end_time = GETDATE();
		PRINT '=========================================='
		PRINT 'Loading Silver Layer is Completed';
        PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '=========================================='
	END TRY
	BEGIN CATCH
	PRINT '=========================================='
		PRINT 'ERROR OCCURED DURING LOADING Silver LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '=========================================='
	END CATCH
END
