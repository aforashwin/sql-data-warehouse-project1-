exec silver.load_silver

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
    DECLARE 
        @start_time DATETIME,
        @end_time DATETIME,
        @batch_start_time DATETIME,
        @batch_end_time DATETIME;

    BEGIN TRY
        SET @batch_start_time = GETDATE();
        PRINT '==============================================';
        PRINT 'Starting Silver Layer Load Procedure';
        PRINT '==============================================';

        -- Load CRM Customer Info
        PRINT 'Truncating table silver.crm_cust_info';
        TRUNCATE TABLE silver.crm_cust_info;

        SET @start_time = GETDATE();

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
            TRIM(cst_firstname) AS first_name,
            TRIM(cst_lastname) AS last_name,
            CASE 
                WHEN UPPER(cst_marital_status) = 'M' THEN 'MARRIED'
                WHEN UPPER(cst_marital_status) = 'S' THEN 'SINGLE'
                ELSE 'N/A'
            END AS cst_marital_status,
            CASE 
                WHEN UPPER(cst_gndr) = 'M' THEN 'MALE'
                WHEN UPPER(cst_gndr) = 'F' THEN 'FEMALE'
                ELSE 'N/A'
            END AS cst_gndr,
            cst_create_date
        FROM (
            SELECT
                *,
                ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
            FROM bronze.crm_cust_info
            WHERE cst_id IS NOT NULL
        ) t
        WHERE flag_last = 1;

        SET @end_time = GETDATE();
        PRINT ' >> silver.crm_cust_info loaded, duration: ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + ' seconds';

        -- Load Product Info
        PRINT 'Truncating table silver.crm_prd_info';
        TRUNCATE TABLE silver.crm_prd_info;

        SET @start_time = GETDATE();

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
            REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
            SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
            prd_nm,
            ISNULL(prd_cost, 0) AS prd_cost, 
            CASE UPPER(TRIM(prd_line)) 
                WHEN 'M' THEN 'mountain'
                WHEN 'R' THEN 'road'
                WHEN 's' THEN 'other sales'
                WHEN 'T' THEN 'touring'
                ELSE 'n/a'
            END AS prd_line,
            CAST(prd_start_dt AS DATE) AS prd_start_dt,
            CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1 AS DATE) AS prd_end_dt
        FROM datawarehouse.bronze.crm_prd_info;

        SET @end_time = GETDATE();
        PRINT ' >> silver.crm_prd_info loaded, duration: ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + ' seconds';

        -- Load Sales Details
        PRINT 'Truncating table silver.crm_sales_details';
        TRUNCATE TABLE silver.crm_sales_details;

        SET @start_time = GETDATE();

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
            CASE 
                WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
                ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
            END AS sls_order_dt,
            CASE 
                WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
                ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
            END AS sls_ship_dt,
            CASE 
                WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
                ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
            END AS sls_due_dt,
            CASE 
                WHEN sls_sales = 0 OR sls_sales IS NULL OR sls_sales != sls_quantity * ABS(sls_price)
                THEN sls_quantity * ABS(sls_price)
                ELSE sls_sales
            END AS sls_sales,
            sls_quantity,
            CASE 
                WHEN sls_price = 0 OR sls_price IS NULL
                THEN sls_sales / NULLIF(sls_quantity, 0)
                ELSE sls_price
            END AS sls_price
        FROM datawarehouse.bronze.crm_sales_details;

        SET @end_time = GETDATE();
        PRINT ' >> silver.crm_sales_details loaded, duration: ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + ' seconds';

        -- Load ERP Customer AZ12
        PRINT 'Truncating table silver.erp_cust_az12';
        TRUNCATE TABLE silver.erp_cust_az12;

        SET @start_time = GETDATE();

        INSERT INTO silver.erp_cust_az12 (cid, bdate, gen)
        SELECT
            CASE 
                WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
                ELSE cid
            END AS cid,
            CASE 
                WHEN bdate > GETDATE() THEN NULL
                ELSE bdate
            END AS bdate,
            CASE 
                WHEN UPPER(TRIM(gen)) IN ('F','Female') THEN 'female'
                WHEN UPPER(TRIM(gen)) IN ('M','male') THEN 'male'
                ELSE 'n/a'
            END AS gen
        FROM bronze.erp_cust_az12;

        SET @end_time = GETDATE();
        PRINT ' >> silver.erp_cust_az12 loaded, duration: ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + ' seconds';

        -- Load ERP Location
        PRINT 'Truncating table silver.erp_loc_a101';
        TRUNCATE TABLE silver.erp_loc_a101;

        SET @start_time = GETDATE();

        INSERT INTO silver.erp_loc_a101 (cid, cntry)
        SELECT 
            REPLACE(cid, '-', '') AS cid,
            CASE 
                WHEN TRIM(cntry) = 'DE' THEN 'germany'
                WHEN TRIM(cntry) IN ('us', 'usa') THEN 'united states'
                WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
                ELSE TRIM(cntry)
            END AS cntry
        FROM bronze.erp_loc_a101
        ORDER BY cntry;

        SET @end_time = GETDATE();
        PRINT ' >> silver.erp_loc_a101 loaded, duration: ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + ' seconds';

        -- Load ERP PX Category
        PRINT 'Truncating table silver.erp_px_cat_g1v2';
        TRUNCATE TABLE silver.erp_px_cat_g1v2;

        SET @start_time = GETDATE();

        INSERT INTO silver.erp_px_cat_g1v2 (id, cat, subcat, maintenance)
        SELECT *
        FROM bronze.erp_px_cat_g1v2;

        SET @end_time = GETDATE();
        PRINT ' >> silver.erp_px_cat_g1v2 loaded, duration: ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + ' seconds';

        SET @batch_end_time = GETDATE();
        PRINT '==============================================';
        PRINT 'Silver Layer Load Completed Successfully';
        PRINT 'Total Load Duration: ' + CAST(DATEDIFF(SECOND,@batch_start_time,@batch_end_time) AS NVARCHAR) + ' seconds';
        PRINT '==============================================';

    END TRY
    BEGIN CATCH
        PRINT '==============================================';
        PRINT 'Error occurred during Silver Layer Load';
        PRINT 'Error message: ' + ERROR_MESSAGE();
        PRINT 'Error number: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'Error state: ' + CAST(ERROR_STATE() AS NVARCHAR);
        PRINT '==============================================';
    END CATCH

END
