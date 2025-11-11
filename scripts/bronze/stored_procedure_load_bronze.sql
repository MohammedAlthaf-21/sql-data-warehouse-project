CREATE OR ALTER PROCEDURE bronze.load_bronze AS 
Begin
	declare @start_time datetime, @end_time datetime
	--use DataWarehouse;
	set @start_time = getdate();
	print 'Truncating table: bronze_crm_cust_info';
	truncate table bronze.crm_cust_info
	print 'Loading data into table: bronze_crm_cust_info';
	bulk insert bronze.crm_cust_info
	from 'C:\Temp\cust_info.csv'
	with(
		firstrow = 2,
		fieldterminator =',',
		rowterminator = '\n',
		tablock
	);
	set @end_time = getdate();
	print ' load duration:' + cast(datediff(second, @start_time, @end_time) AS nvarchar) +' seconds';
	print '===================================================';

	print 'Truncating table: bronze_crm_prd_info';
	truncate table bronze.crm_prd_info
	print 'Loading data into table: bronze_crm_prd_info';
	bulk insert bronze.crm_prd_info
	from 'C:\Temp\prd_info.csv'
	with(
		firstrow = 2,
		fieldterminator =',',
		rowterminator = '\n',
		tablock
	);

	truncate table bronze.crm_sales_details
	bulk insert bronze.crm_sales_details
	from 'C:\Temp\sales_details.csv'
	with(
		firstrow = 2,
		fieldterminator =',',
		rowterminator = '\n',
		tablock
	);

	truncate table bronze.erp_cust_az12
	bulk insert bronze.erp_cust_az12
	from 'C:\Users\Mohammed Anwar\Desktop\Data Analytics\sql project\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_erp\cust_az12.csv'
	with(
		firstrow = 2,
		fieldterminator =',',
		rowterminator = '\n',
		tablock
	);
	
	truncate table bronze.erp_loc_a101
	bulk insert bronze.erp_loc_a101
	from 'C:\Users\Mohammed Anwar\Desktop\Data Analytics\sql project\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_erp\loc_a101.csv'
	with(
		firstrow = 2,
		fieldterminator =',',
		rowterminator = '\n',
		tablock
	);

	truncate table bronze.erp_px_cat_g1v2
	bulk insert bronze.erp_px_cat_g1v2
	from 'C:\Temp\px_cat_g1v2.csv'
	with(
		firstrow = 2,
		fieldterminator =',',
		rowterminator = '\n',
		tablock
	);



END
