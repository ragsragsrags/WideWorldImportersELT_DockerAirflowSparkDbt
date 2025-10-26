USE [master]
GO
/****** Object:  Database [WideWorldImportersDW]    Script Date: 10/13/2025 10:24:08 PM ******/
CREATE DATABASE [WideWorldImportersDW]
 CONTAINMENT = NONE
 ON  PRIMARY 
( NAME = N'WideWorldImportersDW', FILENAME = N'C:\Program Files\Microsoft SQL Server\MSSQL16.MSSQLSERVER05\MSSQL\DATA\WideWorldImportersDW.mdf' , SIZE = 401408KB , MAXSIZE = UNLIMITED, FILEGROWTH = 65536KB )
 LOG ON 
( NAME = N'WideWorldImportersDW_log', FILENAME = N'C:\Program Files\Microsoft SQL Server\MSSQL16.MSSQLSERVER05\MSSQL\DATA\WideWorldImportersDW_log.ldf' , SIZE = 401408KB , MAXSIZE = 2048GB , FILEGROWTH = 65536KB )
 WITH CATALOG_COLLATION = DATABASE_DEFAULT, LEDGER = OFF
GO
ALTER DATABASE [WideWorldImportersDW] SET COMPATIBILITY_LEVEL = 160
GO
IF (1 = FULLTEXTSERVICEPROPERTY('IsFullTextInstalled'))
begin
EXEC [WideWorldImportersDW].[dbo].[sp_fulltext_database] @action = 'enable'
end
GO
ALTER DATABASE [WideWorldImportersDW] SET ANSI_NULL_DEFAULT OFF 
GO
ALTER DATABASE [WideWorldImportersDW] SET ANSI_NULLS OFF 
GO
ALTER DATABASE [WideWorldImportersDW] SET ANSI_PADDING OFF 
GO
ALTER DATABASE [WideWorldImportersDW] SET ANSI_WARNINGS OFF 
GO
ALTER DATABASE [WideWorldImportersDW] SET ARITHABORT OFF 
GO
ALTER DATABASE [WideWorldImportersDW] SET AUTO_CLOSE OFF 
GO
ALTER DATABASE [WideWorldImportersDW] SET AUTO_SHRINK OFF 
GO
ALTER DATABASE [WideWorldImportersDW] SET AUTO_UPDATE_STATISTICS ON 
GO
ALTER DATABASE [WideWorldImportersDW] SET CURSOR_CLOSE_ON_COMMIT OFF 
GO
ALTER DATABASE [WideWorldImportersDW] SET CURSOR_DEFAULT  GLOBAL 
GO
ALTER DATABASE [WideWorldImportersDW] SET CONCAT_NULL_YIELDS_NULL OFF 
GO
ALTER DATABASE [WideWorldImportersDW] SET NUMERIC_ROUNDABORT OFF 
GO
ALTER DATABASE [WideWorldImportersDW] SET QUOTED_IDENTIFIER OFF 
GO
ALTER DATABASE [WideWorldImportersDW] SET RECURSIVE_TRIGGERS OFF 
GO
ALTER DATABASE [WideWorldImportersDW] SET  DISABLE_BROKER 
GO
ALTER DATABASE [WideWorldImportersDW] SET AUTO_UPDATE_STATISTICS_ASYNC OFF 
GO
ALTER DATABASE [WideWorldImportersDW] SET DATE_CORRELATION_OPTIMIZATION OFF 
GO
ALTER DATABASE [WideWorldImportersDW] SET TRUSTWORTHY OFF 
GO
ALTER DATABASE [WideWorldImportersDW] SET ALLOW_SNAPSHOT_ISOLATION OFF 
GO
ALTER DATABASE [WideWorldImportersDW] SET PARAMETERIZATION SIMPLE 
GO
ALTER DATABASE [WideWorldImportersDW] SET READ_COMMITTED_SNAPSHOT OFF 
GO
ALTER DATABASE [WideWorldImportersDW] SET HONOR_BROKER_PRIORITY OFF 
GO
ALTER DATABASE [WideWorldImportersDW] SET RECOVERY FULL 
GO
ALTER DATABASE [WideWorldImportersDW] SET  MULTI_USER 
GO
ALTER DATABASE [WideWorldImportersDW] SET PAGE_VERIFY CHECKSUM  
GO
ALTER DATABASE [WideWorldImportersDW] SET DB_CHAINING OFF 
GO
ALTER DATABASE [WideWorldImportersDW] SET FILESTREAM( NON_TRANSACTED_ACCESS = OFF ) 
GO
ALTER DATABASE [WideWorldImportersDW] SET TARGET_RECOVERY_TIME = 60 SECONDS 
GO
ALTER DATABASE [WideWorldImportersDW] SET DELAYED_DURABILITY = DISABLED 
GO
ALTER DATABASE [WideWorldImportersDW] SET ACCELERATED_DATABASE_RECOVERY = OFF  
GO
EXEC sys.sp_db_vardecimal_storage_format N'WideWorldImportersDW', N'ON'
GO
ALTER DATABASE [WideWorldImportersDW] SET QUERY_STORE = ON
GO
ALTER DATABASE [WideWorldImportersDW] SET QUERY_STORE (OPERATION_MODE = READ_WRITE, CLEANUP_POLICY = (STALE_QUERY_THRESHOLD_DAYS = 30), DATA_FLUSH_INTERVAL_SECONDS = 900, INTERVAL_LENGTH_MINUTES = 60, MAX_STORAGE_SIZE_MB = 1000, QUERY_CAPTURE_MODE = AUTO, SIZE_BASED_CLEANUP_MODE = AUTO, MAX_PLANS_PER_QUERY = 200, WAIT_STATS_CAPTURE_MODE = ON)
GO
USE [WideWorldImportersDW]
GO
/****** Object:  Table [dbo].[DataValidationErrors]    Script Date: 10/13/2025 10:24:08 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[DataValidationErrors](
	[CutoffDate] [datetime2](7) NOT NULL,
	[Error] [nvarchar](200) NOT NULL,
	[Details] [nvarchar](1000) NOT NULL,
	[Schema] [nvarchar](50) NOT NULL,
	[Table] [nvarchar](50) NOT NULL,
	[Process] [nvarchar](50) NOT NULL
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[LoadHistory]    Script Date: 10/13/2025 10:24:08 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[LoadHistory](
	[TableName] [nvarchar](50) NOT NULL,
	[LoadDate] [datetime] NOT NULL,
	[Status] [nvarchar](50) NULL,
	[Details] [nvarchar](max) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [dbo].[WarehouseHistory]    Script Date: 10/13/2025 10:24:08 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[WarehouseHistory](
	[TableName] [nvarchar](50) NOT NULL,
	[LoadDate] [datetime] NOT NULL,
	[Status] [nvarchar](50) NULL,
	[Details] [nvarchar](max) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  StoredProcedure [dbo].[ProcessDimCity]    Script Date: 10/13/2025 10:24:08 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO







/*

EXEC [dbo].[ProcessDimCity] '12/31/2012', '1/1/2013'

*/
CREATE PROCEDURE [dbo].[ProcessDimCity]
(
	@InitialLoad DATETIME,
	@NewCutoff DATETIME
)
WITH EXECUTE AS OWNER
AS
BEGIN

    SET NOCOUNT ON;
    SET XACT_ABORT ON;

	IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[CityKey]') AND type = 'SO')
	BEGIN

		CREATE SEQUENCE [dbo].[CityKey] 
		AS [int]
		START WITH 1
		INCREMENT BY 1
		MINVALUE -2147483648
		MAXVALUE 2147483647
		CACHE 

	END

	IF OBJECT_ID (N'DimCity', N'U') IS NULL 
	BEGIN

		CREATE TABLE [dbo].[DimCity]
		(
			[CityKey] [int] NOT NULL,
			[WWICityID] [int] NOT NULL,
			[City] [nvarchar](50) NOT NULL,
			[StateProvince] [nvarchar](50) NOT NULL,
			[Country] [nvarchar](60) NOT NULL,
			[Continent] [nvarchar](30) NOT NULL,
			[SalesTerritory] [nvarchar](50) NOT NULL,
			[Region] [nvarchar](30) NOT NULL,
			[Subregion] [nvarchar](30) NOT NULL,
			[Location] [geography] NULL,
			[LatestRecordedPopulation] [bigint] NOT NULL,
			[LoadDate] [datetime2](7) NOT NULL,
			CONSTRAINT [PK_DimCity] PRIMARY KEY CLUSTERED 
			(
				[CityKey] ASC
			) WITH 
			(
				STATISTICS_NORECOMPUTE = OFF, 
				IGNORE_DUP_KEY = OFF, 
				OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF
			) ON [PRIMARY]
		) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]

		ALTER TABLE [dbo].[DimCity] ADD  CONSTRAINT [DF_DimCity_CityKey]  DEFAULT (NEXT VALUE FOR [dbo].[CityKey]) FOR [CityKey]

	END



	DECLARE @LastCutoff DATETIME

	IF OBJECT_ID('tempdb..#DimCity', 'U') IS NOT NULL
		DROP TABLE #DimCity

	SELECT TOP 1
		@LastCutoff = WH.LoadDate
	FROM
		dbo.WarehouseHistory WH
	WHERE
		WH.Status = 'Successful' AND
		TableName = 'DimCity'
	ORDER BY
		WH.LoadDate DESC

	SET @LastCutoff = ISNULL(@LastCutoff, @InitialLoad)

	BEGIN TRAN

		;WITH StateProvincesChanged AS
		(

			SELECT
				SP.StateProvinceID
			FROM
				Application_StateProvinces SP
			WHERE
				SP.ValidFrom > @LastCutoff AND
				@NewCutoff BETWEEN SP.ValidFrom AND SP.ValidTo

			UNION ALL

			SELECT
				SPA.StateProvinceID
			FROM
				Application_StateProvinces_Archive SPA
			WHERE
				SPA.ValidFrom > @LastCutoff AND
				@NewCutoff BETWEEN SPA.ValidFrom AND SPA.ValidTo 

		),

		CountriesChanged AS
		(

			SELECT
				C.CountryID
			FROM
				Application_Countries C
			WHERE
				C.ValidFrom > @LastCutoff AND
				@NewCutoff BETWEEN C.ValidFrom AND C.ValidTo

			UNION ALL

			SELECT
				CA.CountryID
			FROM
				Application_Countries CA
			WHERE
				CA.ValidFrom > @LastCutoff AND
				@NewCutoff BETWEEN CA.ValidFrom AND CA.ValidTo

		),

		StateProvincesAvailable AS (

			SELECT
				SP.StateProvinceID,
				SP.CountryID,
				SP.StateProvinceName,
				SP.SalesTerritory
			FROM
				Application_StateProvinces SP
			WHERE
				@NewCutoff BETWEEN ValidFrom AND ValidTo 

			UNION

			SELECT
				SPA.StateProvinceID,
				SPA.CountryID,
				SPA.StateProvinceName,
				SPA.SalesTerritory
			FROM
				Application_StateProvinces_Archive SPA
			WHERE
				@NewCutoff BETWEEN ValidFrom AND ValidTo

		),

		CountriesAvailable AS (

			SELECT
				C.CountryID,
				C.CountryName,
				C.Continent,
				C.Region,
				C.Subregion
			FROM
				Application_Countries C
			WHERE
				@NewCutoff BETWEEN ValidFrom AND ValidTo

			UNION

			SELECT
				CA.CountryID,
				CA.CountryName,
				CA.Continent,
				CA.Region,
				CA.Subregion
			FROM
				Application_Countries_Archive CA
			WHERE
				@NewCutoff BETWEEN ValidFrom AND ValidTo 

		),

		Final AS (

			SELECT 
				[WWICityID] = C.CityID,
				[City] = C.CityName,
				[StateProvince] = SPA.StateProvinceName,
				[Country] = CA.CountryName,
				CA.Continent,
				SPA.SalesTerritory,
				CA.Region,
				CA.Subregion,
				C.Location,
				[LatestRecordedPopulation] = ISNULL(C.LatestRecordedPopulation, 0) 
			FROM 
				Application_Cities C LEFT JOIN
				StateProvincesAvailable SPA ON
					SPA.StateProvinceID = C.StateProvinceID LEFT JOIN
				CountriesAvailable CA ON
					CA.CountryID = SPA.CountryID
			WHERE
				(
					C.ValidFrom > @LastCutoff OR
					SPA.StateProvinceID IN 
					(
						SELECT
							SPC.StateProvinceID
						FROM
							StateProvincesChanged SPC
					) OR
					CA.CountryID IN 
					(
						SELECT
							CC2.CountryID
						FROM
							CountriesChanged CC2
					)
				) AND
				@NewCutoff BETWEEN C.ValidFrom AND C.ValidTo

			UNION ALL

			SELECT 
				C.CityID,
				C.CityName,
				SPA.StateProvinceName,
				CA.CountryName,
				CA.Continent,
				SPA.SalesTerritory,
				CA.Region,
				CA.Subregion,
				C.Location,
				ISNULL(C.LatestRecordedPopulation, 0)
			FROM 
				Application_Cities_Archive C LEFT JOIN
				StateProvincesAvailable SPA ON
					SPA.StateProvinceID = C.StateProvinceID LEFT JOIN
				CountriesAvailable CA ON
					CA.CountryID = SPA.CountryID
			WHERE
				(
					C.ValidFrom > @LastCutoff OR
					SPA.StateProvinceID IN 
					(
						SELECT
							SPC.StateProvinceID
						FROM
							StateProvincesChanged SPC
					) OR
					CA.CountryID IN 
					(
						SELECT
							CC2.CountryID
						FROM
							CountriesChanged CC2
					)
				) AND
				@NewCutoff BETWEEN C.ValidFrom AND C.ValidTo 

		)

		SELECT 
			*
		INTO
			#DimCity
		FROM
			Final

		-- Update Existing
		UPDATE
			C2
		SET
			C2.City = C.City,
			C2.StateProvince = C.StateProvince,
			C2.Country = C.Country,
			C2.Continent = C.Continent,
			C2.SalesTerritory = C.SalesTerritory,
			C2.Region = C.Region,
			C2.Subregion = C.Subregion,
			C2.Location = C.Location,
			C2.LatestRecordedPopulation = C.LatestRecordedPopulation,
			C2.LoadDate = @NewCutoff
		FROM
			#DimCity C JOIN
			dbo.DimCity C2 ON
				C2.WWICityID = C.WWICityID

		-- Insert New
		INSERT INTO dbo.DimCity
		(
			WWICityID,
			City,
			StateProvince,
			Country,
			Continent,
			SalesTerritory,
			Region,
			Subregion,
			Location,
			LatestRecordedPopulation,
			LoadDate
		)
		SELECT
			C.WWICityID,
			C.City,
			C.StateProvince,
			C.Country,
			C.Continent,
			C.SalesTerritory,
			C.Region,
			C.Subregion,
			C.Location,
			C.LatestRecordedPopulation,
			@NewCutoff
		FROM
			#DimCity C LEFT JOIN
			dbo.DimCity C2 ON
				C2.WWICityID = C.WWICityID
		WHERE
			C2.WWICityID IS NULL
		ORDER BY
			C.WWICityID

		IF NOT EXISTS
		(
			SELECT
				1
			FROM
				dbo.DimCity C
			WHERE
				C.CityKey = 0
		)
		BEGIN
	
			INSERT INTO dbo.DimCity
			(
				[CityKey],
				[WWICityID], 
				[City], 
				[StateProvince], 
				[Country], 
				[Continent],
				[SalesTerritory], 
				[Region], 
				[Subregion], 
				[Location],
				[LatestRecordedPopulation], 
				[LoadDate]
			)
			VALUES
			(
				0,
				0,
				'Unknown',
				'N/A',
				'N/A',
				'N/A',
				'N/A',
				'N/A',
				'N/A',
				NULL,
				0,
				@NewCutoff
			)
	
		END

		INSERT INTO dbo.WarehouseHistory
		(
			TableName,
			LoadDate,
			Status,
			Details
		)
		VALUES
		(
			'DimCity',
			@NewCutoff,
			'Successful',
			NULL
		)

	COMMIT TRAN

	IF OBJECT_ID('tempdb..#DimCity', 'U') IS NOT NULL
		DROP TABLE #DimCity

END;
GO
/****** Object:  StoredProcedure [dbo].[ProcessDimCustomer]    Script Date: 10/13/2025 10:24:08 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

/*

EXEC [dbo].[ProcessDimCustomer] '12/31/2012', '1/1/2013'

*/
CREATE PROCEDURE [dbo].[ProcessDimCustomer]
(
	@InitialLoad DATETIME,
	@NewCutoff DATETIME
)
WITH EXECUTE AS OWNER
AS
BEGIN

    SET NOCOUNT ON;
    SET XACT_ABORT ON;

	IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[CustomerKey]') AND type = 'SO')
	BEGIN

		CREATE SEQUENCE [dbo].[CustomerKey] 
		AS [int]
		START WITH 1
		INCREMENT BY 1
		MINVALUE -2147483648
		MAXVALUE 2147483647
		CACHE 

	END

	IF OBJECT_ID (N'DimCustomer', N'U') IS NULL 
	BEGIN

		CREATE TABLE [dbo].[DimCustomer]
		(
			[CustomerKey] [int] NOT NULL,
			[WWICustomerID] [int] NOT NULL,
			[WWIDeliveryCityID] [int] NOT NULL,
			[Customer] [nvarchar](100) NOT NULL,
			[BillToCustomer] [nvarchar](100) NOT NULL,
			[Category] [nvarchar](50) NOT NULL,
			[BuyingGroup] [nvarchar](50) NOT NULL,
			[PrimaryContact] [nvarchar](50) NOT NULL,
			[PostalCode] [nvarchar](10) NOT NULL,
			[LoadDate] [datetime2](7) NOT NULL,
			CONSTRAINT [PK_DimCustomer] PRIMARY KEY CLUSTERED 
			(
				[CustomerKey] ASC
			)
			WITH 
			(
				STATISTICS_NORECOMPUTE = OFF, 
				IGNORE_DUP_KEY = OFF, 
				OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF
			) ON [PRIMARY]
		) ON [PRIMARY]

		ALTER TABLE [dbo].[DimCustomer] ADD  CONSTRAINT [DF_DimCustomer_CustomerKey]  DEFAULT (NEXT VALUE FOR [dbo].[CustomerKey]) FOR [CustomerKey]

	END

	DECLARE @LastCutoff DATETIME

	IF OBJECT_ID('tempdb..#DimCustomer', 'U') IS NOT NULL
		DROP TABLE #DimCustomer

	SELECT TOP 1
		@LastCutoff = WH.LoadDate
	FROM
		dbo.WarehouseHistory WH
	WHERE
		WH.Status = 'Successful' AND
		TableName = 'DimCustomer'
	ORDER BY
		WH.LoadDate DESC

	SET @LastCutoff = ISNULL(@LastCutoff, @InitialLoad)

	BEGIN TRAN

		;WITH BuyingGroupsChanged AS (

			SELECT
				BG.BuyingGroupID
			FROM
				dbo.Sales_BuyingGroups BG
			WHERE
				(
					BG.ValidFrom > @LastCutoff OR
					(
						@NewCutoff = @InitialLoad AND
						BG.ValidFrom >= @LastCutoff
					)
				) AND
				@NewCutoff BETWEEN BG.ValidFrom AND BG.ValidTo 

			UNION

			SELECT
				BGA.BuyingGroupID
			FROM
				dbo.Sales_BuyingGroups_Archive BGA
			WHERE
				(
					BGA.ValidFrom > @LastCutoff OR
					(
						@NewCutoff = @InitialLoad AND
						BGA.ValidFrom >= @LastCutoff
					)
				) AND
				@NewCutoff BETWEEN BGA.ValidFrom AND BGA.ValidTo 

		),

		CustomerCategoriesChanged AS (

			SELECT
				CC.CustomerCategoryID
			FROM
				dbo.Sales_CustomerCategories CC
			WHERE
				(
					CC.ValidFrom > @LastCutoff OR
					(
						@NewCutoff = @InitialLoad AND
						CC.ValidFrom >= @LastCutoff
					)
				) AND
				@NewCutoff BETWEEN CC.ValidFrom AND CC.ValidTo 

			UNION

			SELECT
				CCA.CustomerCategoryID
			FROM
				dbo.Sales_CustomerCategories_Archive CCA
			WHERE
				(
					CCA.ValidFrom > @LastCutoff OR
					(
						@NewCutoff = @InitialLoad AND
						CCA.ValidFrom >= @LastCutoff
					)
				) AND
				@NewCutoff BETWEEN CCA.ValidFrom AND CCA.ValidTo 

		),

		PrimaryContactsChanged AS (

			SELECT
				P.PersonID
			FROM
				dbo.Application_People P 
			WHERE
				(
					P.ValidFrom > @LastCutoff OR
					(
						@NewCutoff = @InitialLoad AND
						P.ValidFrom >= @NewCutoff
					)
				) AND
				@NewCutoff BETWEEN P.ValidFrom AND P.ValidTo 

			UNION

			SELECT
				PA.PersonID
			FROM
				dbo.Application_People_Archive PA
			WHERE
				(
					PA.ValidFrom > @LastCutoff OR
					(
						@NewCutoff = @InitialLoad AND
						PA.ValidFrom >= @NewCutoff
					)
				) AND
				@NewCutoff BETWEEN PA.ValidFrom AND PA.ValidTo 

		),

		CustomersAvailable AS 
		(

			SELECT
				C.CustomerID,
				C.CustomerName
			FROM
				dbo.Sales_Customers C
			WHERE
				@NewCutoff BETWEEN C.ValidFrom AND C.ValidTo

			UNION

			SELECT
				CA.CustomerID,
				CA.CustomerName
			FROM
				dbo.Sales_Customers_Archive CA
			WHERE
				@NewCutoff BETWEEN CA.ValidFrom AND CA.ValidTo

		),

		CustomerCategoriesAvailable AS 
		(

			SELECT
				CC.CustomerCategoryID,
				CC.CustomerCategoryName
			FROM
				dbo.Sales_CustomerCategories CC
			WHERE
				@NewCutoff BETWEEN CC.ValidFrom AND CC.ValidTo

			UNION

			SELECT
				CCA.CustomerCategoryID,
				CCA.CustomerCategoryName
			FROM
				dbo.Sales_CustomerCategories_Archive CCA
			WHERE
				@NewCutoff BETWEEN CCA.ValidFrom AND CCA.ValidTo

		),

		BuyingGroupsAvailable AS 
		(

			SELECT
				BG.BuyingGroupID,
				BG.BuyingGroupName
			FROM
				dbo.Sales_BuyingGroups BG
			WHERE
				@NewCutoff BETWEEN BG.ValidFrom AND BG.ValidTo

			UNION

			SELECT
				BGA.BuyingGroupID,
				BGA.BuyingGroupName
			FROM
				dbo.Sales_BuyingGroups_Archive BGA
			WHERE
				@NewCutoff BETWEEN BGA.ValidFrom AND BGA.ValidTo

		),

		PrimaryContactsAvailable AS 
		(

			SELECT
				P.PersonID,
				P.FullName
			FROM
				dbo.Application_People P
			WHERE
				@NewCutoff BETWEEN P.ValidFrom AND P.ValidTo

			UNION

			SELECT
				PA.PersonID,
				PA.FullName
			FROM
				dbo.Application_People_Archive PA
			WHERE
				@NewCutoff BETWEEN PA.ValidFrom AND PA.ValidTo

		),

		Final AS (

			SELECT
				[WWICustomerID] = C.CustomerID,
				[WWIDeliveryCityID] = ISNULL(C.DeliveryCityID, 0),
				[Customer] = C.CustomerName,
				[BillToCustomer] = CA.CustomerName,
				[Category] = CCA.CustomerCategoryName,
				[BuyingGroup] = ISNULL(BGA.BuyingGroupName, ''),
				[PrimaryContact] = PCA.FullName,
				[PostalCode] = C.DeliveryPostalCode
			FROM
				dbo.Sales_Customers C LEFT JOIN
				CustomersAvailable CA ON
					CA.CustomerID = C.BillToCustomerID LEFT JOIN
				CustomerCategoriesAvailable CCA ON
					CCA.CustomerCategoryID = C.CustomerCategoryID LEFT JOIN
				BuyingGroupsAvailable BGA ON
					BGA.BuyingGroupID = C.BuyingGroupID LEFT JOIN
				PrimaryContactsAvailable PCA ON
					PCA.PersonID = C.PrimaryContactPersonID
			WHERE
				(
					C.ValidFrom > @LastCutoff OR
					(
						@NewCutoff = @InitialLoad AND
						C.ValidFrom >= @LastCutoff
					) OR
					C.BuyingGroupID IN
					(
						SELECT
							BGC.BuyingGroupID
						FROM
							BuyingGroupsChanged BGC
                
					) OR
					C.CustomerCategoryID IN
					(
						SELECT
							CCC.CustomerCategoryID
						FROM
							CustomerCategoriesChanged CCC
					) OR
					C.PrimaryContactPersonID IN
					(
						SELECT
							PCC.PersonID
						FROM
							PrimaryContactsChanged PCC
					)
				) AND
				@NewCutoff BETWEEN C.ValidFrom AND C.ValidTo

			UNION ALL

			SELECT
				C.CustomerID,
				ISNULL(C.DeliveryCityID, 0),
				C.CustomerName,
				CA.CustomerName,
				CCA.CustomerCategoryName,
				ISNULL(BGA.BuyingGroupName, ''),
				PCA.FullName,
				C.DeliveryPostalCode
			FROM
				dbo.Sales_Customers_Archive C LEFT JOIN
				CustomersAvailable CA ON
					CA.CustomerID = C.BillToCustomerID LEFT JOIN
				CustomerCategoriesAvailable CCA ON
					CCA.CustomerCategoryID = C.CustomerCategoryID LEFT JOIN
				BuyingGroupsAvailable BGA ON
					BGA.BuyingGroupID = C.BuyingGroupID LEFT JOIN
				PrimaryContactsAvailable PCA ON
					PCA.PersonID = C.PrimaryContactPersonID
			WHERE
				(
					C.ValidFrom > @LastCutoff OR
					(
						@NewCutoff = @InitialLoad AND
						C.ValidFrom >= @LastCutoff
					) OR
					C.BuyingGroupID IN
					(
						SELECT
							BGC.BuyingGroupID
						FROM
							BuyingGroupsChanged BGC
                
					) OR
					C.CustomerCategoryID IN
					(
						SELECT
							CCC.CustomerCategoryID
						FROM
							CustomerCategoriesChanged CCC
					) OR
					C.PrimaryContactPersonID IN
					(
						SELECT
							PCC.PersonID
						FROM
							PrimaryContactsChanged PCC
					)
				) AND
				@NewCutoff BETWEEN C.ValidFrom AND C.ValidTo 

		)

		SELECT
			*
		INTO
			#DimCustomer
		FROM
			Final

		-- Update Existing
		UPDATE
			C2
		SET
			C2.WWIDeliveryCityID = C.WWIDeliveryCityID,
			C2.Customer = C.Customer,
			C2.BillToCustomer = C.BillToCustomer,
			C2.Category = C.Category,
			C2.BuyingGroup = C.BuyingGroup,
			C2.PrimaryContact = C.PrimaryContact,
			C2.PostalCode = C.PostalCode,
			C2.LoadDate = @NewCutoff
		FROM
			#DimCustomer C JOIN
			dbo.DimCustomer C2 ON
				C2.WWICustomerID = C.WWICustomerID

		-- Insert New
		INSERT INTO dbo.DimCustomer
		(
			WWICustomerID,
			WWIDeliveryCityID,
			Customer,
			BillToCustomer,
			Category,
			BuyingGroup,
			PrimaryContact,
			PostalCode,
			LoadDate
		)
		SELECT
			C.WWICustomerID,
			C.WWIDeliveryCityID,
			C.Customer,
			C.BillToCustomer,
			C.Category,
			C.BuyingGroup,
			C.PrimaryContact,
			C.PostalCode,
			@NewCutoff
		FROM
			#DimCustomer C LEFT JOIN
			dbo.DimCustomer C2 ON
				C2.WWICustomerID = C.WWICustomerID
		WHERE
			C2.WWICustomerID IS NULL
		ORDER BY
			C.WWICustomerID

		IF NOT EXISTS
		(
			SELECT
				1
			FROM
				dbo.DimCustomer C
			WHERE
				C.[CustomerKey] = 0
		)
		BEGIN

			INSERT dbo.DimCustomer
			(
				[CustomerKey],
				[WWICustomerID], 
				[WWIDeliveryCityID], 
				[Customer], 
				[BillToCustomer], 
				[Category],
				[BuyingGroup], 
				[PrimaryContact], 
				[PostalCode], 
				[LoadDate]
			)
			VALUES
			(
				0,
				0,
				0,
				'Unknown',
				'N/A',
				'N/A',
				'N/A',
				'N/A',
				'N/A',
				@NewCutoff
			)

		END

		INSERT INTO dbo.WarehouseHistory
		(
			TableName,
			LoadDate,
			Status,
			Details
		)
		VALUES
		(
			'DimCustomer',
			@NewCutoff,
			'Successful',
			NULL
		)

	COMMIT TRAN

	IF OBJECT_ID('tempdb..#DimCustomer', 'U') IS NOT NULL
		DROP TABLE #DimCustomer

END;
GO
/****** Object:  StoredProcedure [dbo].[ProcessDimDate]    Script Date: 10/13/2025 10:24:08 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


/*

EXEC [dbo].[ProcessDimDate] '12/31/2012', '1/1/2013'

*/
CREATE PROCEDURE [dbo].[ProcessDimDate]
(
	@InitialLoad DATETIME,
	@NewCutoff DATETIME
)
WITH EXECUTE AS OWNER
AS
BEGIN

    SET NOCOUNT ON;
    SET XACT_ABORT ON;

	IF OBJECT_ID (N'DimDate', N'U') IS NULL 
	BEGIN

		CREATE TABLE [dbo].[DimDate]
		(
			[Date] [date] NOT NULL,
			[DayNumber] [int] NOT NULL,
			[Day] [nvarchar](10) NOT NULL,
			[Month] [nvarchar](10) NOT NULL,
			[ShortMonth] [nvarchar](3) NOT NULL,
			[CalendarMonthNumber] [int] NOT NULL,
			[CalendarMonthLabel] [nvarchar](20) NOT NULL,
			[CalendarYear] [int] NOT NULL,
			[CalendarYearLabel] [nvarchar](10) NOT NULL,
			[FiscalMonthNumber] [int] NOT NULL,
			[FiscalMonthLabel] [nvarchar](20) NOT NULL,
			[FiscalYear] [int] NOT NULL,
			[FiscalYearLabel] [nvarchar](10) NOT NULL,
			[ISOWeekNumber] [int] NOT NULL,
			[LoadDate] [datetime2](7) NOT NULL,
			CONSTRAINT [PK_DimDate] PRIMARY KEY CLUSTERED 
			(
				[Date] ASC
			)
			WITH 
			(
				STATISTICS_NORECOMPUTE = OFF, 
				IGNORE_DUP_KEY = OFF, 
				OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF
			) ON [PRIMARY]
		) ON [PRIMARY]

	END

	DECLARE @LastCutoff DATETIME

	SELECT TOP 1
		@LastCutoff = CAST(WH.LoadDate AS DATE)
	FROM
		dbo.WarehouseHistory WH
	WHERE
		WH.Status = 'Successful' AND
		TableName = 'DimDate'
	ORDER BY
		WH.LoadDate DESC

	SET @LastCutoff = ISNULL(@LastCutoff, @InitialLoad)

	BEGIN TRAN

		DECLARE @Date DATE = CAST(@LastCutoff AS DATE) 

		WHILE (
			DATEADD(DAY, 1, @Date) <= CAST(DATEADD(DAY, 30, @NewCutoff) AS DATE) OR
			@Date = @InitialLoad 
		)
		BEGIN

			IF @Date = @InitialLoad 
				IF NOT EXISTS (SELECT 1 FROM dbo.DimDate WHERE Date = @Date)
					INSERT INTO dbo.DimDate
					(
						[Date],
						[DayNumber],
						[Day],
						[Month],
						[ShortMonth],
						[CalendarMonthNumber],
						[CalendarMonthLabel],
						[CalendarYear],
						[CalendarYearLabel],
						[FiscalMonthNumber],
						[FiscalMonthLabel],
						[FiscalYear],
						[FiscalYearLabel],
						[ISOWeekNumber],
						[LoadDate]
					)
					VALUES
					(
						@Date,
						DAY(@Date),
						CAST(DATENAME(DAY, @Date) AS NVARCHAR(10)),
						CAST(DATENAME(MONTH, @Date) AS NVARCHAR(10)),
						CAST(SUBSTRING(DATENAME(MONTH, @Date), 1, 3) AS NVARCHAR(3)),
						MONTH(@Date),
						CAST(N'CY' + CAST(YEAR(@Date) AS NVARCHAR(4)) + N'-' + SUBSTRING(DATENAME(MONTH, @Date), 1, 3) AS nvarchar(10)),
						YEAR(@Date),
						CAST(N'CY' + CAST(YEAR(@Date) AS nvarchar(4)) AS nvarchar(10)),
						CASE WHEN MONTH(@Date) IN (11, 12)
							THEN MONTH(@Date) - 10
							ELSE MONTH(@Date) + 2
						END,
						CAST(N'FY' + CAST(CASE WHEN MONTH(@Date) IN (11, 12)
							THEN YEAR(@Date) + 1
							ELSE YEAR(@Date)
						END AS NVARCHAR(4)) + N'-' + SUBSTRING(DATENAME(month, @Date), 1, 3) AS nvarchar(20)),
						CASE WHEN MONTH(@Date) IN (11, 12)
							THEN YEAR(@Date) + 1
							ELSE YEAR(@Date)
						END,
						CAST(N'FY' + CAST(CASE WHEN MONTH(@Date) IN (11, 12)
							THEN YEAR(@Date) + 1
							ELSE YEAR(@Date)
						END AS NVARCHAR(4)) AS NVARCHAR(10)),
						DATEPART(ISO_WEEK, @Date),
						@NewCutoff
					)

			SET @Date = DATEADD(DAY, 1, @Date)

			IF CAST(@NewCutoff AS DATE) > @InitialLoad
				IF NOT EXISTS (SELECT 1 FROM dbo.DimDate WHERE Date = @Date)
					INSERT INTO dbo.DimDate
					(
						[Date],
						[DayNumber],
						[Day],
						[Month],
						[ShortMonth],
						[CalendarMonthNumber],
						[CalendarMonthLabel],
						[CalendarYear],
						[CalendarYearLabel],
						[FiscalMonthNumber],
						[FiscalMonthLabel],
						[FiscalYear],
						[FiscalYearLabel],
						[ISOWeekNumber],
						[LoadDate]
					)

					VALUES
					(
						@Date,
						DAY(@Date),
						CAST(DATENAME(DAY, @Date) AS NVARCHAR(10)),
						CAST(DATENAME(MONTH, @Date) AS NVARCHAR(10)),
						CAST(SUBSTRING(DATENAME(MONTH, @Date), 1, 3) AS NVARCHAR(3)),
						MONTH(@Date),
						CAST(N'CY' + CAST(YEAR(@Date) AS NVARCHAR(4)) + N'-' + SUBSTRING(DATENAME(MONTH, @Date), 1, 3) AS nvarchar(10)),
						YEAR(@Date),
						CAST(N'CY' + CAST(YEAR(@Date) AS nvarchar(4)) AS nvarchar(10)),
						CASE WHEN MONTH(@Date) IN (11, 12)
							THEN MONTH(@Date) - 10
							ELSE MONTH(@Date) + 2
						END,
						CAST(N'FY' + CAST(CASE WHEN MONTH(@Date) IN (11, 12)
												THEN YEAR(@Date) + 1
												ELSE YEAR(@Date)
										END AS NVARCHAR(4)) + N'-' + SUBSTRING(DATENAME(month, @Date), 1, 3) AS nvarchar(20)),
						CASE WHEN MONTH(@Date) IN (11, 12)
							THEN YEAR(@Date) + 1
							ELSE YEAR(@Date)
						END,
						CAST(N'FY' + CAST(CASE WHEN MONTH(@Date) IN (11, 12)
												THEN YEAR(@Date) + 1
												ELSE YEAR(@Date)
										END AS NVARCHAR(4)) AS NVARCHAR(10)),
						DATEPART(ISO_WEEK, @Date),
						@NewCutoff
					)

		END

	COMMIT TRAN

END;
GO
/****** Object:  StoredProcedure [dbo].[ProcessDimEmployee]    Script Date: 10/13/2025 10:24:08 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO







/*

EXEC [dbo].[ProcessDimEmployee] '12/31/2012', '1/1/2013'

*/
CREATE PROCEDURE [dbo].[ProcessDimEmployee]
(
	@InitialLoad DATETIME,
	@NewCutoff DATETIME
)
WITH EXECUTE AS OWNER
AS
BEGIN

    SET NOCOUNT ON;
    SET XACT_ABORT ON;

	IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[EmployeeKey]') AND type = 'SO')
	BEGIN

		CREATE SEQUENCE [dbo].[EmployeeKey] 
		AS [int]
		START WITH 1
		INCREMENT BY 1
		MINVALUE -2147483648
		MAXVALUE 2147483647
		CACHE 

	END

	IF OBJECT_ID (N'DimEmployee', N'U') IS NULL 
	BEGIN

		CREATE TABLE [dbo].[DimEmployee]
		(
			[EmployeeKey] [int] NOT NULL,
			[WWIEmployeeID] [int] NOT NULL,
			[Employee] [nvarchar](50) NOT NULL,
			[PreferredName] [nvarchar](50) NOT NULL,
			[IsSalesperson] [bit] NOT NULL,
			[Photo] [varbinary](max) NULL,
			[LoadDate] [datetime2](7) NOT NULL,
			CONSTRAINT [PK_DimEmployee] PRIMARY KEY CLUSTERED 
			(
				[EmployeeKey] ASC
			)
			WITH 
			(
				STATISTICS_NORECOMPUTE = OFF, 
				IGNORE_DUP_KEY = OFF, 
				OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF
			) ON [PRIMARY]
		) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
		
		ALTER TABLE [dbo].[DimEmployee] ADD  CONSTRAINT [DF_DimEmployee_EmployeeKey]  DEFAULT (NEXT VALUE FOR [dbo].[EmployeeKey]) FOR [EmployeeKey]

	END

	DECLARE @LastCutoff DATETIME

	IF OBJECT_ID('tempdb..#DimEmployee', 'U') IS NOT NULL
		DROP TABLE #DimEmployee
	
	SELECT TOP 1
		@LastCutoff = WH.LoadDate
	FROM
		dbo.WarehouseHistory WH
	WHERE
		WH.Status = 'Successful' AND
		TableName = 'DimEmployee'
	ORDER BY
		WH.LoadDate DESC

	SET @LastCutoff = ISNULL(@LastCutoff, @InitialLoad)

	BEGIN TRAN

		;WITH Final AS 
		(

			SELECT
				[WWIEmployeeID] = P.PersonID,
				[Employee] = P.FullName,
				[PreferredName] = P.PreferredName,
				[IsSalesPerson] = P.IsSalesperson,
				[Photo] = P.Photo
			FROM
				dbo.Application_People P 
			WHERE
				P.IsEmployee = 1 AND
				P.ValidFrom > @LastCutoff AND
				@NewCutoff BETWEEN P.ValidFrom AND P.ValidTo 

			UNION ALL

			SELECT
				P.PersonID,
				P.FullName,
				P.PreferredName,
				P.IsSalesperson,
				P.Photo
			FROM
				dbo.Application_People_Archive P 
			WHERE
				P.IsEmployee = 1 AND
				P.ValidFrom > @LastCutoff AND
				@NewCutoff BETWEEN P.ValidFrom AND P.ValidTo 

		)

		SELECT 
			*
		INTO
			#DimEmployee
		FROM
			Final

		-- Update Existing
		UPDATE
			E2
		SET
			E2.Employee = E.Employee,
			E2.PreferredName = E.PreferredName,
			E2.IsSalesPerson = E.IsSalesPerson,
			E2.Photo = E.Photo,
			E2.LoadDate = @NewCutoff
		FROM
			#DimEmployee E JOIN
			dbo.DimEmployee E2 ON
				E2.WWIEmployeeID = E.WWIEmployeeID

		-- Insert New
		INSERT INTO dbo.DimEmployee
		(
			WWIEmployeeID,
			Employee,
			PreferredName,
			IsSalesPerson,
			Photo,
			LoadDate
		)
		SELECT
			E.WWIEmployeeID,
			E.Employee,
			E.PreferredName,
			E.IsSalesPerson,
			E.Photo,
			@NewCutoff
		FROM
			#DimEmployee E LEFT JOIN
			dbo.DimEmployee E2 ON
				E2.WWIEmployeeID = E.WWIEmployeeID
		WHERE
			E2.WWIEmployeeID IS NULL
		ORDER BY
			E.WWIEmployeeID

		IF NOT EXISTS
		(
			SELECT
				1
			FROM
				dbo.DimEmployee E
			WHERE
				E.[EmployeeKey] = 0
		)
		BEGIN

			INSERT INTO dbo.DimEmployee
			(
				[EmployeeKey],
				[WWIEmployeeID],
				[Employee],
				[PreferredName],
				[IsSalesperson],
				[Photo],
				[LoadDate]
			)
			VALUES
			(
				0,
				0,
				'Unknown',
				'N/A',
				0,
				NULL,
				@NewCutoff
			)
	
		END

		INSERT INTO dbo.WarehouseHistory
		(
			TableName,
			LoadDate,
			Status,
			Details
		)
		VALUES
		(
			'DimEmployee',
			@NewCutoff,
			'Successful',
			NULL
		)

	COMMIT TRAN

	IF OBJECT_ID('tempdb..#DimEmployee', 'U') IS NOT NULL
		DROP TABLE #DimEmployee

END
GO
/****** Object:  StoredProcedure [dbo].[ProcessDimPaymentMethod]    Script Date: 10/13/2025 10:24:08 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO







/*

EXEC [dbo].[ProcessDimPaymentMethod] '12/31/2012', '1/1/2013'

*/
CREATE PROCEDURE [dbo].[ProcessDimPaymentMethod]
(
	@InitialLoad DATETIME,
	@NewCutoff DATETIME
)
WITH EXECUTE AS OWNER
AS
BEGIN

    SET NOCOUNT ON;
    SET XACT_ABORT ON;

	IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[PaymentMethodKey]') AND type = 'SO')
	BEGIN

		CREATE SEQUENCE [dbo].[PaymentMethodKey] 
		AS [int]
		START WITH 1
		INCREMENT BY 1
		MINVALUE -2147483648
		MAXVALUE 2147483647
		CACHE 

	END

	IF OBJECT_ID (N'DimPaymentMethod', N'U') IS NULL 
	BEGIN

		CREATE TABLE [dbo].[DimPaymentMethod]
		(
			[PaymentMethodKey] [int] NOT NULL,
			[WWIPaymentMethodID] [int] NOT NULL,
			[PaymentMethod] [nvarchar](50) NOT NULL,
			[LoadDate] [datetime2](7) NOT NULL,
			CONSTRAINT [PK_DimPaymentMethod] PRIMARY KEY CLUSTERED 
			(
				[PaymentMethodKey] ASC
			)
			WITH 
			(
				STATISTICS_NORECOMPUTE = OFF, 
				IGNORE_DUP_KEY = OFF, 
				OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF
			) ON [PRIMARY]
		) ON [PRIMARY]

		ALTER TABLE [dbo].[DimPaymentMethod] ADD  CONSTRAINT [DF_DimPaymentMethod_PaymentMethodKey]  DEFAULT (NEXT VALUE FOR [dbo].[PaymentMethodKey]) FOR [PaymentMethodKey]

	END

	DECLARE @LastCutoff DATETIME

	IF OBJECT_ID('tempdb..#DimPaymentMethod', 'U') IS NOT NULL
		DROP TABLE #DimPaymentMethod

	SELECT TOP 1
		@LastCutoff = WH.LoadDate
	FROM
		dbo.WarehouseHistory WH
	WHERE
		WH.Status = 'Successful' AND
		TableName = 'DimPaymentMethod'
	ORDER BY
		WH.LoadDate DESC

	SET @LastCutoff = ISNULL(@LastCutoff, @InitialLoad)

	BEGIN TRAN

		;WITH Final AS 
		(

			SELECT
				[WWIPaymentMethodID] = PM.PaymentMethodID,
				[PaymentMethod] = PM.PaymentMethodName
			FROM
				dbo.Application_PaymentMethods PM
			WHERE
				PM.ValidFrom > @LastCutoff AND
				@NewCutoff BETWEEN PM.ValidFrom AND PM.ValidTo 

			UNION ALL

			SELECT
				PM.PaymentMethodID,
				PM.PaymentMethodName
			FROM
				dbo.Application_PaymentMethods_Archive PM
			WHERE
				PM.ValidFrom > @LastCutoff AND
				@NewCutoff BETWEEN PM.ValidFrom AND PM.ValidTo 

		)


		SELECT 
			*
		INTO
			#DimPaymentMethod
		FROM
			Final
		ORDER BY
			WWIPaymentMethodID

		-- Update Existing
		UPDATE
			PM2
		SET
			PM2.PaymentMethod = PM.PaymentMethod,
			PM2.LoadDate = @NewCutoff
		FROM
			#DimPaymentMethod PM JOIN
			dbo.DimPaymentMethod PM2 ON
				PM2.WWIPaymentMethodID = PM.WWIPaymentMethodID

		-- Insert New
		INSERT INTO dbo.DimPaymentMethod
		(
			WWIPaymentMethodID,
			PaymentMethod,
			LoadDate
		)
		SELECT
			PM.WWIPaymentMethodID,
			PM.PaymentMethod,
			@NewCutoff
		FROM
			#DimPaymentMethod PM LEFT JOIN
			dbo.DimPaymentMethod PM2 ON
				PM2.WWIPaymentMethodID = PM.WWIPaymentMethodID
		WHERE
			PM2.WWIPaymentMethodID IS NULL
		ORDER BY
			PM.WWIPaymentMethodID

		IF NOT EXISTS
		(
			SELECT
				1
			FROM
				dbo.DimPaymentMethod PM
			WHERE
				PM.[PaymentMethodKey] = 0
		)
		BEGIN
		
			INSERT dbo.DimPaymentMethod
			(
				[PaymentMethodKey],
				[WWIPaymentMethodID], 
				[PaymentMethod], 
				[LoadDate]
			)
			VALUES
			(
				0,
				0,
				'Unknown',
				@NewCutoff
			)

		END

		INSERT INTO dbo.WarehouseHistory
		(
			TableName,
			LoadDate,
			Status,
			Details
		)
		VALUES
		(
			'DimPaymentMethod',
			@NewCutoff,
			'Successful',
			NULL
		)

	COMMIT TRAN

	IF OBJECT_ID('tempdb..#DimPaymentMethod', 'U') IS NOT NULL
		DROP TABLE #DimPaymentMethod

END
GO
/****** Object:  StoredProcedure [dbo].[ProcessDimStockItem]    Script Date: 10/13/2025 10:24:08 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO







/*

EXEC [dbo].[ProcessDimStockItem] '12/31/2012', '1/1/2013'

*/
CREATE PROCEDURE [dbo].[ProcessDimStockItem]
(
	@InitialLoad DATETIME,
	@NewCutoff DATETIME
)
WITH EXECUTE AS OWNER
AS
BEGIN

    SET NOCOUNT ON;
    SET XACT_ABORT ON;

	IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[StockItemKey]') AND type = 'SO')
	BEGIN

		CREATE SEQUENCE [dbo].[StockItemKey] 
		AS [int]
		START WITH 1
		INCREMENT BY 1
		MINVALUE -2147483648
		MAXVALUE 2147483647
		CACHE 

	END

	IF OBJECT_ID (N'DimStockItem', N'U') IS NULL 
	BEGIN

		CREATE TABLE [dbo].[DimStockItem]
		(
			[StockItemKey] [int] NOT NULL,
			[WWIStockItemID] [int] NOT NULL,
			[StockItem] [nvarchar](100) NOT NULL,
			[Color] [nvarchar](20) NOT NULL,
			[SellingPackage] [nvarchar](50) NOT NULL,
			[BuyingPackage] [nvarchar](50) NOT NULL,
			[Brand] [nvarchar](50) NOT NULL,
			[Size] [nvarchar](20) NOT NULL,
			[LeadTimeDays] [int] NOT NULL,
			[QuantityPerOuter] [int] NOT NULL,
			[IsChillerStock] [int] NOT NULL,
			[Barcode] [nvarchar](50) NULL,
			[TaxRate] [decimal](18, 3) NOT NULL,
			[UnitPrice] [decimal](18, 2) NOT NULL,
			[RecommendedRetailPrice] [decimal](18, 2) NULL,
			[TypicalWeightPerUnit] [decimal](18, 3) NOT NULL,
			[Photo] [varbinary](max) NULL,
			[LoadDate] [datetime2](7) NOT NULL,
			CONSTRAINT [PK_DimStockItem] PRIMARY KEY CLUSTERED 
			(
				[StockItemKey] ASC
			)
			WITH 
			(
				STATISTICS_NORECOMPUTE = OFF, 
				IGNORE_DUP_KEY = OFF, 
				OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF
			) ON [PRIMARY]
		) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
		
		ALTER TABLE [dbo].[DimStockItem] ADD  CONSTRAINT [DF_DimStockItem_StockItemKey]  DEFAULT (NEXT VALUE FOR [dbo].[StockItemKey]) FOR [StockItemKey]
		
	END

	DECLARE @LastCutoff DATETIME

	IF OBJECT_ID('tempdb..#DimStockItem', 'U') IS NOT NULL
		DROP TABLE #DimStockItem

	SELECT TOP 1
		@LastCutoff = WH.LoadDate
	FROM
		dbo.WarehouseHistory WH
	WHERE
		WH.Status = 'Successful' AND
		TableName = 'DimStockItem'
	ORDER BY
		WH.LoadDate DESC

	SET @LastCutoff = ISNULL(@LastCutoff, @InitialLoad)

	BEGIN TRAN

		;WITH PackageTypesChanged AS 
		(

			SELECT
				PT.PackageTypeID
			FROM
				dbo.Warehouse_PackageTypes PT
			WHERE
				PT.ValidFrom > @LastCutoff AND
				@NewCutoff BETWEEN PT.ValidFrom AND PT.ValidTo 

			UNION ALL

			SELECT
				PTA.PackageTypeID
			FROM
				dbo.Warehouse_PackageTypes_Archive PTA
			WHERE
				PTA.ValidFrom > @LastCutoff AND
				@NewCutoff BETWEEN PTA.ValidFrom AND PTA.ValidTo 

		),

		ColorsChanged AS (

			SELECT
				C.ColorID
			FROM
				dbo.Warehouse_Colors C
			WHERE
				C.ValidFrom > @LastCutoff AND
				@NewCutoff BETWEEN C.ValidFrom AND C.ValidTo 

			UNION ALL

			SELECT
				CA.ColorID
			FROM
				dbo.Warehouse_Colors_Archive CA
			WHERE
				CA.ValidFrom > @LastCutoff AND
				@NewCutoff BETWEEN CA.ValidFrom AND CA.ValidTo 

		),

		PackageTypesAvailable AS (

			SELECT
				PT.PackageTypeID,
				PT.PackageTypeName
			FROM
				dbo.Warehouse_PackageTypes PT
			WHERE
				@NewCutoff BETWEEN PT.ValidFrom AND PT.ValidTo

			UNION

			SELECT
				PTA.PackageTypeID,
				PTA.PackageTypeName
			FROM
				dbo.Warehouse_PackageTypes_Archive PTA
			WHERE
				@NewCutoff BETWEEN PTA.ValidFrom AND PTA.ValidTo

		),

		ColorsAvailable AS (

			SELECT
				C.ColorID,
				C.ColorName
			FROM
				dbo.Warehouse_Colors C
			WHERE
				@NewCutoff BETWEEN C.ValidFrom AND C.ValidTo

			UNION

			SELECT
				CA.ColorID,
				CA.ColorName
			FROM
				dbo.Warehouse_Colors_Archive CA
			WHERE
				@NewCutoff BETWEEN CA.ValidFrom AND CA.ValidTo

		),

		StockitemsChanged AS (

			SELECT
				[WWIStockItemID] = SI.StockItemID,
				[StockItem] = SI.StockItemName,
				[Color] = 
					CASE 
						WHEN CA.ColorName IS NOT NULL THEN CA.ColorName
						ELSE 'N/A'
					END,
				[SellingPackage] = PTA.PackageTypeName,
				[BuyingPackage] = BPTA.PackageTypeName,
				[Brand] = ISNULL(SI.Brand, ''),
				[Size] = ISNULL(SI.Size, ''),
				SI.LeadTimeDays,
				SI.QuantityPerOuter,
				SI.IsChillerStock,
				SI.Barcode,
				SI.TaxRate,
				SI.UnitPrice,
				SI.RecommendedRetailPrice,
				SI.TypicalWeightPerUnit,
				SI.Photo
			FROM
				dbo.Warehouse_StockItems SI LEFT JOIN
				PackageTypesAvailable PTA ON
					SI.UnitPackageID = PTA.PackageTypeID LEFT JOIN
				PackageTypesAvailable AS BPTA ON
					SI.OuterPackageID = BPTA.PackageTypeID LEFT JOIN
				ColorsAvailable CA ON
					SI.ColorID = CA.ColorID 
			WHERE
				(
					SI.ValidFrom > @LastCutoff OR
					SI.UnitPackageID IN
					(
						SELECT
							PTC.PackageTypeID
						FROM
							PackageTypesChanged PTC
					) OR
					SI.OuterPackageID IN
					(
						SELECT
							PTC.PackageTypeID
						FROM
							PackageTypesChanged PTC
					) OR
					SI.ColorID IN 
					(
						SELECT
							CC.ColorID
						FROM
							ColorsChanged CC
					)
				) AND
				@NewCutoff <= SI.ValidTo AND
				SI.ValidFrom <= @NewCutoff

		),

		Final AS (

			SELECT
				[WWIStockItemID] = SI.StockItemID,
				[StockItem] = SI.StockItemName,
				[Color] = 
					CASE 
						WHEN CA.ColorName IS NOT NULL THEN CA.ColorName
						ELSE 'N/A'
					END,
				[SellingPackage] = PTA.PackageTypeName,
				[BuyingPackage] = BPTA.PackageTypeName,
				[Brand] = ISNULL(SI.Brand, ''),
				[Size] = ISNULL(SI.Size, ''),
				SI.LeadTimeDays,
				SI.QuantityPerOuter,
				SI.IsChillerStock,
				SI.Barcode,
				SI.TaxRate,
				SI.UnitPrice,
				SI.RecommendedRetailPrice,
				SI.TypicalWeightPerUnit,
				SI.Photo
			FROM
				dbo.Warehouse_StockItems SI LEFT JOIN
				PackageTypesAvailable PTA ON
					SI.UnitPackageID = PTA.PackageTypeID LEFT JOIN
				PackageTypesAvailable AS BPTA ON
					SI.OuterPackageID = BPTA.PackageTypeID LEFT JOIN
				ColorsAvailable CA ON
					SI.ColorID = CA.ColorID 
			WHERE
				(
					SI.ValidFrom > @LastCutoff OR
					SI.UnitPackageID IN
					(
						SELECT
							PTC.PackageTypeID
						FROM
							PackageTypesChanged PTC
					) OR
					SI.OuterPackageID IN
					(
						SELECT
							PTC.PackageTypeID
						FROM
							PackageTypesChanged PTC
					) OR
					SI.ColorID IN 
					(
						SELECT
							CC.ColorID
						FROM
							ColorsChanged CC
					)
				) AND
				@NewCutoff BETWEEN SI.ValidFrom AND SI.ValidTo 

			UNION ALL

			SELECT
				SI.StockItemID,
				SI.StockItemName, 
				CASE 
					WHEN CA.ColorName IS NOT NULL THEN CA.ColorName
					ELSE 'N/A'
				END,
				PTA.PackageTypeName,
				BPTA.PackageTypeName,
				ISNULL(SI.Brand, ''),
				ISNULL(SI.Size, ''),
				SI.LeadTimeDays,
				SI.QuantityPerOuter,
				SI.IsChillerStock,
				SI.Barcode,
				SI.TaxRate,
				SI.UnitPrice,
				SI.RecommendedRetailPrice,
				SI.TypicalWeightPerUnit,
				SI.Photo
			FROM
				dbo.Warehouse_StockItems_Archive SI LEFT JOIN
				PackageTypesAvailable PTA ON
					SI.UnitPackageID = PTA.PackageTypeID LEFT JOIN
				PackageTypesAvailable AS BPTA ON
					SI.OuterPackageID = BPTA.PackageTypeID LEFT JOIN
				ColorsAvailable CA ON
					SI.ColorID = CA.ColorID 
			WHERE
				(
					SI.ValidFrom > @LastCutoff OR
					SI.UnitPackageID IN
					(
						SELECT
							PTC.PackageTypeID
						FROM
							PackageTypesChanged PTC
					) OR
					SI.OuterPackageID IN
					(
						SELECT
							PTC.PackageTypeID
						FROM
							PackageTypesChanged PTC
					) OR
					SI.ColorID IN 
					(
						SELECT
							CC.ColorID
						FROM
							ColorsChanged CC
					)
				) AND
				@NewCutoff BETWEEN SI.ValidFrom AND SI.ValidTo

		)

		SELECT 
			*
		INTO
			#DimStockItem
		FROM
			Final
		ORDER BY
			WWIStockItemID

		-- Update Existing
		UPDATE
			SI2
		SET
			SI2.StockItem = SI.StockItem,
			SI2.Color = SI.Color,
			SI2.SellingPackage = SI.SellingPackage,
			SI2.BuyingPackage = SI.BuyingPackage,
			SI2.Brand = SI.Brand,
			SI2.Size = SI.Size,
			SI2.LeadTimeDays = SI.LeadTimeDays,
			SI2.QuantityPerOuter = SI.QuantityPerOuter,
			SI2.IsChillerStock = SI.IsChillerStock,
			SI2.Barcode = SI.Barcode,
			SI2.TaxRate = SI.TaxRate,
			SI2.UnitPrice = SI.UnitPrice,
			SI2.RecommendedRetailPrice = SI.RecommendedRetailPrice,
			SI2.TypicalWeightPerUnit = SI.TypicalWeightPerUnit,
			SI2.Photo = SI.Photo,
			SI2.LoadDate = @NewCutoff
		FROM
			#DimStockItem SI JOIN
			dbo.DimStockItem SI2 ON
				SI2.WWIStockItemID = SI.WWIStockItemID

		-- Insert New
		INSERT INTO dbo.DimStockItem
		(
			WWIStockItemID,
			StockItem,
			Color,
			SellingPackage,
			BuyingPackage,
			Brand,
			Size,
			LeadTimeDays,
			QuantityPerOuter,
			IsChillerStock,
			Barcode,
			TaxRate,
			UnitPrice,
			RecommendedRetailPrice,
			TypicalWeightPerUnit,
			Photo,
			LoadDate
		)
		SELECT
			SI.WWIStockItemID,
			SI.StockItem,
			SI.Color,
			SI.SellingPackage,
			SI.BuyingPackage,
			SI.Brand,
			SI.Size,
			SI.LeadTimeDays,
			SI.QuantityPerOuter,
			SI.IsChillerStock,
			SI.Barcode,
			SI.TaxRate,
			SI.UnitPrice,
			SI.RecommendedRetailPrice,
			SI.TypicalWeightPerUnit,
			SI.Photo,
			@NewCutoff
		FROM
			#DimStockItem SI LEFT JOIN
			dbo.DimStockItem SI2 ON
				SI2.WWIStockItemID = SI.WWIStockItemID
		WHERE
			SI2.WWIStockItemID IS NULL
		ORDER BY
			SI.WWIStockItemID

		IF NOT EXiSTS
		(
			SELECT
				1
			FROM
				dbo.DimStockItem SI
			WHERE
				SI.StockItemKey = 0
		)
		BEGIN

			INSERT INTO dbo.DimStockItem
			(
				StockItemKey,
				WWIStockItemID,
				StockItem,
				Color,
				SellingPackage,
				BuyingPackage,
				Brand,
				Size,
				LeadTimeDays,
				QuantityPerOuter,
				IsChillerStock,
				Barcode,
				TaxRate,
				UnitPrice,
				RecommendedRetailPrice,
				TypicalWeightPerUnit,
				Photo,
				LoadDate
			)
			VALUES
			(
				0,
				0,
				'Unknown',
				'N/A',
				'N/A',
				'N/A',
				'N/A',
				'N/A',
				0,
				0,	
				0,
				'N/A',
				0.000,
				0.00,
				0.00,
				0.000,
				NULL,
				@NewCutoff
			)

		END

		INSERT INTO dbo.WarehouseHistory
		(
			TableName,
			LoadDate,
			Status,
			Details
		)
		VALUES
		(
			'DimStockItem',
			@NewCutoff,
			'Successful',
			NULL
		)

	COMMIT TRAN

	IF OBJECT_ID('tempdb..#DimStockItem', 'U') IS NOT NULL
		DROP TABLE #DimStockItem

END
GO
/****** Object:  StoredProcedure [dbo].[ProcessDimSupplier]    Script Date: 10/13/2025 10:24:08 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO







/*

EXEC [dbo].[ProcessDimSupplier] '12/31/2012', '1/1/2013'

*/
CREATE PROCEDURE [dbo].[ProcessDimSupplier]
(
	@InitialLoad DATETIME,
	@NewCutoff DATETIME
)
WITH EXECUTE AS OWNER
AS
BEGIN

    SET NOCOUNT ON;
    SET XACT_ABORT ON;

	IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[SupplierKey]') AND type = 'SO')
	BEGIN

		CREATE SEQUENCE [dbo].[SupplierKey] 
		AS [int]
		START WITH 1
		INCREMENT BY 1
		MINVALUE -2147483648
		MAXVALUE 2147483647
		CACHE 

	END

	IF OBJECT_ID (N'DimSupplier', N'U') IS NULL 
	BEGIN

		CREATE TABLE [dbo].[DimSupplier]
		(
			[SupplierKey] [int] NOT NULL,
			[WWISupplierID] [int] NOT NULL,
			[Supplier] [nvarchar](100) NOT NULL,
			[Category] [nvarchar](50) NOT NULL,
			[PrimaryContact] [nvarchar](50) NOT NULL,
			[SupplierReference] [nvarchar](20) NULL,
			[PaymentDays] [int] NOT NULL,
			[PostalCode] [nvarchar](10) NOT NULL,
			[LoadDate] [datetime2](7) NOT NULL,
			CONSTRAINT [PK_DimSupplier] PRIMARY KEY CLUSTERED 
			(
				[SupplierKey] ASC
			)
			WITH 
			(
				STATISTICS_NORECOMPUTE = OFF, 
				IGNORE_DUP_KEY = OFF, 
				OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF
			) ON [PRIMARY]
		) ON [PRIMARY]

		ALTER TABLE [dbo].[DimSupplier] ADD  CONSTRAINT [DF_DimSupplier_SupplierKey]  DEFAULT (NEXT VALUE FOR [dbo].[SupplierKey]) FOR [SupplierKey]

	END

	DECLARE @LastCutoff DATETIME

	SELECT TOP 1
		@LastCutoff = WH.LoadDate
	FROM
		dbo.WarehouseHistory WH
	WHERE
		WH.Status = 'Successful' AND
		TableName = 'DimSupplier'
	ORDER BY
		WH.LoadDate DESC

	SET @LastCutoff = ISNULL(@LastCutoff, @InitialLoad)

	BEGIN TRAN

		;WITH SupplierCategoriesChanged AS (

			SELECT
				SC.SupplierCategoryID
			FROM
				dbo.Purchasing_SupplierCategories SC
			WHERE
				SC.ValidFrom > @LastCutoff AND
				@NewCutoff BETWEEN SC.ValidFrom AND SC.ValidTo 

			UNION ALL

			SELECT
				SCA.SupplierCategoryID
			FROM
				dbo.Purchasing_SupplierCategories_Archive SCA
			WHERE
				SCA.ValidFrom > @LastCutoff AND
				@NewCutoff BETWEEN SCA.ValidFrom AND SCA.ValidTo 

		),

		PrimaryContactsChanged AS (

			SELECT
				P.PersonID
			FROM
				dbo.Application_People P
			WHERE
				P.ValidFrom > @LastCutoff AND
				@NewCutoff BETWEEN P.ValidFrom AND P.ValidTo 

			UNION ALL

			SELECT
				PA.PersonID
			FROM
				dbo.Application_People_Archive PA
			WHERE
				PA.ValidFrom > @LastCutoff AND
				@NewCutoff BETWEEN PA.ValidFrom AND PA.ValidTo 

		),

		SupplierCategoriesAvailable AS (

			SELECT
				SC.SupplierCategoryID,
				SC.SupplierCategoryName
			FROM
				dbo.Purchasing_SupplierCategories SC
			WHERE
				@NewCutoff BETWEEN SC.ValidFrom AND SC.ValidTo

			UNION

			SELECT
				SCA.SupplierCategoryID,
				SCA.SupplierCategoryName
			FROM
				dbo.Purchasing_SupplierCategories_Archive SCA
			WHERE
				@NewCutoff BETWEEN SCA.ValidFrom AND SCA.ValidTo

		),

		PrimaryContactsAvailable AS (

			SELECT
				P.PersonID,
				P.FullName
			FROM
				dbo.Application_People P
			WHERE
				@NewCutoff BETWEEN P.ValidFrom AND P.ValidTo

			UNION

			SELECT
				PA.PersonID,
				PA.FullName
			FROM
				dbo.Application_People_Archive PA
			WHERE
				@NewCutoff BETWEEN PA.ValidFrom AND PA.ValidTo

		),

		Final AS (

			SELECT
				[WWISupplierID] = S.SupplierID,
				[Supplier] = S.SupplierName,
				[Category] = SCA.SupplierCategoryName,
				[PrimaryContact] = PCA.FullName,
				S.SupplierReference,
				S.PaymentDays,
				[PostalCode] = S.DeliveryPostalCode 
			FROM
				dbo.Purchasing_Suppliers S LEFT JOIN
				SupplierCategoriesAvailable SCA ON
					S.SupplierCategoryID = SCA.SupplierCategoryID LEFT JOIN
				PrimaryContactsAvailable PCA ON
					S.PrimaryContactPersonID = PCA.PersonID
			WHERE
				(
					S.ValidFrom > @LastCutoff OR
					S.SupplierCategoryID IN
					(
						SELECT
							SCC.SupplierCategoryID
						FROM
							SupplierCategoriesChanged SCC
					) OR
					S.PrimaryContactPersonID IN
					(
						SELECT
							PCC.PersonID
						FROM
							PrimaryContactsChanged PCC
					)
				) AND
				@NewCutoff BETWEEN S.ValidFrom AND S.ValidTo

			UNION ALL

			SELECT
				S.SupplierID,
				S.SupplierName,
				SCA.SupplierCategoryName,
				PCA.FullName,
				S.SupplierReference,
				S.PaymentDays,
				S.DeliveryPostalCode 
			FROM
				dbo.Purchasing_Suppliers_Archive S LEFT JOIN
				SupplierCategoriesAvailable SCA ON
					S.SupplierCategoryID = SCA.SupplierCategoryID LEFT JOIN
				PrimaryContactsAvailable PCA ON
					S.PrimaryContactPersonID = PCA.PersonID
			WHERE
				(
					S.ValidFrom > @LastCutoff OR
					S.SupplierCategoryID IN
					(
						SELECT
							SCC.SupplierCategoryID
						FROM
							SupplierCategoriesChanged SCC
					) OR
					S.PrimaryContactPersonID IN
					(
						SELECT
							PCC.PersonID
						FROM
							PrimaryContactsChanged PCC
					)
				) AND
				@NewCutoff BETWEEN S.ValidFrom AND S.ValidTo

		)

		SELECT 
			*
		INTO
			#DimSupplier
		FROM
			Final
		ORDER BY
			WWISupplierID

		-- Update Existing
		UPDATE
			S2
		SET
			S2.Supplier = S.Supplier,
			S2.Category = S.Category,
			S2.PrimaryContact = S.PrimaryContact,
			S2.SupplierReference = S.SupplierReference,
			S2.PaymentDays = S.PaymentDays,
			S2.PostalCode = S.PostalCode,
			S2.LoadDate = @NewCutoff
		FROM
			#DimSupplier S JOIN
			dbo.DimSupplier S2 ON
				S2.WWISupplierID = S.WWISupplierID

		-- Insert New
		INSERT INTO dbo.DimSupplier
		(
			WWISupplierID,
			Supplier,
			Category,
			PrimaryContact,
			SupplierReference,
			PaymentDays,
			PostalCode,
			LoadDate
		)
		SELECT
			S.WWISupplierID,
			S.Supplier,
			S.Category,
			S.PrimaryContact,
			S.SupplierReference,
			S.PaymentDays,
			S.PostalCode,
			@NewCutoff
		FROM
			#DimSupplier S LEFT JOIN
			dbo.DimSupplier S2 ON
				S2.WWISupplierID = S.WWISupplierID
		WHERE
			S2.WWISupplierID IS NULL
		ORDER BY
			S.WWISupplierID


		IF NOT EXISTS
		(
			SELECT
				1
			FROM
				dbo.DimSupplier S
			WHERE
				S.[SupplierKey] = 0
		)
		BEGIN

			INSERT dbo.DimSupplier 
			(
				[SupplierKey],
				[WWISupplierID], 
				[Supplier], 
				[Category], 
				[PrimaryContact], 
				[SupplierReference],
				[PaymentDays], 
				[PostalCode], 
				[LoadDate]
			)
			VALUES
			(
				0,
				0,
				'Unknown',
				'N/A',
				'N/A',
				'N/A',
				0,	
				'N/A',
				@NewCutoff
			)

		END

		INSERT INTO dbo.WarehouseHistory
		(
			TableName,
			LoadDate,
			Status,
			Details
		)
		VALUES
		(
			'DimSupplier',
			@NewCutoff,
			'Successful',
			NULL
		)

	COMMIT TRAN

	IF OBJECT_ID('tempdb..#DimSupplier', 'U') IS NOT NULL
		DROP TABLE #DimSupplier

END
GO
/****** Object:  StoredProcedure [dbo].[ProcessDimTransactionType]    Script Date: 10/13/2025 10:24:08 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO







/*

EXEC [dbo].[ProcessDimTransactionType] '12/31/2012', '1/1/2013'

*/
CREATE PROCEDURE [dbo].[ProcessDimTransactionType]
(
	@InitialLoad DATETIME,
	@NewCutoff DATETIME
)
WITH EXECUTE AS OWNER
AS
BEGIN

    SET NOCOUNT ON;
    SET XACT_ABORT ON;

	IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[TransactionTypeKey]') AND type = 'SO')
	BEGIN

		CREATE SEQUENCE [dbo].[TransactionTypeKey] 
		AS [int]
		START WITH 1
		INCREMENT BY 1
		MINVALUE -2147483648
		MAXVALUE 2147483647
		CACHE 

	END

	IF OBJECT_ID (N'DimTransactionType', N'U') IS NULL 
	BEGIN
		
		CREATE TABLE [dbo].[DimTransactionType]
		(
			[TransactionTypeKey] [int] NOT NULL,
			[WWITransactionTypeID] [int] NOT NULL,
			[TransactionType] [nvarchar](50) NOT NULL,
			[LoadDate] [datetime2](7) NOT NULL,
			CONSTRAINT [PK_DimTransactionType] PRIMARY KEY CLUSTERED 
			(
				[TransactionTypeKey] ASC
			)
			WITH 
			(
				STATISTICS_NORECOMPUTE = OFF, 
				IGNORE_DUP_KEY = OFF, 
				OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF
			) ON [PRIMARY]
		) ON [PRIMARY]

		ALTER TABLE [dbo].[DimTransactionType] ADD  CONSTRAINT [DF_DimTransactionType_TransactionTypeKey]  DEFAULT (NEXT VALUE FOR [dbo].[TransactionTypeKey]) FOR [TransactionTypeKey]

	END

	DECLARE @LastCutoff DATETIME

	IF OBJECT_ID('tempdb..#DimTransactionType', 'U') IS NOT NULL
		DROP TABLE #DimTransactionType

	SELECT TOP 1
		@LastCutoff = WH.LoadDate
	FROM
		dbo.WarehouseHistory WH
	WHERE
		WH.Status = 'Successful' AND
		TableName = 'DimTransactionType'
	ORDER BY
		WH.LoadDate DESC

	SET @LastCutoff = ISNULL(@LastCutoff, @InitialLoad)

	BEGIN TRAN

		;WITH Final AS (

			SELECT
				[WWITransactionTypeID] = TT.TransactionTypeID,
				[TransactionType] = TT.TransactionTypeName
			FROM
				dbo.Application_TransactionTypes TT
			WHERE
				TT.ValidFrom > @LastCutoff AND
				@NewCutoff BETWEEN TT.ValidFrom AND TT.ValidTo

			UNION ALL

			SELECT
				[WWITransactionTypeID] = TTA.TransactionTypeID,
				[TransactionType] = TTA.TransactionTypeName
			FROM
				dbo.Application_TransactionTypes_Archive TTA
			WHERE
				TTA.ValidFrom > @LastCutoff AND
				@NewCutoff BETWEEN TTA.ValidFrom AND TTA.ValidTo

		)

		SELECT 
			*
		INTO
			#DimTransactionType
		FROM
			Final
		ORDER BY
			WWITransactionTypeID

		-- Update Existing
		UPDATE
			TT2
		SET
			TT2.TransactionType = TT.TransactionType,
			TT2.LoadDate = @NewCutoff
		FROM
			#DimTransactionType TT JOIN
			dbo.DimTransactionType TT2 ON
				TT2.WWITransactionTypeID = TT.WWITransactionTypeID

		-- Insert New
		INSERT INTO dbo.DimTransactionType
		(
			WWITransactionTypeID,
			TransactionType,
			LoadDate
		)
		SELECT
			TT.WWITransactionTypeID,
			TT.TransactionType,
			@NewCutoff
		FROM
			#DimTransactionType TT LEFT JOIN
			dbo.DimTransactionType TT2 ON
				TT2.WWITransactionTypeID = TT.WWITransactionTypeID
		WHERE
			TT2.WWITransactionTypeID IS NULL
		ORDER BY
			TT.WWITransactionTypeID

		IF NOT EXISTS
		(
			SELECT
				1
			FROM
				dbo.DimTransactionType TT
			WHERE
				TT.TransactionTypeKey = 0
		)
		BEGIN

			INSERT INTO dbo.DimTransactionType
			(
				TransactionTypeKey,
				WWITransactionTypeID,
				TransactionType,
				LoadDate
			)
			VALUES
			(
				0,
				0,
				'Unknown',
				@NewCutoff
			)

		END

		INSERT INTO dbo.WarehouseHistory
		(
			TableName,
			LoadDate,
			Status,
			Details
		)
		VALUES
		(
			'DimTransactionType',
			@NewCutoff,
			'Successful',
			NULL
		)

	COMMIT TRAN

	IF OBJECT_ID('tempdb..#DimTransactionType', 'U') IS NOT NULL
		DROP TABLE #DimTransactionType

END
GO
/****** Object:  StoredProcedure [dbo].[ProcessFctMovement]    Script Date: 10/13/2025 10:24:08 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO




/*

EXEC [dbo].[ProcessFctMovement] '12/31/2012', '1/1/2013'

*/
CREATE PROCEDURE [dbo].[ProcessFctMovement]
(
	@InitialLoad DATETIME,
	@NewCutoff DATETIME
)
WITH EXECUTE AS OWNER
AS
BEGIN

    SET NOCOUNT ON;
    SET XACT_ABORT ON;

	IF OBJECT_ID (N'FctMovement', N'U') IS NULL 
	BEGIN

		CREATE TABLE [dbo].[FctMovement]
		(
			[MovementKey] [int] IDENTITY(1,1) NOT NULL,
			[DateKey] [date] NOT NULL,
			[StockItemKey] [int] NOT NULL,
			[CustomerKey] [int] NULL,
			[SupplierKey] [int] NULL,
			[TransactionTypeKey] [int] NOT NULL,
			[WWIStockItemTransactionID] [int] NOT NULL,
			[WWIInvoiceID] [int] NULL,
			[WWIPurchaseOrderID] [int] NULL,
			[Quantity] [int] NOT NULL,
			[LoadDate] [datetime2](7) NOT NULL,
			CONSTRAINT [PK_FctMovement] PRIMARY KEY CLUSTERED 
			(
				[MovementKey] ASC,
				[DateKey] ASC
			)
			WITH 
			(
				STATISTICS_NORECOMPUTE = OFF, 
				IGNORE_DUP_KEY = OFF, 
				OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF
			) ON [PRIMARY]
		) ON [PRIMARY]

		ALTER TABLE [dbo].[FctMovement]  WITH CHECK ADD  CONSTRAINT [FK_FctMovement_CustomerKey_DimCustomer] FOREIGN KEY([CustomerKey])
		REFERENCES [dbo].[DimCustomer] ([CustomerKey])

		ALTER TABLE [dbo].[FctMovement] CHECK CONSTRAINT [FK_FctMovement_CustomerKey_DimCustomer]

		ALTER TABLE [dbo].[FctMovement]  WITH CHECK ADD  CONSTRAINT [FK_FctMovement_DateKey_DimDate] FOREIGN KEY([DateKey])
		REFERENCES [dbo].[DimDate] ([Date])

		ALTER TABLE [dbo].[FctMovement] CHECK CONSTRAINT [FK_FctMovement_DateKey_DimDate]

		ALTER TABLE [dbo].[FctMovement]  WITH CHECK ADD  CONSTRAINT [FK_FctMovement_StockItemKey_DimStockItem] FOREIGN KEY([StockItemKey])
		REFERENCES [dbo].[DimStockItem] ([StockItemKey])

		ALTER TABLE [dbo].[FctMovement] CHECK CONSTRAINT [FK_FctMovement_StockItemKey_DimStockItem]

		ALTER TABLE [dbo].[FctMovement]  WITH CHECK ADD  CONSTRAINT [FK_FctMovement_SupplierKey_DimSupplier] FOREIGN KEY([SupplierKey])
		REFERENCES [dbo].[DimSupplier] ([SupplierKey])

		ALTER TABLE [dbo].[FctMovement] CHECK CONSTRAINT [FK_FctMovement_SupplierKey_DimSupplier]

		ALTER TABLE [dbo].[FctMovement]  WITH CHECK ADD  CONSTRAINT [FK_FctMovement_TransactionTypeKey_DimTransactionType] FOREIGN KEY([TransactionTypeKey])
		REFERENCES [dbo].[DimTransactionType] ([TransactionTypeKey])

		ALTER TABLE [dbo].[FctMovement] CHECK CONSTRAINT [FK_FctMovement_TransactionTypeKey_DimTransactionType]

	END

	DECLARE @LastCutoff DATETIME

	IF OBJECT_ID('tempdb..#FctMovement', 'U') IS NOT NULL
		DROP TABLE #FctMovement

	SELECT TOP 1
		@LastCutoff = WH.LoadDate
	FROM
		dbo.WarehouseHistory WH
	WHERE
		WH.Status = 'Successful' AND
		TableName = 'FctMovement'
	ORDER BY
		WH.LoadDate DESC

	SET @LastCutoff = ISNULL(@LastCutoff, @InitialLoad)

	BEGIN TRAN

		;WITH Final AS 
		(

			SELECT 
				[DateKey] = CAST(SIT.TransactionOccurredWhen AS DATE),
				[StockItemKey] = ISNULL(SI.StockItemKey, 0),
				[CustomerKey] = ISNULL(C.CustomerKey, 0),
				[SupplierKey] = ISNULL(S.SupplierKey, 0),
				[TransactionTypeKey] = TT.TransactionTypeKey,
				[WWIStockItemTransactionID] = SIT.StockItemTransactionID,
				[WWIInvoiceID] = SIT.InvoiceID,
				[WWIPurchaseOrderID] = SIT.PurchaseOrderID,
				SIT.Quantity
			FROM 
				dbo.Warehouse_StockItemTransactions AS SIT LEFT JOIN
				dbo.DimStockItem SI ON 
					SI.WWIStockItemID = SIT.StockItemID LEFT JOIN
				dbo.DimCustomer C ON
					C.WWICustomerID = SIT.CustomerID LEFT JOIN
				dbo.DimSupplier S ON
					S.WWISupplierID = SIT.SupplierID LEFT JOIN
				dbo.DimTransactionType TT ON
					TT.WWITransactionTypeID = SIT.TransactionTypeID

			WHERE 
				SIT.LastEditedWhen > @LastCutoff AND
				SIT.LastEditedWhen <= @NewCutoff

		)

		SELECT 
			*
		INTO
			#FctMovement
		FROM
			Final

		-- Update Existing
		UPDATE
			M2
		SET
			M2.DateKey = M.DateKey,
			M2.StockItemKey = M.StockItemKey,
			M2.CustomerKey = M.CustomerKey,
			M2.SupplierKey = M.SupplierKey,
			M2.TransactionTypeKey = M.TransactionTypeKey,
			M2.WWIStockItemTransactionID = M.WWIStockItemTransactionID,
			M2.WWIInvoiceID = M.WWIInvoiceID,
			M2.WWIPurchaseOrderID = M.WWIPurchaseOrderID,
			M2.Quantity = M.Quantity,
			M2.LoadDate = @NewCutoff
		FROM
			#FctMovement M JOIN
			dbo.FctMovement M2 ON
				M2.WWIStockItemTransactionID = M.WWIStockItemTransactionID

		-- Insert New
		INSERT INTO dbo.FctMovement
		(
			DateKey,
			StockItemKey,
			CustomerKey,
			SupplierKey,
			TransactionTypeKey,
			WWIStockItemTransactionID,
			WWIInvoiceID,
			WWIPurchaseOrderID,
			Quantity,
			LoadDate
		)
		SELECT
			M.DateKey,
			M.StockItemKey,
			M.CustomerKey,
			M.SupplierKey,
			M.TransactionTypeKey,
			M.WWIStockItemTransactionID,
			M.WWIInvoiceID,
			M.WWIPurchaseOrderID,
			M.Quantity,
			@NewCutoff
		FROM
			#FctMovement M LEFT JOIN
			dbo.FctMovement M2 ON
				M2.WWIStockItemTransactionID = M.WWIStockItemTransactionID
		WHERE
			M2.WWIStockItemTransactionID IS NULL
		ORDER BY
			M.WWIStockItemTransactionID

		INSERT INTO dbo.WarehouseHistory
		(
			TableName,
			LoadDate,
			Status,
			Details
		)
		VALUES
		(
			'FctMovement',
			@NewCutoff,
			'Successful',
			NULL
		)

	COMMIT TRAN

	IF OBJECT_ID('tempdb..#FctMovement', 'U') IS NOT NULL
		DROP TABLE #FctMovement

END;
GO
/****** Object:  StoredProcedure [dbo].[ProcessFctOrder]    Script Date: 10/13/2025 10:24:08 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO







/*

EXEC [dbo].[ProcessFctOrder] '12/31/2012', '1/1/2013'

*/
CREATE PROCEDURE [dbo].[ProcessFctOrder]
(
	@InitialLoad DATETIME,
	@NewCutoff DATETIME
)
WITH EXECUTE AS OWNER
AS
BEGIN

    SET NOCOUNT ON;
    SET XACT_ABORT ON;

	IF OBJECT_ID (N'FctOrder', N'U') IS NULL 
	BEGIN

		CREATE TABLE [dbo].[FctOrder]
		(
			[OrderKey] [bigint] IDENTITY(1,1) NOT NULL,
			[CityKey] [int] NOT NULL,
			[CustomerKey] [int] NOT NULL,
			[StockItemKey] [int] NOT NULL,
			[OrderDateKey] [date] NOT NULL,
			[PickedDateKey] [date] NULL,
			[SalesPersonKey] [int] NOT NULL,
			[PickerKey] [int] NULL,
			[WWIOrderID] [int] NOT NULL,
			[WWIOrderLineID] [int] NOT NULL,
			[WWIBackorderID] [int] NULL,
			[Description] [nvarchar](100) NOT NULL,
			[Package] [nvarchar](50) NOT NULL,
			[Quantity] [int] NOT NULL,
			[UnitPrice] [decimal](18, 2) NOT NULL,
			[TaxRate] [decimal](18, 3) NOT NULL,
			[TotalExcludingTax] [decimal](18, 2) NOT NULL,
			[TaxAmount] [decimal](18, 2) NOT NULL,
			[TotalIncludingTax] [decimal](18, 2) NOT NULL,
			[LoadDate] [datetime] NOT NULL,
			CONSTRAINT [PK_FctOrder] PRIMARY KEY NONCLUSTERED 
			(
				[OrderKey] ASC,
				[OrderDateKey] ASC
			)
			WITH 
			(
				STATISTICS_NORECOMPUTE = OFF, 
				IGNORE_DUP_KEY = OFF, 
				OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF
			) ON [PRIMARY]
		) ON [PRIMARY]

		ALTER TABLE [dbo].[FctOrder]  WITH CHECK ADD  CONSTRAINT [FK_FctOrder_CityKey_DimCity] FOREIGN KEY([CityKey])
		REFERENCES [dbo].[DimCity] ([CityKey])

		ALTER TABLE [dbo].[FctOrder] CHECK CONSTRAINT [FK_FctOrder_CityKey_DimCity]

		ALTER TABLE [dbo].[FctOrder]  WITH CHECK ADD  CONSTRAINT [FK_FctOrder_CustomerKey_DimCustomer] FOREIGN KEY([CustomerKey])
		REFERENCES [dbo].[DimCustomer] ([CustomerKey])

		ALTER TABLE [dbo].[FctOrder] CHECK CONSTRAINT [FK_FctOrder_CustomerKey_DimCustomer]

		ALTER TABLE [dbo].[FctOrder]  WITH CHECK ADD  CONSTRAINT [FK_FctOrder_OrderDateKey_DimDate] FOREIGN KEY([OrderDateKey])
		REFERENCES [dbo].[DimDate] ([Date])

		ALTER TABLE [dbo].[FctOrder] CHECK CONSTRAINT [FK_FctOrder_OrderDateKey_DimDate]

		ALTER TABLE [dbo].[FctOrder]  WITH CHECK ADD  CONSTRAINT [FK_FctOrder_PickedDateKey_DimDate] FOREIGN KEY([PickedDateKey])
		REFERENCES [dbo].[DimDate] ([Date])

		ALTER TABLE [dbo].[FctOrder] CHECK CONSTRAINT [FK_FctOrder_PickedDateKey_DimDate]

	END

	DECLARE @LastCutoff DATETIME

	IF OBJECT_ID('tempdb..#FctOrder', 'U') IS NOT NULL
		DROP TABLE #FctOrder

	SELECT TOP 1
		@LastCutoff = WH.LoadDate
	FROM
		dbo.WarehouseHistory WH
	WHERE
		WH.Status = 'Successful' AND
		TableName = 'FctOrder'
	ORDER BY
		WH.LoadDate DESC

	SET @LastCutoff = ISNULL(@LastCutoff, @InitialLoad)

	BEGIN TRAN

		;WITH PackageTypesChanged AS 
		(

			SELECT
				PT.PackageTypeID
			FROM
				dbo.Warehouse_PackageTypes PT
			WHERE
				PT.ValidFrom > @LastCutoff AND
				@NewCutoff BETWEEN PT.ValidFrom  AND PT.ValidTo

			UNION ALL

			SELECT
				PTA.PackageTypeID
			FROM
				dbo.Warehouse_PackageTypes_Archive PTA
			WHERE
				PTA.ValidFrom > @LastCutoff AND
				@NewCutoff BETWEEN PTA.ValidFrom AND PTA.ValidTo

		),

		PackageTypesAvailable AS 
		(

			SELECT
				PT.PackageTypeID,
				PT.PackageTypeName
			FROM
				dbo.Warehouse_PackageTypes PT
			WHERE
				@NewCutoff BETWEEN PT.ValidFrom AND PT.ValidTo

			UNION

			SELECT
				PTA.PackageTypeID,
				PTA.PackageTypeName
			FROM
				dbo.Warehouse_PackageTypes_Archive PTA
			WHERE
				@NewCutoff BETWEEN PTA.ValidFrom AND PTA.ValidTo

		),

		Final AS (

		   SELECT
				[CityKey] = ISNULL(CT.CityKey, 0),
				[CustomerKey] = ISNULL(C.CustomerKey, 0),
				[StockItemKey] = ISNULL(SI.StockItemKey, 0),
				[OrderDateKey] = CAST(O.OrderDate AS DATE),
				[PickedDateKey] = CAST(O.PickingCompletedWhen AS DATE),
				[SalesPersonKey] = ISNULL(E.EmployeeKey, 0),
				[PickerKey] = ISNULL(E2.EmployeeKey, 0),
				[WWIOrderID] = O.OrderID,
				[WWIOrderLineID] = OL.OrderLineID,
				[WWIBackorderID] = O.BackorderOrderID,
				OL.Description,
				[Package] = PTA.PackageTypeName,
				OL.Quantity,
				OL.UnitPrice,
				OL.TaxRate,
				[TotalExcludingTax] = ROUND(OL.Quantity * OL.UnitPrice, 2),
				[TaxAmount] = ROUND((OL.Quantity * OL.UnitPrice * OL.TaxRate) / 100.0, 2),
				[TotalIncludingTax] = (
					ROUND(OL.Quantity * OL.UnitPrice, 2) + 
					ROUND((OL.Quantity * OL.UnitPrice * OL.TaxRate) / 100.0, 2)
				)
			FROM
				dbo.Sales_Orders O JOIN
				dbo.Sales_OrderLines OL ON
					O.OrderID = OL.OrderID LEFT JOIN
				dbo.DimCustomer C ON
					O.CustomerID = C.WWICustomerID LEFT JOIN
				dbo.DimCity CT ON
					C.WWIDeliveryCityID = CT.WWICityID LEFT JOIN
				dbo.DimStockItem SI ON
					OL.StockItemID = SI.WWIStockItemID LEFT JOIN
				dbo.DimEmployee E ON
					O.SalespersonPersonID = E.WWIEmployeeID LEFT JOIN
				dbo.DimEmployee E2 ON
					O.PickedByPersonID = E2.WWIEmployeeID LEFT JOIN
				PackageTypesAvailable PTA ON
					OL.PackageTypeID = PTA.PackageTypeID
			WHERE 
				(
					O.LastEditedWhen > @LastCutoff OR
					OL.LastEditedWhen > @LastCutoff OR
					OL.PackageTypeID IN (
						SELECT 
							PTC.PackageTypeID
						FROM 
							PackageTypesChanged PTC
					)
				) AND
				O.LastEditedWhen <= @NewCutoff AND
				OL.LastEditedWhen <= @NewCutoff

		)

		SELECT 
			*
		INTO
			#FctOrder
		FROM
			Final

		-- Update Existing
		UPDATE
			O2
		SET
			O2.CityKey = O.CityKey,
			O2.CustomerKey = O.CustomerKey,
			O2.StockItemKey = O.StockItemKey,
			O2.OrderDateKey = O.OrderDateKey,
			O2.PickedDateKey = O.PickedDateKey,
			O2.SalesPersonKey = O.SalesPersonKey,
			O2.PickerKey = O.PickerKey,
			O2.WWIBackorderID = O.WWIBackorderID,
			O2.Description = O.Description,
			O2.Package = O.Package,
			O2.Quantity = O.Quantity,
			O2.UnitPrice = O.UnitPrice,
			O2.TaxRate = O.TaxRate,
			O2.TotalExcludingTax = O.TotalExcludingTax,
			O2.TaxAmount = O.TaxAmount,
			O2.TotalIncludingTax = O.TotalIncludingTax,
			O2.LoadDate = @NewCutoff
		FROM
			#FctOrder O JOIN
			dbo.FctOrder O2 ON
				O2.WWIOrderID = O.WWIOrderID AND
				O2.WWIOrderLineID = O.WWIOrderLineID

		-- Insert New
		INSERT INTO dbo.FctOrder
		(
			CityKey,
			CustomerKey,
			StockItemKey,
			OrderDateKey,
			PickedDateKey,
			SalesPersonKey,
			PickerKey,
			WWIOrderID,
			WWIOrderLineID,
			WWIBackorderID,
			Description,
			Package,
			Quantity,
			UnitPrice,
			TaxRate,
			TotalExcludingTax,
			TaxAmount,
			TotalIncludingTax,
			LoadDate
		)
		SELECT
			O.CityKey,
			O.CustomerKey,
			O.StockItemKey,
			O.OrderDateKey,
			O.PickedDateKey,
			O.SalesPersonKey,
			O.PickerKey,
			O.WWIOrderID,
			O.WWIOrderLineID,
			O.WWIBackorderID,
			O.Description,
			O.Package,
			O.Quantity,
			O.UnitPrice,
			O.TaxRate,
			O.TotalExcludingTax,
			O.TaxAmount,
			O.TotalIncludingTax,
			@NewCutoff
		FROM
			#FctOrder O LEFT JOIN
			dbo.FctOrder O2 ON
				O2.WWIOrderID = O.WWIOrderID AND
				O2.WWIOrderLineID = O.WWIOrderLineID
		WHERE
			O2.WWIorderID IS NULL
		ORDER BY
			O.WWIorderID,
			O.WWIOrderLineID

		INSERT INTO dbo.WarehouseHistory
		(
			TableName,
			LoadDate,
			Status,
			Details
		)
		VALUES
		(
			'FctOrder',
			@NewCutoff,
			'Successful',
			NULL
		)

	COMMIT TRAN

	IF OBJECT_ID('tempdb..#FctOrder', 'U') IS NOT NULL
		DROP TABLE #FctOrder

END;
GO
/****** Object:  StoredProcedure [dbo].[ProcessFctPurchase]    Script Date: 10/13/2025 10:24:08 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO







/*

EXEC [dbo].[ProcessFctPurchase] '12/31/2012', '1/1/2013'

*/
CREATE PROCEDURE [dbo].[ProcessFctPurchase]
(
	@InitialLoad DATETIME,
	@NewCutoff DATETIME
)
WITH EXECUTE AS OWNER
AS
BEGIN

    SET NOCOUNT ON;
    SET XACT_ABORT ON;

	IF OBJECT_ID (N'FctPurchase', N'U') IS NULL 
	BEGIN

		CREATE TABLE [dbo].[FctPurchase]
		(
			[PurchaseKey] [bigint] IDENTITY(1,1) NOT NULL,
			[DateKey] [date] NOT NULL,
			[SupplierKey] [int] NOT NULL,
			[StockItemKey] [int] NOT NULL,
			[WWIPurchaseOrderID] [int] NULL,
			[WWIPurchaseOrderLineID] [int] NULL,
			[OrderedOuters] [int] NOT NULL,
			[OrderedQuantity] [int] NOT NULL,
			[ReceivedOuters] [int] NOT NULL,
			[Package] [nvarchar](50) NOT NULL,
			[IsOrderFinalized] [bit] NOT NULL,
			[LoadDate] [datetime2](7) NOT NULL,
			CONSTRAINT [PK_Fact_Purchase] PRIMARY KEY NONCLUSTERED 
			(
				[PurchaseKey] ASC,
				[DateKey] ASC
			)
			WITH 
			(
				STATISTICS_NORECOMPUTE = OFF, 
				IGNORE_DUP_KEY = OFF, 
				OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF
			) ON [PRIMARY]
		) ON [PRIMARY]

		ALTER TABLE [dbo].[FctPurchase]  WITH CHECK ADD  CONSTRAINT [FK_FctPurchase_DateKey_DimDate] FOREIGN KEY([DateKey])
		REFERENCES [dbo].[DimDate] ([Date])

		ALTER TABLE [dbo].[FctPurchase] CHECK CONSTRAINT [FK_FctPurchase_DateKey_DimDate]

		ALTER TABLE [dbo].[FctPurchase]  WITH CHECK ADD  CONSTRAINT [FK_FctPurchase_StockItemKey_DimStockItem] FOREIGN KEY([StockItemKey])
		REFERENCES [dbo].[DimStockItem] ([StockItemKey])

		ALTER TABLE [dbo].[FctPurchase] CHECK CONSTRAINT [FK_FctPurchase_StockItemKey_DimStockItem]

		ALTER TABLE [dbo].[FctPurchase]  WITH CHECK ADD  CONSTRAINT [FK_FctPurchase_SupplierKey_DimSupplier] FOREIGN KEY([SupplierKey])
		REFERENCES [dbo].[DimSupplier] ([SupplierKey])

		ALTER TABLE [dbo].[FctPurchase] CHECK CONSTRAINT [FK_FctPurchase_SupplierKey_DimSupplier]

	END

	DECLARE @LastCutoff DATETIME

	IF OBJECT_ID('tempdb..#FctPurchase') IS NOT NULL
		DROP TABLE #FctPurchase;
	
	SELECT TOP 1
		@LastCutoff = WH.LoadDate
	FROM
		dbo.WarehouseHistory WH
	WHERE
		WH.Status = 'Successful' AND
		TableName = 'FctPurchase'
	ORDER BY
		WH.LoadDate DESC

	SET @LastCutoff = ISNULL(@LastCutoff, @InitialLoad)

	BEGIN TRAN

		;WITH PackageTypesChanged AS (

			SELECT
				PT.PackageTypeID
			FROM
				dbo.Warehouse_PackageTypes PT
			WHERE
				PT.ValidFrom > @LastCutoff AND
				@NewCutoff BETWEEN PT.ValidFrom  AND PT.ValidTo

			UNION ALL

			SELECT
				PTA.PackageTypeID
			FROM
				dbo.Warehouse_PackageTypes PTA
			WHERE
				PTA.ValidFrom > @LastCutoff AND
				@NewCutoff BETWEEN PTA.ValidFrom AND PTA.ValidTo

		),

		StockItemsChanged AS (

			SELECT
				SI.StockItemID
			FROM
				dbo.Warehouse_StockItems SI
			WHERE
				SI.ValidFrom > @LastCutoff AND
				@NewCutoff BETWEEN SI.ValidFrom  AND SI.ValidTo

			UNION ALL

			SELECT
				SIA.StockItemID
			FROM
				dbo.Warehouse_StockItems SIA
			WHERE
				SIA.ValidFrom > @LastCutoff AND
				@NewCutoff BETWEEN SIA.ValidFrom  AND SIA.ValidTo

		),

		PackageTypesAvailable AS 
		(

			SELECT
				PT.PackageTypeID,
				PT.PackageTypeName
			FROM
				dbo.Warehouse_PackageTypes PT
			WHERE
				@NewCutoff BETWEEN PT.ValidFrom AND PT.ValidTo

			UNION

			SELECT
				PTA.PackageTypeID,
				PTA.PackageTypeName
			FROM
				dbo.Warehouse_PackageTypes_Archive PTA
			WHERE
				@NewCutoff BETWEEN PTA.ValidFrom AND PTA.ValidTo

		),

		Final AS (

			SELECT 
				[DateKey] = CAST(PO.OrderDate AS DATE),
				[SupplierKey] = ISNULL(S.SupplierKey, 0),
				[StockItemKey] = ISNULL(SI.StockItemKey, 0),
				[WWIPurchaseOrderID] = PO.PurchaseOrderID,
				[WWIPurchaseOrderLineID] = POL.PurchaseOrderLineID,
				POL.OrderedOuters,
				[OrderedQuantity] = POL.OrderedOuters * SI.QuantityPerOuter,
				POL.ReceivedOuters,
				[Package] = PTA.PackageTypeName,
				[IsOrderFinalized] = POL.IsOrderLineFinalized
			FROM 
				dbo.Purchasing_PurchaseOrders PO JOIN 
				dbo.Purchasing_PurchaseOrderLines AS POL ON 
					PO.PurchaseOrderID = POL.PurchaseOrderID LEFT JOIN 
				dbo.DimSupplier S ON
					S.WWISupplierID = PO.SupplierID LEFT JOIN
				dbo.DimStockItem SI ON
					SI.WWIStockItemID = POL.StockItemID LEFT JOIN
				PackageTypesAvailable AS PTA ON 
					PTA.PackageTypeID = POL.PackageTypeID
			WHERE 
				(
					PO.LastEditedWhen > @LastCutoff OR
					POL.LastEditedWhen > @LastCutoff OR
					POL.PackageTypeID IN 
					(
						SELECT 
							PTC.PackageTypeID 
						FROM 
							PackageTypesChanged PTC
					) OR
					POL.StockItemID IN 
					(
						SELECT 
							SIC.StockItemID
						FROM 
							StockItemsChanged SIC
					)
				) AND
				PO.LastEditedWhen <= @NewCutoff AND
				POL.LastEditedWhen <= @NewCutoff

		)

		SELECT 
			*
		INTO
			#FctPurchase
		FROM
			Final

		-- Update Existing
		UPDATE
			P2
		SET
			P2.DateKey = P.DateKey,
			P2.SupplierKey = P.SupplierKey,
			P2.StockItemKey = P.StockItemKey,
			P2.OrderedOuters = P.OrderedOuters,
			P2.OrderedQuantity = P.OrderedQuantity,
			P2.ReceivedOuters = P.ReceivedOuters,
			P2.Package = P.Package,
			P2.IsOrderFinalized = P.IsOrderFinalized,
			P2.LoadDate = @NewCutoff
		FROM
			#FctPurchase P JOIN
			dbo.FctPurchase P2 ON
				P2.WWIPurchaseOrderID = P.WWIPurchaseOrderID AND
				P2.WWIPurchaseOrderLineID = P.WWIPurchaseOrderLineID

		-- Insert New
		INSERT INTO dbo.FctPurchase
		(
			DateKey,
			SupplierKey,
			StockItemKey,
			WWIPurchaseOrderID,
			WWIPurchaseOrderLineID,
			OrderedOuters,
			OrderedQuantity,
			ReceivedOuters,
			Package,
			IsOrderFinalized,
			LoadDate
		)
		SELECT
			P.DateKey,
			P.SupplierKey,
			P.StockItemKey,
			P.WWIPurchaseOrderID,
			P.WWIPurchaseOrderLineID,
			P.OrderedOuters,
			P.OrderedQuantity,
			P.ReceivedOuters,
			P.Package,
			P.IsOrderFinalized,
			@NewCutoff
		FROM
			#FctPurchase P LEFT JOIN
			dbo.FctPurchase P2 ON
				P2.WWIPurchaseOrderID = P.WWIPurchaseOrderID AND
				P2.WWIPurchaseOrderLineID = P.WWIPurchaseOrderLineID
		WHERE
			P2.WWIPurchaseOrderID IS NULL
		ORDER BY
			P.WWIPurchaseOrderID,
			P.WWIPurchaseOrderLineID

		INSERT INTO dbo.WarehouseHistory
		(
			TableName,
			LoadDate,
			Status,
			Details
		)
		VALUES
		(
			'FctPurchase',
			@NewCutoff,
			'Successful',
			NULL
		)

	COMMIT TRAN

	IF OBJECT_ID('tempdb..#FctPurchase') IS NOT NULL
		DROP TABLE #FctPurchase;

END;
GO
/****** Object:  StoredProcedure [dbo].[ProcessFctSale]    Script Date: 10/13/2025 10:24:08 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO











/*

EXEC [dbo].[ProcessFctSale] '12/31/2012', '1/1/2013'

*/
CREATE PROCEDURE [dbo].[ProcessFctSale]
(
	@InitialLoad DATETIME,
	@NewCutoff DATETIME
)
WITH EXECUTE AS OWNER
AS
BEGIN

    SET NOCOUNT ON;
    SET XACT_ABORT ON;

	IF OBJECT_ID (N'FctSale', N'U') IS NULL 
	BEGIN

		CREATE TABLE [dbo].[FctSale](
			[SaleKey] [bigint] IDENTITY(1,1) NOT NULL,
			[CityKey] [int] NOT NULL,
			[CustomerKey] [int] NOT NULL,
			[BillToCustomerKey] [int] NOT NULL,
			[StockItemKey] [int] NOT NULL,
			[InvoiceDateKey] [date] NOT NULL,
			[DeliveryDateKey] [date] NULL,
			[SalespersonKey] [int] NOT NULL,
			[WWIInvoiceID] [int] NOT NULL,
			[WWIInvoiceLineID] [int] NOT NULL,
			[Description] [nvarchar](100) NOT NULL,
			[Package] [nvarchar](50) NOT NULL,
			[Quantity] [int] NOT NULL,
			[UnitPrice] [decimal](18, 2) NOT NULL,
			[TaxRate] [decimal](18, 3) NOT NULL,
			[TotalExcludingTax] [decimal](18, 2) NOT NULL,
			[TaxAmount] [decimal](18, 2) NOT NULL,
			[Profit] [decimal](18, 2) NOT NULL,
			[TotalIncludingTax] [decimal](18, 2) NOT NULL,
			[TotalDryItems] [int] NOT NULL,
			[TotalChillerItems] [int] NOT NULL,
			[LoadDate] [datetime2](7) NOT NULL,
			CONSTRAINT [PK_FctSale] PRIMARY KEY NONCLUSTERED 
			(
				[SaleKey] ASC,
				[InvoiceDateKey] ASC
			)
			WITH 
			(
				STATISTICS_NORECOMPUTE = OFF, 
				IGNORE_DUP_KEY = OFF, 
				OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF
			) ON [PRIMARY]
		) ON [PRIMARY]

		ALTER TABLE [dbo].[FctSale]  WITH CHECK ADD  CONSTRAINT [FK_FctSale_BillToCustomerKey_DimCustomer] FOREIGN KEY([BillToCustomerKey])
		REFERENCES [dbo].[DimCustomer] ([CustomerKey])

		ALTER TABLE [dbo].[FctSale] CHECK CONSTRAINT [FK_FctSale_BillToCustomerKey_DimCustomer]

		ALTER TABLE [dbo].[FctSale]  WITH CHECK ADD  CONSTRAINT [FK_FctSale_CityKey_DimCity] FOREIGN KEY([CityKey])
		REFERENCES [dbo].[DimCity] ([CityKey])

		ALTER TABLE [dbo].[FctSale] CHECK CONSTRAINT [FK_FctSale_CityKey_DimCity]

	END

	DECLARE @LastCutoff DATETIME

	IF OBJECT_ID('tempdb..#FctSale') IS NOT NULL
		DROP TABLE #FctSale;

	SELECT TOP 1
		@LastCutoff = WH.LoadDate
	FROM
		dbo.WarehouseHistory WH
	WHERE
		WH.Status = 'Successful' AND
		TableName = 'FctSale'
	ORDER BY
		WH.LoadDate DESC

	SET @LastCutoff = ISNULL(@LastCutoff, @InitialLoad)

	BEGIN TRAN

		;WITH StockItemsChanged AS (

			SELECT
				ST.StockItemID
			FROM
				dbo.Warehouse_StockItems ST
			WHERE
				ST.ValidFrom > @LastCutoff AND
				@NewCutoff BETWEEN ST.ValidFrom  AND ST.ValidTo

			UNION ALL

			SELECT
				SIA.StockItemID
			FROM
				dbo.Warehouse_StockItems_Archive SIA
			WHERE
				SIA.ValidFrom > @LastCutoff AND
				@NewCutoff BETWEEN SIA.ValidFrom AND SIA.ValidTo

		),

		PackageTypesChanged AS (

			SELECT
				PT.PackageTypeID
			FROM
				dbo.Warehouse_PackageTypes PT
			WHERE
				PT.ValidFrom > @LastCutoff AND
				@NewCutoff BETWEEN PT.ValidFrom  AND PT.ValidTo

			UNION ALL

			SELECT
				PTA.PackageTypeID
			FROM
				dbo.Warehouse_PackageTypes_Archive PTA
			WHERE
				PTA.ValidFrom > @LastCutoff AND
				@NewCutoff BETWEEN PTA.ValidFrom  AND PTA.ValidTo

		),

		PackageTypesAvailable AS 
		(

			SELECT
				PT.PackageTypeID,
				PT.PackageTypeName
			FROM
				dbo.Warehouse_PackageTypes PT
			WHERE
				@NewCutoff BETWEEN PT.ValidFrom AND PT.ValidTo
	
			UNION
	
			SELECT
				PTA.PackageTypeID,
				PTA.PackageTypeName
			FROM
				dbo.Warehouse_PackageTypes_Archive PTA
			WHERE
				@NewCutoff BETWEEN PTA.ValidFrom AND PTA.ValidTo

		),

		Final AS (

			SELECT 
				CityKey = ISNULL(CT.CityKey, 0),
				[CustomerKey] = ISNULL(C.CustomerKey, 0),
				[BillToCustomerKey] = ISNULL(BC.CustomerKey, 0),
				[StockItemKey] = ISNULL(SI.StockItemKey, 0),
				[InvoiceDateKey] = CAST(I.InvoiceDate AS DATE),
				[DeliveryDateKey] = CAST(I.ConfirmedDeliveryTime AS DATE),
				[SalespersonKey] = E.EmployeeKey,
				[WWIInvoiceID] = I.InvoiceID,
				[WWIInvoiceLineID] = IL.InvoiceLineID,
				IL.Description,
				[Package] = PTA.PackageTypeName,
				IL.Quantity,
				IL.UnitPrice,
				IL.TaxRate,
				[TotalExcludingTax] = IL.ExtendedPrice - IL.TaxAmount,
				[TaxAmount] = IL.TaxAmount,
				[Profit] = IL.LineProfit,
				[TotalIncludingTax] = IL.ExtendedPrice,
				[TotalDryItems] = 
					CASE 
						WHEN SI.IsChillerStock = 0 THEN IL.Quantity 
						ELSE 0 
					END,
				[TotalChillerItems] =
					CASE 
						WHEN SI.IsChillerStock <> 0 THEN IL.Quantity 
						ELSE 0 
					END
			FROM 
				dbo.Sales_Invoices AS I JOIN 
				dbo.Sales_InvoiceLines IL ON 
					I.InvoiceID = IL.InvoiceID LEFT JOIN
				dbo.DimStockItem SI ON
					IL.StockItemID = SI.WWIStockItemID LEFT JOIN
				PackageTypesAvailable PTA ON
					IL.PackageTypeID = PTA.PackageTypeID LEFT JOIN
				dbo.DimCustomer C ON
					I.CustomerID = C.WWICustomerID LEFT JOIN
				dbo.DimCity CT ON
					C.WWIDeliveryCityID = CT.WWICityID LEFT JOIN
				dbo.DimCustomer BC ON
					I.BillToCustomerID = BC.WWICustomerID LEFT JOIN
				dbo.DimEmployee E ON
					I.SalespersonPersonID = E.WWIEmployeeID
			WHERE 
				(
					I.LastEditedWhen > @LastCutoff OR
					IL.LastEditedWhen > @LastCutoff OR
					IL.PackageTypeID IN (
						SELECT 
							PTC.PackageTypeID
						FROM 
							PackageTypesChanged PTC
					) OR
					IL.StockItemID IN (
						SELECT 
							SIC.StockItemID
						FROM 
							StockItemsChanged SIC
					) 
				) AND
				I.LastEditedWhen <= @NewCutoff AND
				IL.LastEditedWhen <= @NewCutoff

		)

		SELECT 
			*
		INTO
			#FctSale
		FROM
			Final

		-- Update Existing
		UPDATE
			S2
		SET
			S2.CityKey = S.CityKey,
			S2.CustomerKey = S.CustomerKey,
			S2.BillToCustomerKey = S.BillToCustomerKey,
			S2.StockItemKey = S.StockItemKey,
			S2.InvoiceDateKey = S.InvoiceDateKey,
			S2.DeliveryDateKey = S.DeliveryDateKey,
			S2.SalespersonKey = S.SalespersonKey,
			S2.Description = S.Description,
			S2.Package = S.Package,
			S2.Quantity = S.Quantity,
			S2.UnitPrice = S.UnitPrice,
			S2.TaxRate = S.TaxRate,
			S2.TotalExcludingTax = S.TotalExcludingTax,
			S2.TaxAmount = S.TaxAmount,
			S2.Profit = S.Profit,
			S2.TotalIncludingTax = S.TotalIncludingTax,
			S2.TotalDryItems = S.TotalDryItems,
			S2.TotalChillerItems = S.TotalChillerItems,
			S2.LoadDate = @NewCutoff
		FROM
			#FctSale S JOIN
			dbo.FctSale S2 ON
				S2.WWIInvoiceID = S.WWIInvoiceID AND
				S2.WWIInvoiceLineID = S.WWIInvoiceLineID

		-- Insert New
		INSERT INTO dbo.FctSale
		(
			CityKey,
			CustomerKey,
			BillToCustomerKey,
			StockItemKey,
			InvoiceDateKey,
			DeliveryDateKey,
			SalespersonKey,
			WWIInvoiceID,
			WWIInvoiceLineID,
			Description,
			Package,
			Quantity,
			UnitPrice,
			TaxRate,
			TotalExcludingTax,
			TaxAmount,
			Profit,
			TotalIncludingTax,
			TotalDryItems,
			TotalChillerItems,
			LoadDate
		)
		SELECT
			S.CityKey,
			S.CustomerKey,
			S.BillToCustomerKey,
			S.StockItemKey,
			S.InvoiceDateKey,
			S.DeliveryDateKey,
			S.SalespersonKey,
			S.WWIInvoiceID,
			S.WWIInvoiceLineID,
			S.Description,
			S.Package,
			S.Quantity,
			S.UnitPrice,
			S.TaxRate,
			S.TotalExcludingTax,
			S.TaxAmount,
			S.Profit,
			S.TotalIncludingTax,
			S.TotalDryItems,
			S.TotalChillerItems,
			@NewCutoff
		FROM
			#FctSale S LEFT JOIN
			dbo.FctSale S2 ON
				S2.WWIInvoiceID = S.WWIInvoiceID AND
				S2.WWIInvoiceLineID = S.WWIInvoiceLineID
		WHERE
			S2.WWIInvoiceID IS NULL
		ORDER BY
			S.WWIInvoiceID,
			S.WWIInvoiceLineID

		INSERT INTO dbo.WarehouseHistory
		(
			TableName,
			LoadDate,
			Status,
			Details
		)
		VALUES
		(
			'FctSale',
			@NewCutoff,
			'Successful',
			NULL
		)

	COMMIT TRAN

	IF OBJECT_ID('tempdb..#FctSale') IS NOT NULL
		DROP TABLE #FctSale;

END;
GO
/****** Object:  StoredProcedure [dbo].[ProcessFctStockHolding]    Script Date: 10/13/2025 10:24:08 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO






/*

EXEC [dbo].[ProcessFctStockHolding] '12/31/2012', '1/1/2013'

*/
CREATE PROCEDURE [dbo].[ProcessFctStockHolding]
(
	@InitialLoad DATETIME,
	@NewCutoff DATETIME
)
WITH EXECUTE AS OWNER
AS
BEGIN

    SET NOCOUNT ON;
    SET XACT_ABORT ON;

	IF OBJECT_ID (N'FctStockHolding', N'U') IS NULL 
	BEGIN

		CREATE TABLE [dbo].[FctStockHolding]
		(
			[StockHoldingKey] [bigint] IDENTITY(1,1) NOT NULL,
			[StockItemKey] [int] NOT NULL,
			[QuantityOnHand] [int] NOT NULL,
			[BinLocation] [nvarchar](20) NOT NULL,
			[LastStocktakeQuantity] [int] NOT NULL,
			[LastCostPrice] [decimal](18, 2) NOT NULL,
			[ReorderLevel] [int] NOT NULL,
			[TargetStockLevel] [int] NOT NULL,
			[LoadDate] [datetime2](7) NOT NULL,
			CONSTRAINT [PK_FctStockHolding] PRIMARY KEY NONCLUSTERED 
			(
				[StockHoldingKey] ASC
			)
			WITH 
			(
				STATISTICS_NORECOMPUTE = OFF, 
				IGNORE_DUP_KEY = OFF, 
				OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF
			) ON [PRIMARY]
		) ON [PRIMARY]

		ALTER TABLE [dbo].[FctStockHolding]  WITH CHECK ADD  CONSTRAINT [FK_FctStockHolding_StockItemKey_DimStockItem] FOREIGN KEY([StockItemKey])
		REFERENCES [dbo].[DimStockItem] ([StockItemKey])

		ALTER TABLE [dbo].[FctStockHolding] CHECK CONSTRAINT [FK_FctStockHolding_StockItemKey_DimStockItem]
		

	END

	DECLARE @LastCutoff DATETIME

	IF OBJECT_ID('tempdb..#FctStockHolding') IS NOT NULL
		DROP TABLE #FctStockHolding;

	SELECT TOP 1
		@LastCutoff = WH.LoadDate
	FROM
		dbo.WarehouseHistory WH
	WHERE
		WH.Status = 'Successful' AND
		TableName = 'FctStockHolding'
	ORDER BY
		WH.LoadDate DESC

	SET @LastCutoff = ISNULL(@LastCutoff, @InitialLoad)

	BEGIN TRAN

		;WITH Final AS (

			SELECT
				[StockItemKey] = ISNULL(SI.StockItemKey, 0),
				SIH.QuantityOnHand,
				SIH.BinLocation,
				SIH.LastStocktakeQuantity,
				SIH.LastCostPrice,
				SIH.ReorderLevel,
				SIH.TargetStockLevel
			FROM 
				dbo.Warehouse_StockItemHoldings SIH JOIN
				dbo.DimStockItem SI ON
					SI.WWIStockItemID = SIH.StockItemID
			WHERE 
				(
					SIH.LastEditedWhen > @LastCutoff OR
					(
						@NewCutoff = @InitialLoad AND
						SIH.LastEditedWhen >= @NewCutoff
					)
				) AND
				SIH.LastEditedWhen <= @NewCutoff
		)

		SELECT 
			*
		INTO
			#FctStockHolding
		FROM
			Final

		-- Update Existing
		UPDATE
			SH2
		SET
			SH2.StockItemKey = SH.StockItemKey,
			SH2.QuantityOnHand = SH.QuantityOnHand,
			SH2.BinLocation = SH.BinLocation,
			SH2.LastStocktakeQuantity = SH.LastStocktakeQuantity,
			SH2.LastCostPrice = SH.LastCostPrice,
			SH2.ReorderLevel = SH.ReorderLevel,
			SH2.TargetStockLevel = SH.TargetStockLevel,
			SH2.LoadDate = @NewCutoff
		FROM
			#FctStockHolding SH JOIN
			dbo.FctStockHolding SH2 ON
				SH2.StockItemKey = SH.StockItemKey

		-- Insert New
		INSERT INTO dbo.FctStockHolding
		(
			StockItemKey,
			QuantityOnHand,
			BinLocation,
			LastStocktakeQuantity,
			LastCostPrice,
			ReorderLevel,
			TargetStockLevel,
			LoadDate
		)
		SELECT
			SH.StockItemKey,
			SH.QuantityOnHand,
			SH.BinLocation,
			SH.LastStocktakeQuantity,
			SH.LastCostPrice,
			SH.ReorderLevel,
			SH.TargetStockLevel,
			@NewCutoff
		FROM
			#FctStockHolding SH LEFT JOIN
			dbo.FctStockHolding SH2 ON
				SH2.StockItemKey = SH.StockItemKey
		WHERE
			SH2.StockItemKey IS NULL
		ORDER BY
			SH.StockItemKey

		INSERT INTO dbo.WarehouseHistory
		(
			TableName,
			LoadDate,
			Status,
			Details
		)
		VALUES
		(
			'FctStockHolding',
			@NewCutoff,
			'Successful',
			NULL
		)

	COMMIT TRAN

	IF OBJECT_ID('tempdb..#FctStockHolding') IS NOT NULL
		DROP TABLE #FctStockHolding;

END;
GO
/****** Object:  StoredProcedure [dbo].[ProcessFctTransaction]    Script Date: 10/13/2025 10:24:08 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO






/*

EXEC [dbo].[ProcessFctTransaction] '12/31/2012', '1/1/2013'

*/
CREATE PROCEDURE [dbo].[ProcessFctTransaction]
(
	@InitialLoad DATETIME,
	@NewCutoff DATETIME
)
WITH EXECUTE AS OWNER
AS
BEGIN

    SET NOCOUNT ON;
    SET XACT_ABORT ON;

	IF OBJECT_ID (N'FctTransaction', N'U') IS NULL 
	BEGIN

		CREATE TABLE [dbo].[FctTransaction]
		(
			[TransactionKey] [bigint] IDENTITY(1,1) NOT NULL,
			[DateKey] [date] NOT NULL,
			[CustomerKey] [int] NULL,
			[BillToCustomerKey] [int] NULL,
			[SupplierKey] [int] NULL,
			[TransactionTypeKey] [int] NOT NULL,
			[PaymentMethodKey] [int] NULL,
			[WWICustomerTransactionID] [int] NULL,
			[WWISupplierTransactionID] [int] NULL,
			[WWIInvoiceID] [int] NULL,
			[WWIPurchaseOrderID] [int] NULL,
			[SupplierInvoiceNumber] [nvarchar](20) NULL,
			[TotalExcludingTax] [decimal](18, 2) NOT NULL,
			[TaxAmount] [decimal](18, 2) NOT NULL,
			[TotalIncludingTax] [decimal](18, 2) NOT NULL,
			[OutstandingBalance] [decimal](18, 2) NOT NULL,
			[IsFinalized] [bit] NOT NULL,
			[LoadDate] [datetime2](7) NOT NULL,
			CONSTRAINT [PK_FctTransaction] PRIMARY KEY NONCLUSTERED 
			(
				[TransactionKey] ASC,
				[DateKey] ASC
			)
			WITH 
			(
				STATISTICS_NORECOMPUTE = OFF, 
				IGNORE_DUP_KEY = OFF, 
				OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF
			) ON [PRIMARY]
		) ON [PRIMARY]

		ALTER TABLE [dbo].[FctTransaction]  WITH CHECK ADD  CONSTRAINT [FK_FctTransaction_BillToCustomerKey_DimCustomer] FOREIGN KEY([BillToCustomerKey])
		REFERENCES [dbo].[DimCustomer] ([CustomerKey])

		ALTER TABLE [dbo].[FctTransaction] CHECK CONSTRAINT [FK_FctTransaction_BillToCustomerKey_DimCustomer]

		ALTER TABLE [dbo].[FctTransaction]  WITH CHECK ADD  CONSTRAINT [FK_FctTransaction_CustomerKey_DimCustomer] FOREIGN KEY([CustomerKey])
		REFERENCES [dbo].[DimCustomer] ([CustomerKey])

		ALTER TABLE [dbo].[FctTransaction] CHECK CONSTRAINT [FK_FctTransaction_CustomerKey_DimCustomer]

		ALTER TABLE [dbo].[FctTransaction]  WITH CHECK ADD  CONSTRAINT [FK_FctTransaction_DateKey_DimDate] FOREIGN KEY([DateKey])
		REFERENCES [dbo].[DimDate] ([Date])

	END

	DECLARE @LastCutoff DATETIME

	IF OBJECT_ID('tempdb..#FctTransaction') IS NOT NULL
		DROP TABLE #FctTransaction;

	SELECT TOP 1
		@LastCutoff = WH.LoadDate
	FROM
		dbo.WarehouseHistory WH
	WHERE
		WH.Status = 'Successful' AND
		TableName = 'FctTransaction'
	ORDER BY
		WH.LoadDate DESC

	SET @LastCutoff = ISNULL(@LastCutoff, @InitialLoad)

	BEGIN TRAN

		;WITH Final AS (

			SELECT 
				[DateKey] = CAST(CT.TransactionDate AS DATE),
				C.CustomerKey,
				[BillToCustomerKey] = BC.CustomerKey,
				[SupplierKey] = CAST(NULL AS INT),
				[TransactionTypeKey] = ISNULL(TT.TransactionTypeKey, 0),
				PM.PaymentMethodKey,
				[WWICustomerTransactionID] = CT.CustomerTransactionID,
				[WWISupplierTransactionID] = CAST(NULL AS INT),
				[WWIInvoiceID] = CT.InvoiceID,
				[WWIPurchaseOrderID] = CAST(NULL AS INT),
				[SupplierInvoiceNumber] = CAST(NULL AS NVARCHAR(20)),
				[TotalExcludingTax] = CT.AmountExcludingTax,
				CT.TaxAmount,
				[TotalIncludingTax] = CT.TransactionAmount,
				CT.OutstandingBalance,
				CT.IsFinalized
			FROM 
				dbo.Sales_CustomerTransactions CT LEFT JOIN 
				dbo.Sales_Invoices I ON 
					CT.InvoiceID = I.InvoiceID LEFT JOIN
				dbo.DimCustomer C ON 
					COALESCE(I.CustomerID, CT.CustomerID) = C.WWICustomerID LEFT JOIN
				dbo.DimCustomer BC ON
					CT.CustomerID = BC.WWICustomerID LEFT JOIN
				dbo.DimTransactionType TT ON
					CT.TransactionTypeID = TT.WWITransactionTypeID LEFT JOIN
				dbo.DimPaymentMethod PM ON
					CT.PaymentMethodID = PM.WWIPaymentMethodID

			WHERE 
				CT.LastEditedWhen > @LastCutoff AND
				CT.LastEditedWhen <= @NewCutoff
		
			UNION ALL
			
			SELECT 
				CAST(ST.TransactionDate AS DATE),
				CAST(NULL AS INT),
				CAST(NULL AS INT),
				S.SupplierKey,
				ISNULL(TT.TransactionTypeKey, 0),
				PM.PaymentMethodKey,
				CAST(NULL AS INT),
				ST.SupplierTransactionID,
				CAST(NULL AS INT),
				ST.PurchaseOrderID,
				ST.SupplierInvoiceNumber,
				ST.AmountExcludingTax,
				ST.TaxAmount,
				ST.TransactionAmount,
				ST.OutstandingBalance,
				ST.IsFinalized
			FROM 
				dbo.Purchasing_SupplierTransactions ST LEFT JOIN
				dbo.DimSupplier S ON
					ST.SupplierID = S.WWISupplierID LEFT JOIN
				dbo.DimTransactionType TT ON
					ST.TransactionTypeID = TT.WWITransactionTypeID LEFT JOIN
				dbo.DimPaymentMethod PM ON
					ST.PaymentMethodID = PM.WWIPaymentMethodID
			WHERE 
				ST.LastEditedWhen > @LastCutoff AND
				ST.LastEditedWhen <= @NewCutoff

		)

		SELECT 
			*
		INTO
			#FctTransaction
		FROM
			Final

		-- Update Existing
		UPDATE
			T2
		SET
			T2.DateKey = T.DateKey,
			T2.CustomerKey = T.CustomerKey,
			T2.BillToCustomerKey = T.BillToCustomerKey,
			T2.SupplierKey = T.SupplierKey,
			T2.TransactionTypeKey = T.TransactionTypeKey,
			T2.PaymentMethodKey = T.PaymentMethodKey,
			T2.WWIInvoiceID = T.WWIInvoiceID,
			T2.WWIPurchaseOrderID = T.WWIPurchaseOrderID,
			T2.SupplierInvoiceNumber = T.SupplierInvoiceNumber,
			T2.TotalExcludingTax = T.TotalExcludingTax,
			T2.TaxAmount = T.TaxAmount,
			T2.TotalIncludingTax = T.TotalIncludingTax,
			T2.OutstandingBalance = T.OutstandingBalance,
			T2.IsFinalized = T.IsFinalized,
			T2.LoadDate = @NewCutoff
		FROM
			#FctTransaction T JOIN
			dbo.FctTransaction T2 ON
				ISNULL(T2.WWICustomerTransactionID, 0) = ISNULL(T.WWICustomerTransactionID, 0) AND
				ISNULL(T2.WWISupplierTransactionID, 0) = ISNULL(T.WWISupplierTransactionID, 0)

		-- Insert New
		INSERT INTO dbo.FctTransaction
		(
			DateKey,
			CustomerKey,
			BillToCustomerKey,
			SupplierKey,
			TransactionTypeKey,
			PaymentMethodKey,
			WWICustomerTransactionID,
			WWISupplierTransactionID,
			WWIInvoiceID,
			WWIPurchaseOrderID,
			SupplierInvoiceNumber,
			TotalExcludingTax,
			TaxAmount,
			TotalIncludingTax,
			OutstandingBalance,
			IsFinalized,
			LoadDate
		)
		SELECT
			T.DateKey,
			T.CustomerKey,
			T.BillToCustomerKey,
			T.SupplierKey,
			T.TransactionTypeKey,
			T.PaymentMethodKey,
			T.WWICustomerTransactionID,
			T.WWISupplierTransactionID,
			T.WWIInvoiceID,
			T.WWIPurchaseOrderID,
			T.SupplierInvoiceNumber,
			T.TotalExcludingTax,
			T.TaxAmount,
			T.TotalIncludingTax,
			T.OutstandingBalance,
			T.IsFinalized,
			@NewCutoff
		FROM
			#FctTransaction T LEFT JOIN
			dbo.FctTransaction T2 ON
				ISNULL(T2.WWICustomerTransactionID, 0) = ISNULL(T.WWICustomerTransactionID, 0) AND
				ISNULL(T2.WWISupplierTransactionID, 0) = ISNULL(T.WWISupplierTransactionID, 0)
		WHERE
			T2.WWICustomerTransactionID IS NULL AND
			T2.WWISupplierTransactionID IS NULL
		ORDER BY
			T.DateKey

		INSERT INTO dbo.WarehouseHistory
		(
			TableName,
			LoadDate,
			Status,
			Details
		)
		VALUES
		(
			'FctTransaction',
			@NewCutoff,
			'Successful',
			NULL
		)

	COMMIT TRAN

	IF OBJECT_ID('tempdb..#FctTransaction') IS NOT NULL
		DROP TABLE #FctTransaction;

END;
GO
/****** Object:  StoredProcedure [dbo].[ValidateDimCityData]    Script Date: 10/13/2025 10:24:08 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO









/***

EXEC [dbo].[ValidateDimCityData] 
	@Process='WarehouseWideWorldImporters', 
	@CutoffDate='10/12/2025',
	@Schema='dbo',
	@Table='DimCity'

SELECT * FROM [dbo].[DataValidationErrors]

***/
CREATE PROCEDURE [dbo].[ValidateDimCityData]
(
	@Process NVARCHAR(50),
	@CutoffDate DATETIME,
	@Schema NVARCHAR(50),
	@Table NVARCHAR(50),
	@PurgeExisting BIT = 1
)
WITH EXECUTE AS OWNER
AS
BEGIN

    SET NOCOUNT ON;
    SET XACT_ABORT ON;

	DECLARE @MismatchCountError NVARCHAR(50) = 'Mismatch in record count'
	DECLARE @MismatchValueError NVARCHAR(50) = 'Mismatch in data values'

	BEGIN TRAN

	IF @PurgeExisting = 1 
	BEGIN

		DELETE
			[dbo].[DataValidationErrors]
		WHERE
			[CutoffDate] = @CutoffDate AND
			[Error] IN (@MismatchCountError, @MismatchValueError) AND
			[Schema] = @Schema AND
			[Table] = @Table AND
			[Process] = @Process

	END

	-- Main data validation
    ;WITH DimCityOriginal AS
	(

		SELECT
			[WWICityID] = C.CityID,
			[City] = C.CityName,
			[StateProvince] = SP.StateProvinceName,
			[Country] = CA.CountryName,
			[Continent] = CA.Continent,
			[SalesTerritory] = SP.SalesTerritory,
			[Region] = CA.Region,
			[Subregion] = CA.Subregion,
			[Location] = CAST(C.Location AS NVARCHAR(MAX)),
			[LatestRecordedPopulation] = C.LatestRecordedPopulation
		FROM
			(
				SELECT
					C.CityID,
					C.CityName,
					[Location] = CAST(C.Location AS NVARCHAR(100)),
					[LatestRecordedPopulation] = ISNULL(C.LatestRecordedPopulation, 0),
					C.StateProvinceID
				FROM
					dbo.Application_Cities C
				WHERE
					@CutoffDate BETWEEN C.ValidFrom AND C.ValidTo 

				UNION

				SELECT
					CA.CityID,
					CA.CityName,
					[Location] = CAST(CA.Location AS NVARCHAR(100)),
					[LatestRecordedPopulation] = ISNULL(CA.LatestRecordedPopulation, 0),
					CA.StateProvinceID
				FROM
					dbo.Application_Cities_Archive CA
				WHERE
					@CutoffDate BETWEEN CA.ValidFrom AND CA.ValidTo 
			) C LEFT JOIN
			(
				SELECT
					SP.StateProvinceID,
					SP.CountryID,
					SP.StateProvinceName,
					SP.SalesTerritory
				FROM
					dbo.Application_StateProvinces SP
				WHERE
					@CutoffDate BETWEEN SP.ValidFrom AND SP.ValidTo 

				UNION

				SELECT
					SPA.StateProvinceID,
					SPA.CountryID,
					SPA.StateProvinceName,
					SPA.SalesTerritory
				FROM
					dbo.Application_StateProvinces_Archive SPA
				WHERE
					@CutoffDate BETWEEN SPA.ValidFrom AND SPA.ValidTo
			) SP ON
				SP.StateProvinceID = C.StateProvinceID LEFT JOIN
			(
				SELECT
					C.CountryID,
					C.CountryName,
					C.Continent,
					C.Region,
					C.Subregion
				FROM
					dbo.Application_Countries C
				WHERE
					@CutoffDate BETWEEN C.ValidFrom AND C.ValidTo 

				UNION

				SELECT
					CA.CountryID,
					CA.CountryName,
					CA.Continent,
					CA.Region,
					CA.Subregion
				FROM
					dbo.Application_Countries_Archive CA
				WHERE
					@CutoffDate BETWEEN CA.ValidFrom AND CA.ValidTo
			) CA ON
				CA.CountryID = SP.CountryID

	),

	DimCityWarehouse AS 
	(

		SELECT
			*
		FROM
			dbo.DimCity
		WHERE
			LoadDate <= @CutoffDate AND
			CityKey > 0

	),

	MismatchCount AS
	(

		SELECT
			[Error] = @MismatchCountError,
			[Details] = 'Non-warehouse tables has ' + CAST(O.[Count] AS NVARCHAR(50)) + ' records compared to warehouse table with ' + CAST(W.[Count] AS NVARCHAR(50)) + ' records.'
		FROM
			(
				SELECT
					[Count] = COUNT(*)
				FROM		
					DimCityOriginal 				
			) O LEFT JOIN
			(
				SELECT
					[Count] = COUNT(*)
				FROM
					DimCityWarehouse
			) W ON 1 = 1
		WHERE
			O.[Count] <> W.[Count]

	),

	MismatchValues AS
	(
		SELECT
			[Error] = @MismatchValueError,
			[Details] = 'There are ' + CAST(M.[Count] AS NVARCHAR(50)) + ' records with mismatched values.'
		FROM
		(	
			SELECT TOP 1
				*
			FROM
			(
				SELECT
					[Count] = COUNT(*)
				FROM
					DimCityOriginal O LEFT JOIN
					DimCityWarehouse W ON
						W.WWICityID = O.WWICityID
				WHERE
					O.[City] <> W.[City] OR
					O.[StateProvince] <> W.[StateProvince] OR
					O.[Country] <> W.[Country] OR
					O.[Continent] <> W.[Continent] OR
					O.[SalesTerritory] <> W.[SalesTerritory] OR
					O.[Region] <> W.[Region] OR
					O.[Subregion] <> W.[Subregion] OR
					ISNULL(CAST(O.[Location] AS VARBINARY(MAX)), CAST('' AS VARBINARY(MAX))) <> ISNULL(CAST(W.[Location] AS VARBINARY(MAX)), CAST('' AS VARBINARY(MAX))) OR
					O.[LatestRecordedPopulation] <> W.[LatestRecordedPopulation]
			) M 
		) M
		WHERE
			M.[Count] > 0
	)

	INSERT INTO [dbo].[DataValidationErrors]
	(
		[CutoffDate],
		[Error],
		[Details],
		[Schema],
		[Table],
		[Process]
	)
	SELECT
		@CutoffDate,
		Error,
		Details,
		@Schema,
		@Table,
		@Process
	FROM
		MismatchCount
	
	UNION

	SELECT
		@CutoffDate,
		Error,
		Details,
		@Schema,
		@Table,
		@Process
	FROM
		MismatchValues

	COMMIT TRAN

END;
GO
/****** Object:  StoredProcedure [dbo].[ValidateDimCustomerData]    Script Date: 10/13/2025 10:24:08 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO






/***

EXEC [dbo].[ValidateDimCustomerData] 
	@Process='WarehouseWideWorldImporters', 
	@CutoffDate='10/12/2025',
	@Schema='dbo',
	@Table='DimCustomer'

SELECT * FROM [dbo].[DataValidationErrors]

***/
CREATE PROCEDURE [dbo].[ValidateDimCustomerData]
(
	@Process NVARCHAR(50),
	@CutoffDate DATETIME,
	@Schema NVARCHAR(50),
	@Table NVARCHAR(50),
	@PurgeExisting BIT = 1
)
WITH EXECUTE AS OWNER
AS
BEGIN

    SET NOCOUNT ON;
    SET XACT_ABORT ON;

	DECLARE @MismatchCountError NVARCHAR(50) = 'Mismatch in record count'
	DECLARE @MismatchValueError NVARCHAR(50) = 'Mismatch in data values'

	BEGIN TRAN

	IF @PurgeExisting = 1 
	BEGIN

		DELETE
			[dbo].[DataValidationErrors]
		WHERE
			[CutoffDate] = @CutoffDate AND
			[Error] IN (@MismatchCountError, @MismatchValueError) AND
			[Schema] = @Schema AND
			[Table] = @Table AND
			[Process] = @Process

	END

	-- Main data validation
    ;WITH DimCustomerOriginal AS
	(

		SELECT
			[WWICustomerID] = C.CustomerID,
			[WWIDeliveryCityID] = C.DeliveryCityID,
			[Customer] = C.CustomerName,
			[BillToCustomer] = BC.CustomerName,
			[Category] = CC.CustomerCategoryName,
			[BuyingGroup] = BG.BuyingGroupName,
			[PrimaryContact] = PA.FullName,
			[PostalCode] = C.DeliveryPostalCode
		FROM
			(
				SELECT 
					C.CustomerID,
					C.BillToCustomerID,
					C.CustomerCategoryID,
					C.PrimaryContactPersonID,
					C.BuyingGroupID,
					C.CustomerName,
					C.DeliveryPostalCode,
					C.DeliveryCityID
				FROM
					dbo.Sales_Customers C
				WHERE
					@CutoffDate BETWEEN C.ValidFrom AND C.ValidTo

				UNION

				SELECT 
					CA.CustomerID,
					CA.BillToCustomerID,
					CA.CustomerCategoryID,
					CA.PrimaryContactPersonID,
					CA.BuyingGroupID,
					CA.CustomerName,
					CA.DeliveryPostalCode,
					CA.DeliveryCityID
				FROM
					dbo.Sales_Customers_Archive CA
				WHERE
					@CutoffDate BETWEEN CA.ValidFrom AND CA.ValidTo
			) C LEFT JOIN
			(
				SELECT 
					C.CustomerID,
					C.CustomerName,
					C.DeliveryPostalCode
				FROM
					dbo.Sales_Customers C
				WHERE
					@CutoffDate BETWEEN C.ValidFrom AND C.ValidTo

				UNION

				SELECT 
					CA.CustomerID,
					CA.CustomerName,
					CA.DeliveryPostalCode
				FROM
					dbo.Sales_Customers_Archive CA
				WHERE
					@CutoffDate BETWEEN CA.ValidFrom AND CA.ValidTo
			) BC ON
				BC.CustomerID = C.BillToCustomerID LEFT JOIN
			(
				SELECT
					CC.CustomerCategoryID,
					CC.CustomerCategoryName
				FROM
					dbo.Sales_CustomerCategories CC 
				WHERE
					@CutoffDate BETWEEN CC.ValidFrom AND CC.ValidTo

				UNION

				SELECT
					CCA.CustomerCategoryID,
					CCA.CustomerCategoryName
				FROM
					dbo.Sales_CustomerCategories_Archive CCA
				WHERE
					@CutoffDate BETWEEN CCA.ValidFrom AND CCA.ValidTo
			) CC ON
				CC.CustomerCategoryID = C.CustomerCategoryID LEFT JOIN
			(
				SELECT
					P.PersonID,
					P.FullName
				FROM
					dbo.Application_People P 
				WHERE
					@CutoffDate BETWEEN P.ValidFrom AND P.ValidTo

				UNION

				SELECT
					PA.PersonID,
					PA.FullName
				FROM
					dbo.Application_People_Archive PA
				WHERE
					@CutoffDate BETWEEN PA.ValidFrom AND PA.ValidTo
			) PA ON
				PA.PersonID = C.PrimaryContactPersonID LEFT JOIN
			(
				SELECT
					BG.BuyingGroupID,
					BG.BuyingGroupName
				FROM
					dbo.Sales_BuyingGroups BG 
				WHERE
					@CutoffDate BETWEEN BG.ValidFrom AND BG.ValidTo

				UNION

				SELECT
					BGA.BuyingGroupID,
					BGA.BuyingGroupName
				FROM
					dbo.Sales_BuyingGroups_Archive BGA
				WHERE
					@CutoffDate BETWEEN BGA.ValidFrom AND BGA.ValidTo
			) BG ON
				BG.BuyingGroupID = C.BuyingGroupID

	),

	DimCustomerWarehouse AS 
	(

		SELECT
			*
		FROM
			dbo.DimCustomer
		WHERE
			LoadDate <= @CutoffDate AND
			CustomerKey > 0

	),

	MismatchCount AS
	(

		SELECT
			[Error] = @MismatchCountError,
			[Details] = 'Non-warehouse tables has ' + CAST(O.[Count] AS NVARCHAR(50)) + ' records compared to warehouse table with ' + CAST(W.[Count] AS NVARCHAR(50)) + ' records.'
		FROM
			(
				SELECT
					[Count] = COUNT(*)
				FROM		
					DimCustomerOriginal
			) O LEFT JOIN
			(
				SELECT
					[Count] = COUNT(*)
				FROM
					DimCustomerWarehouse
			) W ON 1 = 1
		WHERE
			O.[Count] <> W.[Count]

	),

	MismatchValues AS
	(
		SELECT
			[Error] = @MismatchValueError,
			[Details] = 'There are ' + CAST(M.[Count] AS NVARCHAR(50)) + ' records with mismatched values.'
		FROM
		(	
			SELECT TOP 1
				*
			FROM
			(
				SELECT
					[Count] = COUNT(*)
				FROM
					DimCustomerOriginal O LEFT JOIN
					DimCustomerWarehouse W ON
						W.WWICustomerID = O.WWICustomerID 
				WHERE
					O.[WWICustomerID] <> W.[WWICustomerID] OR
					O.[WWIDeliveryCityID] <> W.[WWIDeliveryCityID] OR
					O.[Customer] <> W.[Customer] OR
					O.[BillToCustomer] <> W.[BillToCustomer] OR
					O.[Category] <> W.[Category] OR
					O.[BuyingGroup] <> W.[BuyingGroup] OR
					O.[PrimaryContact] <> W.[PrimaryContact] OR
					O.[PostalCode] <> W.[PostalCode]
			) M 
		) M
		WHERE
			M.[Count] > 0
	)

	INSERT INTO [dbo].[DataValidationErrors]
	(
		[CutoffDate],
		[Error],
		[Details],
		[Schema],
		[Table],
		[Process]
	)
	SELECT
		@CutoffDate,
		Error,
		Details,
		@Schema,
		@Table,
		@Process
	FROM
		MismatchCount
	
	UNION

	SELECT
		@CutoffDate,
		Error,
		Details,
		@Schema,
		@Table,
		@Process
	FROM
		MismatchValues

	COMMIT TRAN

END;
GO
/****** Object:  StoredProcedure [dbo].[ValidateDimDateData]    Script Date: 10/13/2025 10:24:08 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO








/***

EXEC [dbo].[ValidateDimDateData] 
	@Process='WarehouseWideWorldImporters', 
	@CutoffDate='1/1/2013',
	@Schema='dbo',
	@Table='DimDate',
	@PurgeExisting=1

SELECT * FROM [dbo].[DataValidationErrors]

***/
CREATE PROCEDURE [dbo].[ValidateDimDateData]
(
	@Process NVARCHAR(50),
	@CutoffDate DATETIME,
	@Schema NVARCHAR(50),
	@Table NVARCHAR(50),
	@PurgeExisting BIT = 1
)
WITH EXECUTE AS OWNER
AS
BEGIN

    SET NOCOUNT ON;
    SET XACT_ABORT ON;

	DECLARE @InitialLoad DATE = '12/31/2012'
	DECLARE @MismatchCountError NVARCHAR(50) = 'Mismatch in record count'
	
	BEGIN TRAN

	IF @PurgeExisting = 1 
	BEGIN

		DELETE
			[dbo].[DataValidationErrors]
		WHERE
			[CutoffDate] = @CutoffDate AND
			[Error] = @MismatchCountError AND
			[Schema] = @Schema AND
			[Table] = @Table AND
			[Process] = @Process

	END

	-- Main data validation
    ;WITH MismatchCount AS
	(

		SELECT
			[Error] = @MismatchCountError,
			[Details] = 'Non-warehouse tables has ' + CAST(O.[Count] AS NVARCHAR(50)) + ' records compared to warehouse table with ' + CAST(W.[Count] AS NVARCHAR(50)) + ' records.'
		FROM
			(
				SELECT
					[Count] = DATEDIFF(DAY, @InitialLoad, DATEADD(DAY, 31, @CutoffDate))
			) O LEFT JOIN
			(
				SELECT
					[Count] = COUNT(*)
				FROM
					dbo.DimDate 
				WHERE
					[LoadDate] <= @CutoffDate
			) W ON 1 = 1
		WHERE
			O.[Count] <> W.[Count]

	)

	INSERT INTO [dbo].[DataValidationErrors]
	(
		[CutoffDate],
		[Error],
		[Details],
		[Schema],
		[Table],
		[Process]
	)
	SELECT
		@CutoffDate,
		Error,
		Details,
		@Schema,
		@Table,
		@Process
	FROM
		MismatchCount
	
	COMMIT TRAN

END;
GO
/****** Object:  StoredProcedure [dbo].[ValidateDimEmployeeData]    Script Date: 10/13/2025 10:24:08 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO






/***

EXEC [dbo].[ValidateDimEmployeeData] 
	@Process='WarehouseWideWorldImporters', 
	@CutoffDate='1/1/2013',
	@Schema='dbo',
	@Table='DimPaymentMethod'

SELECT * FROM [dbo].[DataValidationErrors]

***/
CREATE PROCEDURE [dbo].[ValidateDimEmployeeData]
(
	@Process NVARCHAR(50),
	@CutoffDate DATETIME,
	@Schema NVARCHAR(50),
	@Table NVARCHAR(50),
	@PurgeExisting BIT = 1
)
WITH EXECUTE AS OWNER
AS
BEGIN

    SET NOCOUNT ON;
    SET XACT_ABORT ON;

	DECLARE @MismatchCountError NVARCHAR(50) = 'Mismatch in record count'
	DECLARE @MismatchValueError NVARCHAR(50) = 'Mismatch in data values'

	BEGIN TRAN

	IF @PurgeExisting = 1 
	BEGIN

		DELETE
			[dbo].[DataValidationErrors]
		WHERE
			[CutoffDate] = @CutoffDate AND
			[Error] IN (@MismatchCountError, @MismatchValueError) AND
			[Schema] = @Schema AND
			[Table] = @Table AND
			[Process] = @Process

	END

	-- Main data validation
    ;WITH DimEmployeeOriginal AS
	(

		SELECT
			[WWIEmployeeID] = E.PersonID,
			[Employee] = E.FullName,
			[PreferredName] = E.PreferredName,
			[IsSalesPerson] = E.IsSalesperson,
			[Photo] = E.Photo
		FROM
			(
				SELECT 
					P.PersonID,
					P.FullName,
					P.PreferredName,
					P.IsSalesperson,
					P.Photo
				FROM 
					dbo.Application_People P 
				WHERE
					P.IsEmployee = 1 AND
					@CutoffDate BETWEEN P.ValidFrom AND P.ValidTo

				UNION

				SELECT 
					PA.PersonID,
					PA.FullName,
					PA.PreferredName,
					PA.IsSalesperson,
					PA.Photo
				FROM 
					dbo.Application_People_Archive PA 
				WHERE
					PA.IsEmployee = 1 AND
					@CutoffDate BETWEEN PA.ValidFrom AND PA.ValidTo
			) E

	),

	DimEmployeeWarehouse AS 
	(

		SELECT
			*
		FROM
			dbo.DimEmployee
		WHERE
			LoadDate <= @CutoffDate AND
			EmployeeKey > 0
	),

	MismatchCount AS
	(

		SELECT
			[Error] = @MismatchCountError,
			[Details] = 'Non-warehouse tables has ' + CAST(O.[Count] AS NVARCHAR(50)) + ' records compared to warehouse table with ' + CAST(W.[Count] AS NVARCHAR(50)) + ' records.'
		FROM
			(
				SELECT
					[Count] = COUNT(*)
				FROM		
					DimEmployeeOriginal
			) O LEFT JOIN
			(
				SELECT
					[Count] = COUNT(*)
				FROM
					DimEmployeeWarehouse
			) W ON 1 = 1
		WHERE
			O.[Count] <> W.[Count]

	),

	MismatchValues AS
	(
		SELECT
			[Error] = @MismatchValueError,
			[Details] = 'There are ' + CAST(M.[Count] AS NVARCHAR(50)) + ' records with mismatched values.'
		FROM
		(	
			SELECT TOP 1
				*
			FROM
			(
				SELECT
					[Count] = COUNT(*)
				FROM
					DimEmployeeOriginal O LEFT JOIN
					DimEmployeeWarehouse W ON
						W.WWIEmployeeID = O.WWIEmployeeID 
				WHERE
					O.[Employee] <> W.[Employee] OR
					O.[PreferredName] <> W.[PreferredName] OR
					O.[IsSalesPerson] <> W.[IsSalesPerson] OR
					ISNULL(O.[Photo], CAST('' AS VARBINARY(MAX))) <> ISNULL(W.[Photo], CAST('' AS VARBINARY(MAX)))
			) M 
		) M
		WHERE
			M.[Count] > 0
	)

	INSERT INTO [dbo].[DataValidationErrors]
	(
		[CutoffDate],
		[Error],
		[Details],
		[Schema],
		[Table],
		[Process]
	)
	SELECT
		@CutoffDate,
		Error,
		Details,
		@Schema,
		@Table,
		@Process
	FROM
		MismatchCount
	
	UNION

	SELECT
		@CutoffDate,
		Error,
		Details,
		@Schema,
		@Table,
		@Process
	FROM
		MismatchValues

	COMMIT TRAN

END;
GO
/****** Object:  StoredProcedure [dbo].[ValidateDimPaymentMethodData]    Script Date: 10/13/2025 10:24:08 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO





/***

EXEC [dbo].[ValidateDimPaymentMethodData] 
	@Process='WarehouseWideWorldImporters', 
	@CutoffDate='1/1/2013',
	@Schema='dbo',
	@Table='DimPaymentMethod'

SELECT * FROM [dbo].[DataValidationErrors]

***/
CREATE PROCEDURE [dbo].[ValidateDimPaymentMethodData]
(
	@Process NVARCHAR(50),
	@CutoffDate DATETIME,
	@Schema NVARCHAR(50),
	@Table NVARCHAR(50),
	@PurgeExisting BIT = 1
)
WITH EXECUTE AS OWNER
AS
BEGIN

    SET NOCOUNT ON;
    SET XACT_ABORT ON;

	DECLARE @MismatchCountError NVARCHAR(50) = 'Mismatch in record count'
	DECLARE @MismatchValueError NVARCHAR(50) = 'Mismatch in data values'

	BEGIN TRAN

	IF @PurgeExisting = 1 
	BEGIN

		DELETE
			[dbo].[DataValidationErrors]
		WHERE
			[CutoffDate] = @CutoffDate AND
			[Error] IN (@MismatchCountError, @MismatchValueError) AND
			[Schema] = @Schema AND
			[Table] = @Table AND
			[Process] = @Process

	END

	-- Main data validation
    ;WITH DimPaymentMethodOriginal AS
	(

		SELECT
			[WWIPaymentMethodID] = PM.PaymentMethodID,
			[PaymentMethod] = PM.PaymentMethodName
		FROM
			(
				SELECT
					PM.PaymentMethodID,
					PM.PaymentMethodName
				FROM
					dbo.Application_PaymentMethods PM
				WHERE
					@CutoffDate BETWEEN PM.ValidFrom AND PM.ValidTo

				UNION

				SELECT
					PMA.PaymentMethodID,
					PMA.PaymentMethodName
				FROM
					dbo.Application_PaymentMethods_Archive PMA
				WHERE
					@CutoffDate BETWEEN PMA.ValidFrom AND PMA.ValidTo
			) PM

	),

	DimPaymentMethodWarehouse AS 
	(

		SELECT
			*
		FROM
			dbo.DimPaymentMethod
		WHERE
			LoadDate <= @CutoffDate AND
			PaymentMethodKey > 0

	),

	MismatchCount AS
	(

		SELECT
			[Error] = @MismatchCountError,
			[Details] = 'Non-warehouse tables has ' + CAST(O.[Count] AS NVARCHAR(50)) + ' records compared to warehouse table with ' + CAST(W.[Count] AS NVARCHAR(50)) + ' records.'
		FROM
			(
				SELECT
					[Count] = COUNT(*)
				FROM		
					DimPaymentMethodOriginal
			) O LEFT JOIN
			(
				SELECT
					[Count] = COUNT(*)
				FROM
					DimPaymentMethodWarehouse
			) W ON 1 = 1
		WHERE
			O.[Count] <> W.[Count]

	),

	MismatchValues AS
	(
		SELECT
			[Error] = @MismatchValueError,
			[Details] = 'There are ' + CAST(M.[Count] AS NVARCHAR(50)) + ' records with mismatched values.'
		FROM
		(	
			SELECT TOP 1
				*
			FROM
			(
				SELECT
					[Count] = COUNT(*)
				FROM
					DimPaymentMethodOriginal O LEFT JOIN
					DimPaymentMethodWarehouse W ON
						W.WWIPaymentMethodID = O.WWIPaymentMethodID
				WHERE
					O.[PaymentMethod] <> W.[PaymentMethod]
			) M 
		) M
		WHERE
			M.[Count] > 0

	)

	INSERT INTO [dbo].[DataValidationErrors]
	(
		[CutoffDate],
		[Error],
		[Details],
		[Schema],
		[Table],
		[Process]
	)
	SELECT
		@CutoffDate,
		Error,
		Details,
		@Schema,
		@Table,
		@Process
	FROM
		MismatchCount
	
	UNION

	SELECT
		@CutoffDate,
		Error,
		Details,
		@Schema,
		@Table,
		@Process
	FROM
		MismatchValues

	COMMIT TRAN

END;
GO
/****** Object:  StoredProcedure [dbo].[ValidateDimStockItemData]    Script Date: 10/13/2025 10:24:08 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO





/***

EXEC [dbo].[ValidateDimPaymentMethodData] 
	@Process='WarehouseWideWorldImporters', 
	@CutoffDate='1/1/2013',
	@Schema='dbo',
	@Table='DimStockItem'

SELECT * FROM [dbo].[DataValidationErrors]

***/
CREATE PROCEDURE [dbo].[ValidateDimStockItemData]
(
	@Process NVARCHAR(50),
	@CutoffDate DATETIME,
	@Schema NVARCHAR(50),
	@Table NVARCHAR(50),
	@PurgeExisting BIT = 1
)
WITH EXECUTE AS OWNER
AS
BEGIN

    SET NOCOUNT ON;
    SET XACT_ABORT ON;

	DECLARE @MismatchCountError NVARCHAR(50) = 'Mismatch in record count'
	DECLARE @MismatchValueError NVARCHAR(50) = 'Mismatch in data values'

	BEGIN TRAN

	IF @PurgeExisting = 1 
	BEGIN

		DELETE
			[dbo].[DataValidationErrors]
		WHERE
			[CutoffDate] = @CutoffDate AND
			[Error] IN (@MismatchCountError, @MismatchValueError) AND
			[Schema] = @Schema AND
			[Table] = @Table AND
			[Process] = @Process

	END

	-- Main data validation
    ;WITH DimStockItemOriginal AS
	(

		SELECT
			[WWIStockItemID] = SI.StockItemID,
			[StockItem] = SI.StockItemName,
			[Color] = C.ColorName,
			[SellingPackage] = SP.PackageTypeName,
			[BuyingPackage] = BP.PackageTypeName,
			SI.Brand,
			SI.Size,
			SI.LeadTimeDays,
			SI.QuantityPerOuter,
			SI.IsChillerStock,
			SI.Barcode,
			SI.TaxRate,
			SI.UnitPrice,
			SI.RecommendedRetailPrice,
			SI.TypicalWeightPerUnit,
			SI.Photo
		FROM
			(
				SELECT
					SI.StockItemID,
					SI.StockItemName,
					SI.ColorID,
					SI.OuterPackageID,
					SI.UnitPackageID,
					SI.Brand,
					SI.Size,
					SI.LeadTimeDays,
					SI.QuantityPerOuter,
					SI.IsChillerStock,
					SI.Barcode,
					SI.TaxRate,
					SI.UnitPrice,
					SI.RecommendedRetailPrice,
					SI.TypicalWeightPerUnit,
					SI.Photo
				FROM
					dbo.Warehouse_StockItems SI
				WHERE
					@CutoffDate BETWEEN SI.ValidFrom AND SI.ValidTo

				UNION

				SELECT
					SIA.StockItemID,
					SIA.StockItemName,
					SIA.ColorID,
					SIA.OuterPackageID,
					SIA.UnitPackageID,
					SIA.Brand,
					SIA.Size,
					SIA.LeadTimeDays,
					SIA.QuantityPerOuter,
					SIA.IsChillerStock,
					SIA.Barcode,
					SIA.TaxRate,
					SIA.UnitPrice,
					SIA.RecommendedRetailPrice,
					SIA.TypicalWeightPerUnit,
					SIA.Photo
				FROM
					dbo.Warehouse_StockItems_Archive SIA
				WHERE
					@CutoffDate BETWEEN SIA.ValidFrom AND SIA.ValidTo
			) SI LEFT JOIN
			(
				SELECT
					C.ColorID,
					C.ColorName
				FROM
					dbo.Warehouse_Colors C 
				WHERE
					@CutoffDate BETWEEN C.ValidFrom AND C.ValidTo

				UNION

				SELECT
					CA.ColorID,
					CA.ColorName
				FROM
					dbo.Warehouse_Colors_Archive CA 
				WHERE
					@CutoffDate BETWEEN CA.ValidFrom AND CA.ValidTo
			) C ON
				C.ColorID = SI.ColorID LEFT JOIN
			(
				SELECT
					PT.PackageTypeID,
					PT.PackageTypeName
				FROM
					dbo.Warehouse_PackageTypes PT 
				WHERE
					@CutoffDate BETWEEN PT.ValidFrom AND PT.ValidTo

				UNION

				SELECT
					PTA.PackageTypeID,
					PTA.PackageTypeName
				FROM
					dbo.Warehouse_PackageTypes_Archive PTA 
				WHERE
					@CutoffDate BETWEEN PTA.ValidFrom AND PTA.ValidTo
			) SP ON
				SP.PackageTypeID = SI.UnitPackageID LEFT JOIN
			(
				SELECT
					PT.PackageTypeID,
					PT.PackageTypeName
				FROM
					dbo.Warehouse_PackageTypes PT 
				WHERE
					@CutoffDate BETWEEN PT.ValidFrom AND PT.ValidTo

				UNION

				SELECT
					PTA.PackageTypeID,
					PTA.PackageTypeName
				FROM
					dbo.Warehouse_PackageTypes_Archive PTA 
				WHERE
					@CutoffDate BETWEEN PTA.ValidFrom AND PTA.ValidTo
			) BP ON
				BP.PackageTypeID = SI.OuterPackageID

	),

	DimStockItemWarehouse AS 
	(

		SELECT
			*
		FROM
			dbo.DimStockItem
		WHERE
			LoadDate <= @CutoffDate AND
			StockItemKey > 0

	),

	MismatchCount AS
	(

		SELECT
			[Error] = @MismatchCountError,
			[Details] = 'Non-warehouse tables has ' + CAST(O.[Count] AS NVARCHAR(50)) + ' records compared to warehouse table with ' + CAST(W.[Count] AS NVARCHAR(50)) + ' records.'
		FROM
			(
				SELECT
					[Count] = COUNT(*)
				FROM		
					DimStockItemOriginal
			) O LEFT JOIN
			(
				SELECT
					[Count] = COUNT(*)
				FROM
					DimStockItemWarehouse
			) W ON 1 = 1
		WHERE
			O.[Count] <> W.[Count]

	),

	MismatchValues AS
	(
		SELECT
			[Error] = @MismatchValueError,
			[Details] = 'There are ' + CAST(M.[Count] AS NVARCHAR(50)) + ' records with mismatched values.'
		FROM
		(	
			SELECT TOP 1
				*
			FROM
			(
				SELECT
					[Count] = COUNT(*)
				FROM
					DimStockItemOriginal O LEFT JOIN
					DimStockItemWarehouse W ON
						W.WWIStockItemID = O.WWIStockItemID
				WHERE
					O.[StockItem] <> W.[StockItem] OR
					O.[Color] <> W.[Color] OR
					O.[SellingPackage] <> W.[SellingPackage] OR
					O.[BuyingPackage] <> W.[BuyingPackage] OR
					O.[Brand] <> W.[Brand] OR
					O.[Size] <> W.[Size] OR
					O.[LeadTimeDays] <> W.[LeadTimeDays] OR 
					O.[QuantityPerOuter] <> W.[QuantityPerOuter] OR
					O.[IsChillerStock] <> W.[IsChillerStock] OR
					ISNULL(O.[Barcode], '') <> ISNULL(W.[Barcode], '') OR
					O.[TaxRate] <> W.[TaxRate] OR
					O.[UnitPrice] <> W.[UnitPrice] OR
					ISNULL(O.[RecommendedRetailPrice], 0) <> ISNULL(W.[RecommendedRetailPrice], 0) OR
					O.[TypicalWeightPerUnit] <> W.[TypicalWeightPerUnit] OR
					ISNULL(O.[Photo], CAST('' AS VARBINARY(MAX))) <> ISNULL(W.[Photo], CAST('' AS VARBINARY(MAX)))
			) M 
		) M
		WHERE
			M.[Count] > 0

	)

	INSERT INTO [dbo].[DataValidationErrors]
	(
		[CutoffDate],
		[Error],
		[Details],
		[Schema],
		[Table],
		[Process]
	)
	SELECT
		@CutoffDate,
		Error,
		Details,
		@Schema,
		@Table,
		@Process
	FROM
		MismatchCount
	
	UNION

	SELECT
		@CutoffDate,
		Error,
		Details,
		@Schema,
		@Table,
		@Process
	FROM
		MismatchValues

	COMMIT TRAN

END;
GO
/****** Object:  StoredProcedure [dbo].[ValidateDimSupplierData]    Script Date: 10/13/2025 10:24:08 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO





/***

EXEC [dbo].[ValidateDimPaymentMethodData] 
	@Process='WarehouseWideWorldImporters', 
	@CutoffDate='1/1/2013',
	@Schema='dbo',
	@Table='DimSupplier'

SELECT * FROM [dbo].[DataValidationErrors]

***/
CREATE PROCEDURE [dbo].[ValidateDimSupplierData]
(
	@Process NVARCHAR(50),
	@CutoffDate DATETIME,
	@Schema NVARCHAR(50),
	@Table NVARCHAR(50),
	@PurgeExisting BIT = 1
)
WITH EXECUTE AS OWNER
AS
BEGIN

    SET NOCOUNT ON;
    SET XACT_ABORT ON;

	DECLARE @MismatchCountError NVARCHAR(50) = 'Mismatch in record count'
	DECLARE @MismatchValueError NVARCHAR(50) = 'Mismatch in data values'

	BEGIN TRAN

	IF @PurgeExisting = 1 
	BEGIN

		DELETE
			[dbo].[DataValidationErrors]
		WHERE
			[CutoffDate] = @CutoffDate AND
			[Error] IN (@MismatchCountError, @MismatchValueError) AND
			[Schema] = @Schema AND
			[Table] = @Table AND
			[Process] = @Process

	END

	-- Main data validation
    ;WITH DimSupplierOriginal AS
	(

		SELECT
			[WWISupplierID] = S.SupplierID,
			[Supplier] = S.SupplierName,
			[Category] = SC.SupplierCategoryName,
			[PrimaryContact] = P.FullName,
			[SupplierReference] = S.SupplierReference,
			[PaymentDays] = S.PaymentDays,
			[PostalCode] = S.DeliveryPostalCode
		FROM
			(
				SELECT 
					S.SupplierID,
					S.SupplierCategoryID,
					S.PrimaryContactPersonID,
					S.SupplierName,
					S.SupplierReference,
					S.PaymentDays,
					S.DeliveryPostalCode
				FROM 
					dbo.Purchasing_Suppliers S
				WHERE
					@CutoffDate BETWEEN S.ValidFrom AND S.ValidTo

				UNION

				SELECT
					SA.SupplierID,
					SA.SupplierCategoryID,
					SA.PrimaryContactPersonID,
					SA.SupplierName,
					SA.SupplierReference,
					SA.PaymentDays,
					SA.DeliveryPostalCode
				FROM
					dbo.Purchasing_Suppliers_Archive SA
				WHERE
					@CutoffDate BETWEEN SA.ValidFrom AND SA.ValidTo
			) S LEFT JOIN
			(
				SELECT 
					SC.SupplierCategoryID,
					SC.SupplierCategoryName
				FROM
					dbo.Purchasing_SupplierCategories SC 
				WHERE
					@CutoffDate BETWEEN SC.ValidFrom AND SC.ValidTo

				UNION

				SELECT
					SCA.SupplierCategoryID,
					SCA.SupplierCategoryName
				FROM
					dbo.Purchasing_SupplierCategories_Archive SCA
				WHERE
					@CutoffDate BETWEEN SCA.ValidFrom AND SCA.ValidTo
			) SC ON
				SC.SupplierCategoryID = S.SupplierCategoryID LEFT JOIN
			(
				SELECT
					P.PersonID,
					P.FullName
				FROM
					dbo.Application_People P
				WHERE
					@CutoffDate BETWEEN P.ValidFrom AND P.ValidTo

				UNION

				SELECT
					PA.PersonID,
					PA.FullName
				FROM
					dbo.Application_People_Archive PA
				WHERE
					@CutoffDate BETWEEN PA.ValidFrom AND PA.ValidTo
			) P ON
				P.PersonID = S.PrimaryContactPersonID

	),

	DimSupplierWarehouse AS 
	(

		SELECT
			*
		FROM
			dbo.DimSupplier
		WHERE
			LoadDate <= @CutoffDate AND
			SupplierKey > 0

	),

	MismatchCount AS
	(

		SELECT
			[Error] = @MismatchCountError,
			[Details] = 'Non-warehouse tables has ' + CAST(O.[Count] AS NVARCHAR(50)) + ' records compared to warehouse table with ' + CAST(W.[Count] AS NVARCHAR(50)) + ' records.'
		FROM
			(
				SELECT
					[Count] = COUNT(*)
				FROM		
					DimSupplierOriginal
			) O LEFT JOIN
			(
				SELECT
					[Count] = COUNT(*)
				FROM
					DimSupplierWarehouse
			) W ON 1 = 1
		WHERE
			O.[Count] <> W.[Count]

	),

	MismatchValues AS
	(
		SELECT
			[Error] = @MismatchValueError,
			[Details] = 'There are ' + CAST(M.[Count] AS NVARCHAR(50)) + ' records with mismatched values.'
		FROM
		(	
			SELECT TOP 1
				*
			FROM
			(
				SELECT
					[Count] = COUNT(*)
				FROM
					DimSupplierOriginal O LEFT JOIN
					DimSupplierWarehouse W ON
						W.WWISupplierID = O.WWISupplierID
				WHERE
					O.[Supplier] <> W.[Supplier] OR
					O.[Category] <> W.[Category] OR
					O.[PrimaryContact] <> W.[PrimaryContact] OR
					ISNULL(O.[SupplierReference], '') <> ISNULL(W.[SupplierReference], '') OR
					O.[PaymentDays] <> W.[PaymentDays] OR
					O.[PostalCode] <> W.[PostalCode]
			) M 
		) M
		WHERE
			M.[Count] > 0

	)

	INSERT INTO [dbo].[DataValidationErrors]
	(
		[CutoffDate],
		[Error],
		[Details],
		[Schema],
		[Table],
		[Process]
	)
	SELECT
		@CutoffDate,
		Error,
		Details,
		@Schema,
		@Table,
		@Process
	FROM
		MismatchCount
	
	UNION

	SELECT
		@CutoffDate,
		Error,
		Details,
		@Schema,
		@Table,
		@Process
	FROM
		MismatchValues

	COMMIT TRAN

END;
GO
/****** Object:  StoredProcedure [dbo].[ValidateDimTransactionTypeData]    Script Date: 10/13/2025 10:24:08 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO



/***

EXEC [dbo].[ValidateDimPaymentMethodData] 
	@Process='WarehouseWideWorldImporters', 
	@CutoffDate='1/1/2013',
	@Schema='dbo',
	@Table='DimTransactionType'

SELECT * FROM [dbo].[DataValidationErrors]

***/
CREATE PROCEDURE [dbo].[ValidateDimTransactionTypeData]
(
	@Process NVARCHAR(50),
	@CutoffDate DATETIME,
	@Schema NVARCHAR(50),
	@Table NVARCHAR(50),
	@PurgeExisting BIT = 1
)
WITH EXECUTE AS OWNER
AS
BEGIN

    SET NOCOUNT ON;
    SET XACT_ABORT ON;

	DECLARE @MismatchCountError NVARCHAR(50) = 'Mismatch in record count'
	DECLARE @MismatchValueError NVARCHAR(50) = 'Mismatch in data values'

	BEGIN TRAN

	IF @PurgeExisting = 1 
	BEGIN

		DELETE
			[dbo].[DataValidationErrors]
		WHERE
			[CutoffDate] = @CutoffDate AND
			[Error] IN (@MismatchCountError, @MismatchValueError) AND
			[Schema] = @Schema AND
			[Table] = @Table AND
			[Process] = @Process

	END

	-- Main data validation
    ;WITH DimTransactionTypeOriginal AS
	(

		SELECT
			[WWITransactionTypeID] = TT.TransactionTypeID,
			[TransactionType] = TT.TransactionTypeName
		FROM
			(
				SELECT
					TT.[TransactionTypeID],
					TT.TransactionTypeName
				FROM
					dbo.Application_TransactionTypes TT
				WHERE
					@CutoffDate BETWEEN TT.ValidFrom AND TT.ValidTo

				UNION 

				SELECT
					TTA.[TransactionTypeID],
					TTA.TransactionTypeName
				FROM
					dbo.Application_TransactionTypes_Archive TTA
				WHERE
					@CutoffDate BETWEEN TTA.ValidFrom AND TTA.ValidTo
			) TT

	),

	DimTransactionTypeWarehouse AS 
	(

		SELECT
			*
		FROM
			dbo.DimTransactionType
		WHERE
			LoadDate <= @CutoffDate AND
			TransactionTypeKey > 0

	),

	MismatchCount AS
	(

		SELECT
			[Error] = @MismatchCountError,
			[Details] = 'Non-warehouse tables has ' + CAST(O.[Count] AS NVARCHAR(50)) + ' records compared to warehouse table with ' + CAST(W.[Count] AS NVARCHAR(50)) + ' records.'
		FROM
			(
				SELECT
					[Count] = COUNT(*)
				FROM		
					DimTransactionTypeOriginal
			) O LEFT JOIN
			(
				SELECT
					[Count] = COUNT(*)
				FROM
					DimTransactionTypeWarehouse
			) W ON 1 = 1
		WHERE
			O.[Count] <> W.[Count]

	),

	MismatchValues AS
	(

		SELECT
			[Error] = @MismatchValueError,
			[Details] = 'There are ' + CAST(M.[Count] AS NVARCHAR(50)) + ' records with mismatched values.'
		FROM
		(	
			SELECT TOP 1
				*
			FROM
			(
				SELECT
					[Count] = COUNT(*)
				FROM
					DimTransactionTypeOriginal O LEFT JOIN
					DimTransactionTypeWarehouse W ON
						W.WWITransactionTypeID = O.WWITransactionTypeID
				WHERE
					O.[TransactionType] <> W.[TransactionType]
			) M 
		) M
		WHERE
			M.[Count] > 0

	)

	INSERT INTO [dbo].[DataValidationErrors]
	(
		[CutoffDate],
		[Error],
		[Details],
		[Schema],
		[Table],
		[Process]
	)
	SELECT
		@CutoffDate,
		Error,
		Details,
		@Schema,
		@Table,
		@Process
	FROM
		MismatchCount
	
	UNION

	SELECT
		@CutoffDate,
		Error,
		Details,
		@Schema,
		@Table,
		@Process
	FROM
		MismatchValues

	COMMIT TRAN

END;
GO
/****** Object:  StoredProcedure [dbo].[ValidateFctMovementData]    Script Date: 10/13/2025 10:24:08 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO




/***

EXEC [dbo].[ValidateDimPaymentMethodData] 
	@Process='WarehouseWideWorldImporters', 
	@CutoffDate='1/1/2013',
	@Schema='dbo',
	@Table='FctMovement'

SELECT * FROM [dbo].[DataValidationErrors]

***/
CREATE PROCEDURE [dbo].[ValidateFctMovementData]
(
	@Process NVARCHAR(50),
	@CutoffDate DATETIME,
	@Schema NVARCHAR(50),
	@Table NVARCHAR(50),
	@PurgeExisting BIT = 1
)
WITH EXECUTE AS OWNER
AS
BEGIN

    SET NOCOUNT ON;
    SET XACT_ABORT ON;

	DECLARE @MismatchCountError NVARCHAR(50) = 'Mismatch in record count'
	DECLARE @MismatchValueError NVARCHAR(50) = 'Mismatch in data values'

	BEGIN TRAN

	IF @PurgeExisting = 1 
	BEGIN

		DELETE
			[dbo].[DataValidationErrors]
		WHERE
			[CutoffDate] = @CutoffDate AND
			[Error] IN (@MismatchCountError, @MismatchValueError) AND
			[Schema] = @Schema AND
			[Table] = @Table AND
			[Process] = @Process

	END

	-- Main data validation
    ;WITH FctMovementOriginal AS
	(

		SELECT
			[DateKey] = CAST(SIT.TransactionOccurredWhen AS DATE),
			[WWIStockItemID] = SIT.StockItemID,
			[WWICustomerID] = SIT.CustomerID,
			[WWISupplierID] = SIT.SupplierID,
			[WWITransactionTypeID] = SIT.TransactionTypeID,
			[WWIStockItemTransactionID] = SIT.StockItemTransactionID,
			[WWIInvoiceID] = SIT.InvoiceID,
			[WWIPurchaseOrderID] = SIT.PurchaseOrderID,
			[Quantity] = SIT.Quantity
		FROM
			dbo.Warehouse_StockItemTransactions SIT LEFT JOIN
			(
				SELECT
					SI.StockItemID,
					SI.StockItemName
				FROM
					dbo.Warehouse_StockItems SI 
				WHERE
					@CutoffDate BETWEEN Si.ValidFrom AND SI.ValidTo

				UNION

				SELECT
					SIA.StockItemID,
					SIA.StockItemName
				FROM
					dbo.Warehouse_StockItems_Archive SIA
				WHERE
					@CutoffDate BETWEEN SIA.ValidFrom AND SIA.ValidTo
			) SI ON
				SI.StockItemID = SIT.StockItemID LEFT JOIN
			(
				SELECT
					C.CustomerID,
					C.CustomerName
				FROM
					dbo.Sales_Customers C 
				WHERE
					@CutoffDate BETWEEN C.ValidFrom AND C.ValidTo

				UNION

				SELECT
					CA.CustomerID,
					CA.CustomerName
				FROM
					dbo.Sales_Customers_Archive CA
				WHERE
					@CutoffDate BETWEEN CA.ValidFrom AND CA.ValidTo
			) C ON 
				C.CustomerID = SIT.CustomerID LEFT JOIN
			(
				SELECT
					S.SupplierID,
					S.SupplierName
				FROM
					dbo.Purchasing_Suppliers S 
				WHERE
					@CutoffDate BETWEEN S.ValidFrom AND S.ValidTo

				UNION

				SELECT
					SA.SupplierID,
					SA.SupplierName
				FROM
					dbo.Purchasing_Suppliers_Archive SA
				WHERE
					@CutoffDate BETWEEN SA.ValidFrom AND SA.ValidTo
			) S ON 
				S.SupplierID = SIT.SupplierID LEFT JOIN
			(
				SELECT
					TT.TransactionTypeID,
					TT.TransactionTypeName
				FROM
					dbo.Application_TransactionTypes TT 
				WHERE
					@CutoffDate BETWEEN TT.ValidFrom AND TT.ValidTo

				UNION

				SELECT
					TTA.TransactionTypeID,
					TTA.TransactionTypeName
				FROM
					dbo.Application_TransactionTypes_Archive TTA
				WHERE
					@CutoffDate BETWEEN TTA.ValidFrom AND TTA.ValidTo
			) TT ON 
				TT.TransactionTypeID = SIT.TransactionTypeID 
		WHERE
			SIT.LastEditedWhen <= @CutoffDate

	),

	FctMovementWarehouse AS 
	(

		SELECT
			[MovementKey] = M.MovementKey,
			[DateKey] = M.DateKey,
			[WWIStockItemID] = SI.WWIStockItemID,
			[WWICustomerID] = C.WWICustomerID,
			[WWISupplierID] = S.WWISupplierID,
			[WWITransactionTypeID] = TT.WWITransactionTypeID,
			[WWIStockItemTransactionID] = M.WWIStockItemTransactionID,
			[WWIInvoiceID] = M.WWIInvoiceID,
			[WWIPurchaseOrderID] = M.WWIPurchaseOrderID,
			[Quantity] = M.Quantity
		FROM
			dbo.FctMovement M LEFT JOIN
			dbo.DimStockItem SI ON
				M.StockItemKey = SI.StockItemKey AND
				SI.LoadDate <= @CutoffDate LEFT JOIN
			dbo.DimCustomer C ON
				M.CustomerKey = C.CustomerKey AND 
				C.LoadDate <= @CutoffDate LEFT JOIN
			dbo.DimSupplier S ON
				M.SupplierKey = S.SupplierKey AND
				S.LoadDate <= @CutoffDate LEFT JOIN
			dbo.DimTransactionType TT ON
				M.TransactionTypeKey = TT.TransactionTypeKey AND
				TT.LoadDate <= @CutoffDate
		WHERE
			M.LoadDate <= @CutoffDate AND
			M.MovementKey > 0

	),

	MismatchCount AS
	(

		SELECT
			[Error] = @MismatchCountError,
			[Details] = 'Non-warehouse tables has ' + CAST(O.[Count] AS NVARCHAR(50)) + ' records compared to warehouse table with ' + CAST(W.[Count] AS NVARCHAR(50)) + ' records.'
		FROM
			(
				SELECT
					[Count] = COUNT(*)
				FROM		
					FctMovementOriginal
			) O LEFT JOIN
			(
				SELECT
					[Count] = COUNT(*)
				FROM
					FctMovementWarehouse
			) W ON 1 = 1
		WHERE
			O.[Count] <> W.[Count]

	),

	MismatchValues AS
	(

		SELECT
			[Error] = @MismatchValueError,
			[Details] = 'There are ' + CAST(M.[Count] AS NVARCHAR(50)) + ' records with mismatched values.'
		FROM
		(	
			SELECT TOP 1
				*
			FROM
			(
				SELECT
					[Count] = COUNT(*)
				FROM
					FctMovementOriginal O LEFT JOIN
					FctMovementWarehouse W ON
						W.WWIStockItemTransactionID = O.WWIStockItemTransactionID
				WHERE
					O.[DateKey] <> W.[DateKey] OR
					O.[WWIStockItemID] <> W.[WWIStockItemID] OR
					O.[WWICustomerID] <> W.[WWICustomerID] OR
					O.[WWISupplierID] <> W.[WWISupplierID] OR
					O.[WWITransactionTypeID] <> W.[WWITransactionTypeID] OR
					O.[WWIStockItemTransactionID] <> W.[WWIStockItemTransactionID] OR
					O.[WWIInvoiceID] <> W.[WWIInvoiceID] OR
					O.[WWIPurchaseOrderID] <> W.[WWIPurchaseOrderID] OR
					O.[Quantity] <> W.[Quantity]
			) M 
		) M
		WHERE
			M.[Count] > 0

	)

	INSERT INTO [dbo].[DataValidationErrors]
	(
		[CutoffDate],
		[Error],
		[Details],
		[Schema],
		[Table],
		[Process]
	)
	SELECT
		@CutoffDate,
		Error,
		Details,
		@Schema,
		@Table,
		@Process
	FROM
		MismatchCount
	
	UNION

	SELECT
		@CutoffDate,
		Error,
		Details,
		@Schema,
		@Table,
		@Process
	FROM
		MismatchValues

	COMMIT TRAN

END;
GO
/****** Object:  StoredProcedure [dbo].[ValidateFctOrderData]    Script Date: 10/13/2025 10:24:08 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO




/***

EXEC [dbo].[ValidateFctOrderData] 
	@Process='WarehouseWideWorldImporters', 
	@CutoffDate='1/1/2013',
	@Schema='dbo',
	@Table='FctOrder'

SELECT * FROM [dbo].[DataValidationErrors]

***/
CREATE PROCEDURE [dbo].[ValidateFctOrderData]
(
	@Process NVARCHAR(50),
	@CutoffDate DATETIME,
	@Schema NVARCHAR(50),
	@Table NVARCHAR(50),
	@PurgeExisting BIT = 1
)
WITH EXECUTE AS OWNER
AS
BEGIN

    SET NOCOUNT ON;
    SET XACT_ABORT ON;

	DECLARE @MismatchCountError NVARCHAR(50) = 'Mismatch in record count'
	DECLARE @MismatchValueError NVARCHAR(50) = 'Mismatch in data values'

	BEGIN TRAN

	IF @PurgeExisting = 1 
	BEGIN

		DELETE
			[dbo].[DataValidationErrors]
		WHERE
			[CutoffDate] = @CutoffDate AND
			[Error] IN (@MismatchCountError, @MismatchValueError) AND
			[Schema] = @Schema AND
			[Table] = @Table AND
			[Process] = @Process

	END

	-- Main data validation
    ;WITH FctOrderOriginal AS
	(

		SELECT
			[WWICityID] = C.DeliveryCityID,
			[WWICustomerID] = O.CustomerID,
			[WWIStockItemID] = OL.StockItemID,
			[OrderDateKey] = CAST(O.OrderDate AS DATE),
			[PickedDateKey] = CAST(OL.PickingCompletedWhen AS DATE),
			[WWISalespersonID] = O.SalespersonPersonID,
			[WWIPickerID] = O.PickedByPersonID,
			[WWIOrderID] = O.OrderID,
			[WWIOrderLineID] = OL.OrderLineID,
			[WWIBackorderID] = O.BackorderOrderID,
			[Description] = OL.[Description],
			[Package] = PT.PackageTypeName,
			[Quantity] = OL.Quantity,
			[UnitPrice] = OL.UnitPrice,
			[TaxRate] = OL.TaxRate,
			[TotalExcludingTax] = ROUND(OL.Quantity * OL.UnitPrice, 2),
			[TaxAmount] = ROUND((OL.Quantity * OL.UnitPrice * OL.TaxRate) / 100.0, 2),
			[TotaIncludingTax] =(
				ROUND(OL.Quantity * OL.UnitPrice, 2) + 
				ROUND((OL.Quantity * OL.UnitPrice * OL.TaxRate) / 100.0, 2)
			)
		FROM
			dbo.Sales_Orders O LEFT JOIN
			dbo.Sales_OrderLines OL ON
				OL.OrderID = O.OrderID LEFT JOIN 
			(
				SELECT
					C.CustomerID,
					C.DeliveryCityID,
					C.CustomerName
				FROM
					dbo.Sales_Customers C
				WHERE
					@CutoffDate BETWEEN C.ValidFrom AND C.ValidTo

				UNION

				SELECT
					CA.CustomerID,
					CA.DeliveryCityID,
					CA.CustomerName
				FROM
					dbo.Sales_Customers_Archive CA
				WHERE
					@CutoffDate BETWEEN CA.ValidFrom AND CA.ValidTo
			) C ON
				C.CustomerID = O.CustomerID LEFT JOIN
			(
				SELECT
					PT.PackageTypeID,
					PT.PackageTypeName
				FROM
					dbo.Warehouse_PackageTypes PT 
				WHERE
					@CutoffDate BETWEEN PT.ValidFrom AND PT.ValidTo

				UNION

				SELECT
					PTA.PackageTypeID,
					PTA.PackageTypeName
				FROM
					dbo.Warehouse_PackageTypes_Archive PTA 
				WHERE
					@CutoffDate BETWEEN PTA.ValidFrom AND PTA.ValidTo
			) PT ON
				PT.PackageTypeID = OL.PackageTypeID
		WHERE
			O.LastEditedWhen <= @CutoffDate AND
			OL.LastEditedWhen <= @CutoffDate

	),

	FctOrderWarehouse AS 
	(

		SELECT
			[OrderKey] = O.OrderKey,
			[WWICityID] = C.WWICityID,
			[WWICustomerID] = CU.WWICustomerID,
			[WWIStockItemID] = SI.WWIStockItemID,
			[OrderDateKey] = O.OrderDateKey,
			[PickedDateKey] = O.PickedDateKey,
			[WWISalespersonID] = E.WWIEmployeeID,
			[WWIPickerID] = E2.WWIEmployeeID,
			[WWIOrderID] = O.WWIOrderID,
			[WWIOrderLineID] = O.WWIOrderLineID,
			[WWIBackorderID] = O.WWIBackorderID,
			[Description] = O.[Description],
			[Package] = O.Package,
			[Quantity] = O.Quantity,
			[UnitPrice] = O.UnitPrice,
			[TaxRate] = O.TaxRate,
			[TotalExcludingTax] = O.TotalExcludingTax,
			[TaxAmount] = O.TaxAmount,
			[TotaIncludingTax] = O.TotalIncludingTax
		FROM
			dbo.FctOrder O LEFT JOIN
			dbo.DimCity C ON
				O.CityKey = C.CityKey AND
				C.LoadDate <= @CutoffDate LEFT JOIN
			dbo.DimCustomer CU ON
				O.CustomerKey = CU.CustomerKey AND
				CU.LoadDate <= @CutoffDate LEFT JOIN
			dbo.DimStockItem SI ON
				O.StockItemKey = SI.StockItemKey AND
				SI.LoadDate <= @CutoffDate LEFT JOIN
			dbo.DimEmployee E ON
				O.SalespersonKey = E.EmployeeKey AND
				E.LoadDate <= @CutoffDate LEFT JOIN
			dbo.DimEmployee E2 ON
				O.PickerKey = E2.EmployeeKey AND
				E2.LoadDate <= @CutoffDate
		WHERE
			O.LoadDate <= @CutoffDate AND
			O.OrderKey > 0

	),

	MismatchCount AS
	(

		SELECT
			[Error] = @MismatchCountError,
			[Details] = 'Non-warehouse tables has ' + CAST(O.[Count] AS NVARCHAR(50)) + ' records compared to warehouse table with ' + CAST(W.[Count] AS NVARCHAR(50)) + ' records.'
		FROM
			(
				SELECT
					[Count] = COUNT(*)
				FROM		
					FctOrderOriginal
			) O LEFT JOIN
			(
				SELECT
					[Count] = COUNT(*)
				FROM
					FctOrderWarehouse
			) W ON 1 = 1
		WHERE
			O.[Count] <> W.[Count]

	),

	MismatchValues AS
	(

		SELECT
			[Error] = @MismatchValueError,
			[Details] = 'There are ' + CAST(M.[Count] AS NVARCHAR(50)) + ' records with mismatched values.'
		FROM
		(	
			SELECT TOP 1
				*
			FROM
			(
				SELECT
					[Count] = COUNT(*)
				FROM
					FctOrderOriginal O LEFT JOIN
					FctOrderWarehouse W ON
						W.WWIOrderID = O.WWIOrderID AND
						W.WWIOrderLineID = O.WWIOrderLineID
				WHERE
					O.[WWICityID] <> W.[WWICityID] OR
					O.[WWICustomerID] <> W.[WWICustomerID] OR
					O.[WWIStockItemID] <> W.[WWIStockItemID] OR
					O.[OrderDateKey] <> W.[OrderDateKey] OR
					ISNULL(O.[PickedDateKey], CAST('1/1/1900' AS DATE)) <> ISNULL(W.[PickedDateKey], CAST('1/1/1900' AS DATE)) OR
					O.[WWISalespersonID] <> W.[WWISalespersonID] OR
					ISNULL(O.[WWIPickerID], 0) <> ISNULL(W.[WWIPickerID], 0) OR
					ISNULL(O.[WWIBackorderID], 0) <> ISNULL(W.[WWIBackorderID], 0) OR
					O.[Description] <> W.[Description] OR
					O.[Package] <> W.[Package] OR
					O.[Quantity] <> W.[Quantity] OR
					O.[UnitPrice] <> W.[UnitPrice] OR
					O.[TaxRate] <> W.[TaxRate] OR
					O.[TotalExcludingTax] <> W.[TotalExcludingTax] OR
					O.[TaxAmount] <> W.[TaxAmount] OR
					O.[TotaIncludingTax] <> W.[TotaIncludingTax]
			) M 
		) M
		WHERE
			M.[Count] > 0

	)

	INSERT INTO [dbo].[DataValidationErrors]
	(
		[CutoffDate],
		[Error],
		[Details],
		[Schema],
		[Table],
		[Process]
	)
	SELECT
		@CutoffDate,
		Error,
		Details,
		@Schema,
		@Table,
		@Process
	FROM
		MismatchCount
	
	UNION

	SELECT
		@CutoffDate,
		Error,
		Details,
		@Schema,
		@Table,
		@Process
	FROM
		MismatchValues

	COMMIT TRAN

END;
GO
/****** Object:  StoredProcedure [dbo].[ValidateFctPurchaseData]    Script Date: 10/13/2025 10:24:08 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO




/***

EXEC [dbo].[ValidateFctPurchaseData] 
	@Process='WarehouseWideWorldImporters', 
	@CutoffDate='1/1/2013',
	@Schema='dbo',
	@Table='FctPurchase'

SELECT * FROM [dbo].[DataValidationErrors]

***/
CREATE PROCEDURE [dbo].[ValidateFctPurchaseData]
(
	@Process NVARCHAR(50),
	@CutoffDate DATETIME,
	@Schema NVARCHAR(50),
	@Table NVARCHAR(50),
	@PurgeExisting BIT = 1
)
WITH EXECUTE AS OWNER
AS
BEGIN

    SET NOCOUNT ON;
    SET XACT_ABORT ON;

	DECLARE @MismatchCountError NVARCHAR(50) = 'Mismatch in record count'
	DECLARE @MismatchValueError NVARCHAR(50) = 'Mismatch in data values'

	BEGIN TRAN

	IF @PurgeExisting = 1 
	BEGIN

		DELETE
			[dbo].[DataValidationErrors]
		WHERE
			[CutoffDate] = @CutoffDate AND
			[Error] IN (@MismatchCountError, @MismatchValueError) AND
			[Schema] = @Schema AND
			[Table] = @Table AND
			[Process] = @Process

	END

	-- Main data validation
    ;WITH FctPurchaseOriginal AS
	(

		SELECT
			[DateKey] = CAST(PO.OrderDate AS DATE),
			[WWISupplierID] = PO.SupplierID,
			[WWIStockItemID] = POL.StockItemID,
			[WWIPurchaseOrderID] = PO.PurchaseOrderID,
			[WWIPurchaseOrderLineID] = POL.PurchaseOrderLineID,
			[OrderedOuters] = POL.OrderedOuters,
			[OrderedQuantity] = POL.OrderedOuters * SI.QuantityPerOuter,
			[ReceivedOuters] = POL.ReceivedOuters,
			[Package] = PT.PackageTypeName,
			[IsOrderFinalized] = PO.IsOrderFinalized
		FROM
			dbo.Purchasing_PurchaseOrders PO LEFT JOIN
			dbo.Purchasing_PurchaseOrderLines POL ON
				POL.PurchaseOrderID = PO.PurchaseOrderID LEFT JOIN
			(
				SELECT
					SI.StockItemID,
					SI.StockItemName,
					SI.QuantityPerOuter
				FROM
					dbo.Warehouse_StockItems SI 
				WHERE
					@CutoffDate BETWEEN SI.ValidFrom AND SI.ValidTo

				UNION

				SELECT
					SIA.StockItemID,
					SIA.StockItemName,
					SIA.QuantityPerOuter
				FROM
					dbo.Warehouse_StockItems_Archive SIA
				WHERE
					@CutoffDate BETWEEN SIA.ValidFrom AND SIA.ValidTo
			) SI ON
				SI.StockItemID = POL.StockItemID LEFT JOIN
			(
				SELECT
					PT.PackageTypeID,
					PT.PackageTypeName
				FROM
					dbo.Warehouse_PackageTypes PT
				WHERE
					@CutoffDate BETWEEN PT.ValidFrom AND PT.ValidTo

				UNION

				SELECT
					PTA.PackageTypeID,
					PTA.PackageTypeName
				FROM
					dbo.Warehouse_PackageTypes_Archive PTA
				WHERE
					@CutoffDate BETWEEN PTA.ValidFrom AND PTA.ValidTo
			) PT ON
				PT.PackageTypeID = POL.PackageTypeID
		WHERE
			PO.LastEditedWhen <= @CutoffDate AND
			POL.LastEditedWhen <= @CutoffDate

	),

	FctPurchaseWarehouse AS 
	(

		SELECT
			[PurchaseKey] = P.PurchaseKey,
			[DateKey] = P.DateKey,
			[WWISupplierID] = S.WWISupplierID,
			[WWIStockItemID] = SI.WWIStockItemID,
			[WWIPurchaseOrderID] = P.WWIPurchaseOrderID,
			[WWIPurchaseOrderLineID] = P.WWIPurchaseOrderLineID,
			[OrderedOuters] = P.OrderedOuters,
			[OrderedQuantity] = P.OrderedQuantity,
			[ReceivedOuters] = P.ReceivedOuters,
			[Package] = P.Package,
			[IsOrderFinalized] = P.IsOrderFinalized
		FROM
			dbo.FctPurchase P LEFT JOIN
			dbo.DimSupplier S ON
				P.SupplierKey = S.SupplierKey AND 
				S.LoadDate <= @CutoffDate LEFT JOIN
			dbo.DimStockItem SI ON
				P.StockItemKey = SI.StockItemKey AND
				SI.LoadDate <= @CutoffDate
		WHERE
			P.LoadDate <= @CutoffDate AND
			P.PurchaseKey > 0

	),

	MismatchCount AS
	(

		SELECT
			[Error] = @MismatchCountError,
			[Details] = 'Non-warehouse tables has ' + CAST(O.[Count] AS NVARCHAR(50)) + ' records compared to warehouse table with ' + CAST(W.[Count] AS NVARCHAR(50)) + ' records.'
		FROM
			(
				SELECT
					[Count] = COUNT(*)
				FROM		
					FctPurchaseOriginal
			) O LEFT JOIN
			(
				SELECT
					[Count] = COUNT(*)
				FROM
					FctPurchaseWarehouse
			) W ON 1 = 1
		WHERE
			O.[Count] <> W.[Count]

	),

	MismatchValues AS
	(

		SELECT
			[Error] = @MismatchValueError,
			[Details] = 'There are ' + CAST(M.[Count] AS NVARCHAR(50)) + ' records with mismatched values.'
		FROM
		(	
			SELECT TOP 1
				*
			FROM
			(
				SELECT
					[Count] = COUNT(*)
				FROM
					FctPurchaseOriginal O LEFT JOIN
					FctPurchaseWarehouse W ON
						W.WWIPurchaseOrderID = O.WWIPurchaseOrderID AND
						W.WWIPurchaseOrderLineID = O.WWIPurchaseOrderLineID
				WHERE
					ISNULL(O.[DateKey], CAST('1/1/1900' AS DATE)) <> ISNULL(W.[DateKey], CAST('1/1/1900' AS DATE)) OR
					ISNULL(O.[WWISupplierID], 0) <> ISNULL(W.[WWISupplierID], 0) OR
					ISNULL(O.[WWIStockItemID], 0) <> ISNULL(W.[WWIStockItemID], 0) OR
					O.[OrderedOuters] <> W.[OrderedOuters] OR
					ISNULL(O.[OrderedQuantity], 0) <> ISNULL(W.[OrderedQuantity], 0) OR
					O.[ReceivedOuters] <> W.[ReceivedOuters] OR
					ISNULL(O.[Package], '') <> ISNULL(W.[Package], '') OR
					O.[IsOrderFinalized] <> W.[IsOrderFinalized]
			) M 
		) M
		WHERE
			M.[Count] > 0

	)

	INSERT INTO [dbo].[DataValidationErrors]
	(
		[CutoffDate],
		[Error],
		[Details],
		[Schema],
		[Table],
		[Process]
	)
	SELECT
		@CutoffDate,
		Error,
		Details,
		@Schema,
		@Table,
		@Process
	FROM
		MismatchCount
	
	UNION

	SELECT
		@CutoffDate,
		Error,
		Details,
		@Schema,
		@Table,
		@Process
	FROM
		MismatchValues

	COMMIT TRAN

END;
GO
/****** Object:  StoredProcedure [dbo].[ValidateFctSaleData]    Script Date: 10/13/2025 10:24:08 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO





/***

EXEC [dbo].[ValidateFctSaleData] 
	@Process='WarehouseWideWorldImporters', 
	@CutoffDate='1/1/2013',
	@Schema='dbo',
	@Table='FctSale'

SELECT * FROM [dbo].[DataValidationErrors]

***/
CREATE PROCEDURE [dbo].[ValidateFctSaleData]
(
	@Process NVARCHAR(50),
	@CutoffDate DATETIME,
	@Schema NVARCHAR(50),
	@Table NVARCHAR(50),
	@PurgeExisting BIT = 1
)
WITH EXECUTE AS OWNER
AS
BEGIN

    SET NOCOUNT ON;
    SET XACT_ABORT ON;

	DECLARE @MismatchCountError NVARCHAR(50) = 'Mismatch in record count'
	DECLARE @MismatchValueError NVARCHAR(50) = 'Mismatch in data values'

	BEGIN TRAN

	IF @PurgeExisting = 1 
	BEGIN

		DELETE
			[dbo].[DataValidationErrors]
		WHERE
			[CutoffDate] = @CutoffDate AND
			[Error] IN (@MismatchCountError, @MismatchValueError) AND
			[Schema] = @Schema AND
			[Table] = @Table AND
			[Process] = @Process

	END

	-- Main data validation
    ;WITH FctSaleOriginal AS
	(

		SELECT
			[WWICityID] = CU.DeliveryCityID,
			[WWICustomerID] = I.CustomerID,
			[WWIBillToCustomerID] = I.BillToCustomerID,
			[WWIStockItemID] = IL.StockItemID,
			[InvoiceDateKey] = CAST(I.InvoiceDate AS DATE),
			[DeliveryDateKey] = CAST(I.ConfirmedDeliveryTime AS DATE),
			[WWISalespersonID] = I.SalespersonPersonID,
			[WWIInvoiceID] = I.InvoiceID,
			[WWIInvoiceLineID] = IL.InvoiceLineID,
			[Description] = IL.[Description],
			[Package] = PT.PackageTypeName,
			[Quantity] = IL.Quantity,
			[UnitPrice] = IL.UnitPrice,
			[TaxRate] = IL.TaxRate,
			[TotalExcludingTax] = IL.ExtendedPrice - IL.TaxAmount,
			[TaxAmount] = IL.TaxAmount,
			[Profit] = IL.LineProfit,
			[TotalIncludingTax] = IL.ExtendedPrice,
			[TotalDryItems] = 
				CASE 
					WHEN SI.IsChillerStock = 0 THEN IL.Quantity 
					ELSE 0 
				END,
			[TotalChillerItems] = 
				CASE 
					WHEN SI.IsChillerStock <> 0 THEN IL.Quantity 
					ELSE 0 
				END 
		FROM
			dbo.Sales_Invoices I JOIN
			dbo.Sales_InvoiceLines IL ON
				IL.InvoiceID = I.InvoiceID LEFT JOIN
			(
				SELECT
					C.CustomerID,
					C.CustomerName,
					C.DeliveryCityID
				FROM
					dbo.Sales_Customers C
				WHERE
					@CutoffDate BETWEEN C.ValidFrom AND C.ValidTo

				UNION

				SELECT
					CA.CustomerID,
					CA.CustomerName,
					CA.DeliveryCityID
				FROM
					dbo.Sales_Customers_Archive CA
				WHERE
					@CutoffDate BETWEEN CA.ValidFrom AND CA.ValidTo
			) CU ON
				CU.CustomerID = I.CustomerID LEFT JOIN
			(
				SELECT
					SI.StockItemID,
					SI.StockItemName,
					SI.IsChillerStock
				FROM
					dbo.Warehouse_StockItems SI
				WHERE
					@CutoffDate BETWEEN SI.ValidFrom AND SI.ValidTo

				UNION

				SELECT
					SIA.StockItemID,
					SIA.StockItemName,
					SIA.IsChillerStock
				FROM
					dbo.Warehouse_StockItems_Archive SIA
				WHERE
					@CutoffDate BETWEEN SIA.ValidFrom AND SIA.ValidTo
			) SI ON
				SI.StockItemID = IL.StockItemID LEFT JOIN
			(
				SELECT
					PT.PackageTypeID,
					PT.PackageTypeName
				FROM
					dbo.Warehouse_PackageTypes PT
				WHERE
					@CutoffDate BETWEEN PT.ValidFrom AND PT.ValidTo

				UNION

				SELECT
					PTA.PackageTypeID,
					PTA.PackageTypeName
				FROM
					dbo.Warehouse_PackageTypes_Archive PTA
				WHERE
					@CutoffDate BETWEEN PTA.ValidFrom AND PTA.ValidTo
			) PT ON
				PT.PackageTypeID = IL.PackageTypeID
		WHERE	
			I.LastEditedWhen <= @CutoffDate AND
			IL.LastEditedWhen <= @CutoffDate

	),

	FctSaleWarehouse AS 
	(

		SELECT
			[SaleKey] = S.SaleKey,
			[WWICityID] = C.WWICityID,
			[WWICustomerID] = CU.WWICustomerID,
			[WWIBillToCustomerID] = BCU.WWICustomerID,
			[WWIStockItemID] = SI.WWIStockItemID,
			[InvoiceDateKey] = S.InvoiceDateKey,
			[DeliveryDateKey] = S.DeliveryDateKey,
			[WWISalespersonID] = E.WWIEmployeeID,
			[WWIInvoiceID] = S.WWIInvoiceID,
			[WWIInvoiceLineID] = S.WWIInvoiceLineID,
			[Description] = S.[Description],
			[Package] = S.[Package],
			[Quantity] = S.Quantity,
			[UnitPrice] = S.UnitPrice,
			[TaxRate] = S.TaxRate,
			[TotalExcludingTax] = S.TotalExcludingTax,
			[TaxAmount] = S.TaxAmount,
			[Profit] = S.Profit,
			[TotalIncludingTax] = S.TotalIncludingTax,
			[TotalDryItems] = S.TotalDryItems,
			[TotalChillerItems] = S.TotalChillerItems
		FROM
			dbo.FctSale S LEFT JOIN
			dbo.DimCity C ON
				S.CityKey = C.CityKey AND
				C.LoadDate <= @CutoffDate LEFT JOIN
			dbo.DimCustomer CU ON
				S.CustomerKey = CU.CustomerKey AND
			    CU.LoadDate <= @CutoffDate LEFT JOIN
			dbo.DimCustomer BCU ON
				S.BillToCustomerKey = BCU.CustomerKey AND
				BCU.LoadDate <= @CutoffDate LEFT JOIN
			dbo.DimStockItem SI ON
				S.StockItemKey = SI.StockItemKey AND
				SI.LoadDate <= @CutoffDate LEFT JOIN
			dbo.DimEmployee E ON
				S.SalespersonKey = E.EmployeeKey AND
				E.LoadDate <= @CutoffDate
		WHERE
			S.LoadDate <= @CutoffDate AND
			S.SaleKey > 0

	),

	MismatchCount AS
	(

		SELECT
			[Error] = @MismatchCountError,
			[Details] = 'Non-warehouse tables has ' + CAST(O.[Count] AS NVARCHAR(50)) + ' records compared to warehouse table with ' + CAST(W.[Count] AS NVARCHAR(50)) + ' records.'
		FROM
			(
				SELECT
					[Count] = COUNT(*)
				FROM		
					FctSaleOriginal
			) O LEFT JOIN
			(
				SELECT
					[Count] = COUNT(*)
				FROM
					FctSaleWarehouse
			) W ON 1 = 1
		WHERE
			O.[Count] <> W.[Count]

	),

	MismatchValues AS
	(

		SELECT
			[Error] = @MismatchValueError,
			[Details] = 'There are ' + CAST(M.[Count] AS NVARCHAR(50)) + ' records with mismatched values.'
		FROM
		(	
			SELECT TOP 1
				*
			FROM
			(
				SELECT
					[Count] = COUNT(*)
				FROM
					FctSaleOriginal O LEFT JOIN
					FctSaleWarehouse W ON
						W.WWIInvoiceID = O.WWIInvoiceID AND
						W.WWIInvoiceLineID = O.WWIInvoiceLineID
				WHERE
					O.[WWICityID] <> W.[WWICityID] OR
					O.[WWICustomerID] <> W.[WWICustomerID] OR
					O.[WWIBillToCustomerID] <> W.[WWIBillToCustomerID] OR
					O.[WWIStockItemID] <> W.[WWIStockItemID] OR
					O.[InvoiceDateKey] <> W.[InvoiceDateKey] OR
					ISNULL(O.[DeliveryDateKey], CAST('1/1/1900' AS DATE)) <> ISNULL(W.[DeliveryDateKey], CAST('1/1/1900' AS DATE)) OR
					O.[WWISalespersonID] <> W.[WWISalespersonID] OR
					O.[Description] <> W.[Description] OR
					O.[Package] <> W.[Package] OR
					O.[Quantity] <> W.[Quantity] OR
					O.[UnitPrice] <> W.[UnitPrice] OR
					O.[TaxRate] <> W.[TaxRate] OR
					O.[TotalExcludingTax] <> W.[TotalExcludingTax] OR
					O.[TaxAmount] <> W.[TaxAmount] OR
					O.[Profit] <> W.[Profit] OR
					O.[TotalIncludingTax] <> W.[TotalIncludingTax] OR
					O.[TotalDryItems] <> W.[TotalDryItems] OR
					O.[TotalChillerItems] <> W.[TotalChillerItems]
			) M 
		) M
		WHERE
			M.[Count] > 0

	)

	INSERT INTO [dbo].[DataValidationErrors]
	(
		[CutoffDate],
		[Error],
		[Details],
		[Schema],
		[Table],
		[Process]
	)
	SELECT
		@CutoffDate,
		Error,
		Details,
		@Schema,
		@Table,
		@Process
	FROM
		MismatchCount
	
	UNION

	SELECT
		@CutoffDate,
		Error,
		Details,
		@Schema,
		@Table,
		@Process
	FROM
		MismatchValues

	COMMIT TRAN

END;
GO
/****** Object:  StoredProcedure [dbo].[ValidateFctStockHoldingData]    Script Date: 10/13/2025 10:24:08 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO



/***

EXEC [dbo].[ValidateFctStockHoldingData] 
	@Process='WarehouseWideWorldImporters', 
	@CutoffDate='1/1/2013',
	@Schema='dbo',
	@Table='FctStockHolding'

SELECT * FROM [dbo].[DataValidationErrors]

***/
CREATE PROCEDURE [dbo].[ValidateFctStockHoldingData]
(
	@Process NVARCHAR(50),
	@CutoffDate DATETIME,
	@Schema NVARCHAR(50),
	@Table NVARCHAR(50),
	@PurgeExisting BIT = 1
)
WITH EXECUTE AS OWNER
AS
BEGIN

    SET NOCOUNT ON;
    SET XACT_ABORT ON;

	DECLARE @MismatchCountError NVARCHAR(50) = 'Mismatch in record count'
	DECLARE @MismatchValueError NVARCHAR(50) = 'Mismatch in data values'

	BEGIN TRAN

	IF @PurgeExisting = 1 
	BEGIN

		DELETE
			[dbo].[DataValidationErrors]
		WHERE
			[CutoffDate] = @CutoffDate AND
			[Error] IN (@MismatchCountError, @MismatchValueError) AND
			[Schema] = @Schema AND
			[Table] = @Table AND
			[Process] = @Process

	END

	-- Main data validation
    ;WITH FctStockHoldingOriginal AS
	(

		SELECT
			[WWIStockItemID] = SIH.StockItemID,
			[QuantityOnHand] = SIH.QuantityOnHand,
			[BinLocation] = SIH.BinLocation,
			[LastStocktakeQuantity] = SIH.LastStocktakeQuantity,
			[LastCostPrice] = SIH.LastCostPrice,
			[ReorderLevel] = SIH.ReorderLevel,
			[TargetStockLevel] = SIH.TargetStockLevel
		FROM
			dbo.Warehouse_StockItemHoldings SIH 
		WHERE
			SIH.LastEditedWhen <= @CutoffDate

	),

	FctStockHoldingWarehouse AS 
	(

		SELECT
			[StockHoldingKey] = SH.StockHoldingKey,
			[WWIStockItemID] = SI.WWIStockItemID,
			[QuantityOnHand] = SH.QuantityOnHand,
			[BinLocation] = SH.BinLocation,
			[LastStocktakeQuantity] = SH.LastStocktakeQuantity,
			[LastCostPrice] = SH.LastCostPrice,
			[ReorderLevel] = SH.ReorderLevel,
			[TargetStockLevel] = SH.TargetStockLevel
		FROM
			dbo.FctStockHolding SH LEFT JOIN
			dbo.DimStockItem SI ON
				SH.StockItemKey = SI.StockItemKey AND
				SI.LoadDate <= @CutoffDate
		WHERE
			SH.LoadDate <= @CutoffDate AND
			SH.StockHoldingKey > 0

	),

	MismatchCount AS
	(

		SELECT
			[Error] = @MismatchCountError,
			[Details] = 'Non-warehouse tables has ' + CAST(O.[Count] AS NVARCHAR(50)) + ' records compared to warehouse table with ' + CAST(W.[Count] AS NVARCHAR(50)) + ' records.'
		FROM
			(
				SELECT
					[Count] = COUNT(*)
				FROM		
					FctStockHoldingOriginal
			) O LEFT JOIN
			(
				SELECT
					[Count] = COUNT(*)
				FROM
					FctStockHoldingWarehouse
			) W ON 1 = 1
		WHERE
			O.[Count] <> W.[Count]

	),

	MismatchValues AS
	(

		SELECT
			[Error] = @MismatchValueError,
			[Details] = 'There are ' + CAST(M.[Count] AS NVARCHAR(50)) + ' records with mismatched values.'
		FROM
		(	
			SELECT TOP 1
				*
			FROM
			(
				SELECT
					[Count] = COUNT(*)
				FROM
					FctStockHoldingOriginal O LEFT JOIN
					FctStockHoldingWarehouse W ON
						W.WWIStockItemID = O.WWIStockItemID
				WHERE
					O.[QuantityOnHand] <> W.[QuantityOnHand] OR
					O.[BinLocation] <> W.[BinLocation] OR
					O.[LastStocktakeQuantity] <> W.[LastStocktakeQuantity] OR
					O.[LastCostPrice] <> W.[LastCostPrice] OR
					O.[ReorderLevel] <> W.[ReorderLevel] OR
					O.[TargetStockLevel] <> W.[TargetStockLevel]
			) M 
		) M
		WHERE
			M.[Count] > 0

	)

	INSERT INTO [dbo].[DataValidationErrors]
	(
		[CutoffDate],
		[Error],
		[Details],
		[Schema],
		[Table],
		[Process]
	)
	SELECT
		@CutoffDate,
		Error,
		Details,
		@Schema,
		@Table,
		@Process
	FROM
		MismatchCount
	
	UNION

	SELECT
		@CutoffDate,
		Error,
		Details,
		@Schema,
		@Table,
		@Process
	FROM
		MismatchValues

	COMMIT TRAN

END;
GO
/****** Object:  StoredProcedure [dbo].[ValidateFctTransactionData]    Script Date: 10/13/2025 10:24:08 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO




/***

EXEC [dbo].[ValidateFctTransactionData] 
	@Process='WarehouseWideWorldImporters', 
	@CutoffDate='1/1/2013',
	@Schema='dbo',
	@Table='FctTransaction'

SELECT * FROM [dbo].[DataValidationErrors]

***/
CREATE PROCEDURE [dbo].[ValidateFctTransactionData]
(
	@Process NVARCHAR(50),
	@CutoffDate DATETIME,
	@Schema NVARCHAR(50),
	@Table NVARCHAR(50),
	@PurgeExisting BIT = 1
)
WITH EXECUTE AS OWNER
AS
BEGIN

    SET NOCOUNT ON;
    SET XACT_ABORT ON;

	DECLARE @MismatchCountError NVARCHAR(50) = 'Mismatch in record count'
	DECLARE @MismatchValueError NVARCHAR(50) = 'Mismatch in data values'

	BEGIN TRAN

	IF @PurgeExisting = 1 
	BEGIN

		DELETE
			[dbo].[DataValidationErrors]
		WHERE
			[CutoffDate] = @CutoffDate AND
			[Error] IN (@MismatchCountError, @MismatchValueError) AND
			[Schema] = @Schema AND
			[Table] = @Table AND
			[Process] = @Process

	END

	-- Main data validation
    ;WITH FctTransactionOriginal AS
	(

		SELECT
			[DateKey] = CAST(CT.TransactionDate AS DATE),
			[WWICustomerID] = COALESCE(I.CustomerID, CT.CustomerID),
			[WWIBillToCustomerID] = CT.CustomerID,
			[WWISupplierID] = CAST(NULL AS INT),
			[WWITransactionTypeID] = CT.TransactionTypeID,
			[WWIPaymentMethodID] = CT.PaymentMethodID,
			[WWICustomerTransactionID] = CT.CustomerTransactionID,
			[WWISupplierTransactionID] = CAST(NULL AS INT),
			[WWIInvoiceID] = CT.InvoiceID,
			[WWIPurchaseOrderID] = CAST(NULL AS INT),
			[SupplierInvoiceNumber] = CAST(NULL AS NVARCHAR(20)),
			[TotalExcludingTax] = CT.AmountExcludingTax,
			[TaxAmount] = CT.TaxAmount,
			[TotalIncludingTax] = CT.TransactionAmount,
			[OutstandingBalance] = CT.OutstandingBalance,
			[IsFinalized] = CT.IsFinalized
		FROM
			dbo.Sales_CustomerTransactions CT LEFT JOIN
			dbo.Sales_Invoices I ON
				I.InvoiceID = CT.InvoiceID 
		WHERE
			CT.LastEditedWhen <= @CutoffDate

		UNION ALL

		SELECT
			[DateKey] = CAST(ST.TransactionDate AS DATE),
			[WWICustomerID] = CAST(NULL AS INT),
			[WWIBillToCustomerID] = CAST(NULL AS INT),
			[WWISupplierID] = ST.SupplierID,
			[WWITransactionTypeID] = ST.TransactionTypeID,
			[WWIPaymentMethodID] = ST.PaymentMethodID,
			[WWICustomerTransactionID] = CAST(NULL AS INT),
			[WWISupplierTransactionID] = ST.SupplierTransactionID,
			[WWIInvoiceID] = CAST(NULL AS INT),
			[WWIPurchaseOrderID] = ST.PurchaseOrderID,
			[SupplierInvoiceNumber] = ST.SupplierInvoiceNumber,
			[TotalExcludingTax] = ST.AmountExcludingTax,
			[TaxAmount] = ST.TaxAmount,
			[TotalIncludingTax] = ST.TransactionAmount,
			[OutstandingBalance] = ST.OutstandingBalance,
			[IsFinalized] = ST.IsFinalized
		FROM
			dbo.Purchasing_SupplierTransactions ST 
		WHERE
			ST.LastEditedWhen <= @CutoffDate
	),

	FctTransactionWarehouse AS 
	(

		SELECT
			[TransactionKey] = T.TransactionKey,
			[DateKey] = T.DateKey,
			[WWICustomerID] = C.WWICustomerID,
			[WWIBillToCustomerID] = BC.WWICustomerID,
			[WWISupplierID] = S.WWISupplierID,
			[WWITransactionTypeID] = DT.WWITransactionTypeID,
			[WWIPaymentMethodID] = PM.WWIPaymentMethodID,
			[WWICustomerTransactionID] = T.WWICustomerTransactionID,
			[WWISupplierTransactionID] = T.WWISupplierTransactionID,
			[WWIInvoiceID] = T.WWIInvoiceID,
			[WWIPurchaseOrderID] = T.WWIPurchaseOrderID,
			[SupplierInvoiceNumber] = T.SupplierInvoiceNumber,
			[TotalExcludingTax] = T.TotalExcludingTax,
			[TaxAmount] = T.TaxAmount,
			[TotalIncludingTax] = T.TotalIncludingTax,
			[OutstandingBalance] = T.OutstandingBalance,
			[IsFinalized] = T.IsFinalized
		FROM
			dbo.FctTransaction T LEFT JOIN
			dbo.DimCustomer C ON
				T.CustomerKey = C.CustomerKey AND
				C.LoadDate <= @CutoffDate LEFT JOIN
			dbo.DimCustomer BC ON
				T.BillToCustomerKey = BC.CustomerKey AND
				BC.LoadDate <= @CutoffDate LEFT JOIN
			dbo.DimSupplier S ON
				T.SupplierKey = S.SupplierKey AND
				S.LoadDate <= @CutoffDate LEFT JOIN
			dbo.DimTransactionType DT ON
				T.TransactionTypeKey = DT.TransactionTypeKey AND
				DT.LoadDate <= @CutoffDate LEFT JOIN
			dbo.DimPaymentMethod PM ON
				T.PaymentMethodKey = PM.PaymentMethodKey AND
				PM.LoadDate <= @CutoffDate
		WHERE
			T.LoadDate <= @CutoffDate AND
			T.TransactionKey > 0

	),

	MismatchCount AS
	(

		SELECT
			[Error] = @MismatchCountError,
			[Details] = 'Non-warehouse tables has ' + CAST(O.[Count] AS NVARCHAR(50)) + ' records compared to warehouse table with ' + CAST(W.[Count] AS NVARCHAR(50)) + ' records.'
		FROM
			(
				SELECT
					[Count] = COUNT(*)
				FROM		
					FctTransactionOriginal
			) O LEFT JOIN
			(
				SELECT
					[Count] = COUNT(*)
				FROM
					FctTransactionWarehouse
			) W ON 1 = 1
		WHERE
			O.[Count] <> W.[Count]

	),

	MismatchValues AS
	(

		SELECT
			[Error] = @MismatchValueError,
			[Details] = 'There are ' + CAST(M.[Count] AS NVARCHAR(50)) + ' records with mismatched values.'
		FROM
		(	
			SELECT TOP 1
				*
			FROM
			(
				SELECT
					[Count] = COUNT(*)
				FROM
					FctTransactionOriginal O LEFT JOIN
					FctTransactionWarehouse W ON
						ISNULL(W.WWICustomerTransactionID, 0) = ISNULL(O.WWICustomerTransactionID, 0) AND
						ISNULL(W.WWISupplierTransactionID, 0) = ISNULL(O.WWISupplierTransactionID, 0)
				WHERE
					O.[DateKey] <> W.[DateKey] OR
					ISNULL(O.[WWICustomerID], 0) <> ISNULL(W.[WWICustomerID], 0) OR
					ISNULL(O.[WWIBillToCustomerID], 0) <> ISNULL(W.[WWIBillToCustomerID], 0) OR
					ISNULL(O.[WWISupplierID], 0) <> ISNULL(W.[WWISupplierID], 0) OR
					O.[WWITransactionTypeID] <> W.[WWITransactionTypeID] OR
					ISNULL(O.[WWIPaymentMethodID], 0) <> ISNULL(W.[WWIPaymentMethodID], 0) OR
					ISNULL(O.[WWIInvoiceID], 0) <> ISNULL(W.[WWIInvoiceID], 0) OR
					ISNULL(O.[WWIPurchaseOrderID], 0) <> ISNULL(W.[WWIPurchaseOrderID], 0) OR
					ISNULL(O.[SupplierInvoiceNumber], '') <> ISNULL(W.[SupplierInvoiceNumber], '') OR
					O.[TotalExcludingTax] <> W.[TotalExcludingTax] OR
					O.[TaxAmount] <> W.[TaxAmount] OR
					O.[TotalIncludingTax] <> W.[TotalIncludingTax] OR
					O.[OutstandingBalance] <> W.[OutstandingBalance] OR
					O.[IsFinalized] <> W.[IsFinalized]
			) M 
		) M
		WHERE
			M.[Count] > 0

	)

	INSERT INTO [dbo].[DataValidationErrors]
	(
		[CutoffDate],
		[Error],
		[Details],
		[Schema],
		[Table],
		[Process]
	)
	SELECT
		@CutoffDate,
		Error,
		Details,
		@Schema,
		@Table,
		@Process
	FROM
		MismatchCount
	
	UNION

	SELECT
		@CutoffDate,
		Error,
		Details,
		@Schema,
		@Table,
		@Process
	FROM
		MismatchValues

	COMMIT TRAN

END;
GO
/****** Object:  StoredProcedure [dbo].[ValidateForeignKeyFields]    Script Date: 10/13/2025 10:24:08 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


/***

EXEC [dbo].[ValidateForeignKeyFields]
	@Process='warehouse_fact_wwi',
	@CutoffDate='10/12/2025',
	@Schema='dbo',
	@Table='FctTransaction',
	@ForeignKeyFields='
		[
			{
				"ForeignKey": "DateKey",
				"Primary": 
				{
					"Key": "Date",
					"Schema": "dbo",
					"Table": "DimDate"
				}
			},
			{
				"ForeignKey": "StockItemKey",
				"Primary": 
				{
					"Key": "StockItemKey",
					"Schema": "dbo",
					"Table": "DimStockItem"
				}
			}
		] 
	'

SELECT * FROM DataValidationErrors

***/
CREATE PROCEDURE [dbo].[ValidateForeignKeyFields]
(
	@Process NVARCHAR(50),
	@CutoffDate DATETiME,
	@Schema NVARCHAR(50),
	@Table NVARCHAR(50),
	@ForeignKeyFields NVARCHAR(MAX),
	@PurgeExisting BIT = 1
)
AS
BEGIN

    SET NOCOUNT ON;
    SET XACT_ABORT ON;

	DECLARE @Error NVARCHAR(50) = 'Foreign Key Constraint Error'
	DECLARE @Sql NVARCHAR(MAX) = '
		INSERT INTO [dbo].[DataValidationErrors]
		(
			[CutoffDate],
			[Error],
			[Details],
			[Schema],
			[Table],
			[Process]
		)
	'
		
	SELECT
		@Sql += '
	
		SELECT 
			[CutoffDate] = @CutoffDate,
			[Error] = @Error,
			[Details] = ''There are '' + CAST(T.[Count] AS NVARCHAR(50)) + '' records for foreign key ' + [ForeignKey] + ' in '' + @Schema + ''.'' + @Table + '' table with missing primary key ' + [PrimaryKey] + ' in ' + [PrimarySchema] + '.' + [PrimaryTable] + ' table.'',
			[Schema] = @Schema,
			[Table] = @Table,
			[Process] = @Process
		FROM
			(
				SELECT
					[ForeignKey] = ''' + [ForeignKey] + ''',
					[PrimaryKey] = ''' + [PrimaryKey] + ''',
					[PrimarySchema] = ''' + [PrimarySchema] + ''',
					[PrimaryTable] = ''' + [PrimaryTable] + ''',
					[Count] = COUNT(*)
				FROM
					[' + @Schema + '].[' + @Table + ']
				WHERE
					[' + [ForeignKey] + '] NOT IN
					(
						SELECT
							[' + [PrimaryKey] + ']
						FROM
							[' + PrimarySchema + '].[' + PrimaryTable + ']
						WHERE
							[LoadDate] <= @CutoffDate
					) AND
					[LoadDate] <= @CutoffDate
			) T
		WHERE
			T.[Count] > 0

		UNION'
	FROM
		OPENJSON(@ForeignKeyFields) WITH (
			[ForeignKey] NVARCHAR(100) '$.ForeignKey',
			[PrimaryKey] NVARCHAR(100) '$.Primary.Key',
			[PrimarySchema] NVARCHAR(100) '$.Primary.Schema',
			[PrimaryTable] NVARCHAR(100) '$.Primary.Table'
		)

	SET @Sql = RTRIM(@Sql, 'UNION')

	PRINT @Sql

	BEGIN TRAN

	IF @PurgeExisting = 1 
	BEGIN

		DELETE
			[dbo].[DataValidationErrors]
		WHERE
			[CutoffDate] = @CutoffDate AND
			[Error] = @Error AND
			[Schema] = @Schema AND
			[Table] = @Table AND
			[Process] = @Process

	END

	EXEC sp_executesql @Sql, N'@Process NVARCHAR(50), @CutoffDate DATETIME, @Schema NVARCHAR(50), @Table NVARCHAR(50), @Error NVARCHAR(50)', 
		@Process, @CutoffDate, @Schema, @Table, @Error

	COMMIT TRAN

END;
GO
/****** Object:  StoredProcedure [dbo].[ValidateNotNullFields]    Script Date: 10/13/2025 10:24:08 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


/***

EXEC [dbo].[ValidateNotNullFields]
	@Process='warehouse_dimension_wwi',
	@CutoffDate='1/1/2013',
	@Schema='dbo',
	@Table='FctTransaction',
	@NotNullFields='WWICustomerTransactionID,WWISupplierTransactionID'

SELECT * FROM [dbo].[DataValidationErrors]

***/
CREATE PROCEDURE [dbo].[ValidateNotNullFields]
(
	@Process NVARCHAR(50),
	@CutoffDate DATETiME,
	@Schema NVARCHAR(50),
	@Table NVARCHAR(50),
	@NotNullFields NVARCHAR(MAX),
	@PurgeExisting BIT = 1
)
AS
BEGIN

    SET NOCOUNT ON;
    SET XACT_ABORT ON;

	DECLARE @Error NVARCHAR(50) = 'Not Null Field Error'
	DECLARE @Sql NVARCHAR(MAX) = '
		INSERT INTO [dbo].[DataValidationErrors]
		(
			[CutoffDate],
			[Error],
			[Details],
			[Schema],
			[Table],
			[Process]
		)
	'

	SELECT
		@Sql += '

			SELECT 
				[CutoffDate] = @CutoffDate,
				[Error] = @Error,
				[Details] = ''There are '' + CAST(COUNT(*) AS NVARCHAR(50)) + '' records in '' + @Schema + ''.'' + @Table + '' with null values in ' + [Value] + ' field.'',
				[Schema] = @Schema,
				[Table] = @Table,
				[Process] = @Process
			FROM
				[' + @Schema + '].[' + @Table + ']
			WHERE
				[LoadDate] <= @CutoffDate AND
				[' + Value + '] IS NULL
			HAVING
				COUNT(*) > 0

			UNION'
	FROM
		STRING_SPLIT(@NotNullFields, ',')

	SET @Sql = RTRIM(@Sql, 'UNION')

	BEGIN TRAN

	IF @PurgeExisting = 1 
	BEGIN

		DELETE
			[dbo].[DataValidationErrors]
		WHERE
			[CutoffDate] = @CutoffDate AND
			[Error] = @Error AND
			[Schema] = @Schema AND
			[Table] = @Table AND
			[Process] = @Process

	END

	EXEC sp_executesql @Sql, N'@Process NVARCHAR(50), @CutoffDate DATETIME, @Schema NVARCHAR(50), @Table NVARCHAR(50), @Error NVARCHAR(50)', 
		@Process, @CutoffDate, @Schema, @Table, @Error

	COMMIT TRAN

END;
GO
/****** Object:  StoredProcedure [dbo].[ValidateNotNullUniqueFields]    Script Date: 10/13/2025 10:24:08 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

/***

EXEC [dbo].[ValidateNotNullUniqueFields]
	@Process='warehouse_dimension_wwi',
	@CutoffDate='1/1/2013',
	@Schema='dbo',
	@Table='FctTransaction',
	@NotNullUniqueFields='WWICustomerTransactionID,WWISupplierTransactionID'

SELECT * FROM [dbo].[DataValidationErrors]

***/
CREATE PROCEDURE [dbo].[ValidateNotNullUniqueFields]
(
	@Process NVARCHAR(50),
	@CutoffDate DATETiME,
	@Schema NVARCHAR(50),
	@Table NVARCHAR(50),
	@NotNullUniqueFields NVARCHAR(MAX),
	@PurgeExisting BIT = 1
)
AS
BEGIN

    SET NOCOUNT ON;
    SET XACT_ABORT ON;

	DECLARE @Error NVARCHAR(50) = 'Not Null Unique Field Error'
	DECLARE @Sql NVARCHAR(MAX) = '
		INSERT INTO [dbo].[DataValidationErrors]
		(
			[CutoffDate],
			[Error],
			[Details],
			[Schema],
			[Table],
			[Process]
		)
	'

	SELECT
		@Sql += '

			SELECT 
				[CutoffDate] = @CutoffDate,
				[Error] = @Error,
				[Details] = ''There are '' + CAST(COUNT(U.[Count]) AS NVARCHAR(50)) + '' records in '' + @Schema + ''.'' + @Table + '' with non-unique values in ' + [Value] + ' field.'',
				[Schema] = @Schema,
				[Table] = @Table,
				[Process] = @Process
			FROM
				(
					SELECT
						[Field] = [' + [Value] + '],
						[Count] = COUNT([' + [Value] + '])
					FROM
						[' + @Schema + '].[' + @Table + ']
					WHERE
						[LoadDate] <= @CutoffDate AND
						[' + Value + '] IS NOT NULL
					GROUP BY
						[' + [Value] + ']
					HAVING
						COUNT([' + [Value] + ']) > 1
				) U
			WHERE
				U.[Count] > 0
			HAVING
				COUNT(U.[Count]) > 0

			UNION'
	FROM
		STRING_SPLIT(@NotNullUniqueFields, ',')

	SET @Sql = RTRIM(@Sql, 'UNION')

	BEGIN TRAN

	IF @PurgeExisting = 1 
	BEGIN

		DELETE
			[dbo].[DataValidationErrors]
		WHERE
			[CutoffDate] = @CutoffDate AND
			[Error] = @Error AND
			[Schema] = @Schema AND
			[Process] = @Process

	END

	EXEC sp_executesql @Sql, N'@Process NVARCHAR(50), @CutoffDate DATETIME, @Schema NVARCHAR(50), @Table NVARCHAR(50), @Error NVARCHAR(50)', 
		@Process, @CutoffDate, @Schema, @Table, @Error

	COMMIT TRAN

END;
GO
/****** Object:  StoredProcedure [dbo].[ValidateUniqueFields]    Script Date: 10/13/2025 10:24:08 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO



/***

EXEC [dbo].[ValidateUniqueFields]
	@Process='warehouse_dimension_wwi',
	@CutoffDate='1/1/2013',
	@Schema='dbo',
	@Table='FctTransaction',
	@NotNullUniqueFields='WWICustomerTransactionID,WWISupplierTransactionID'

SELECT * FROM [dbo].[DataValidationErrors]

***/
CREATE PROCEDURE [dbo].[ValidateUniqueFields]
(
	@Process NVARCHAR(50),
	@CutoffDate DATETiME,
	@Schema NVARCHAR(50),
	@Table NVARCHAR(50),
	@UniqueFields NVARCHAR(MAX),
	@PurgeExisting BIT = 1
)
AS
BEGIN

    SET NOCOUNT ON;
    SET XACT_ABORT ON;

	DECLARE @Error NVARCHAR(50) = 'Unique Field Error'
	DECLARE @Sql NVARCHAR(MAX) = '
		INSERT INTO [dbo].[DataValidationErrors]
		(
			[CutoffDate],
			[Error],
			[Details],
			[Schema],
			[Table],
			[Process]
		)
	'

	SELECT
		@Sql += '

			SELECT 
				[CutoffDate] = @CutoffDate,
				[Error] = @Error,
				[Details] = ''There are '' + CAST(COUNT(U.[Count]) AS NVARCHAR(50)) + '' records in '' + @Schema + ''.'' + @Table + '' with non-unique values in ' + [Value] + ' field.'',
				[Schema] = @Schema,
				[Table] = @Table,
				[Process] = @Process
			FROM
				(
					SELECT
						[Field] = [' + [Value] + '],
						[Count] = COUNT([' + [Value] + '])
					FROM
						[' + @Schema + '].[' + @Table + ']
					WHERE
						[LoadDate] <= @CutoffDate
					GROUP BY
						[' + [Value] + ']
					HAVING
						COUNT([' + [Value] + ']) > 1
				) U
			WHERE
				U.[Count] > 0
			HAVING
				COUNT(U.[Count]) > 0

			UNION'
	FROM
		STRING_SPLIT(@UniqueFields, ',')

	SET @Sql = RTRIM(@Sql, 'UNION')

	BEGIN TRAN

	IF @PurgeExisting = 1 
	BEGIN

		DELETE
			[dbo].[DataValidationErrors]
		WHERE
			[CutoffDate] = @CutoffDate AND
			[Error] = @Error AND
			[Schema] = @Schema AND
			[Process] = @Process

	END

	EXEC sp_executesql @Sql, N'@Process NVARCHAR(50), @CutoffDate DATETIME, @Schema NVARCHAR(50), @Table NVARCHAR(50), @Error NVARCHAR(50)', 
		@Process, @CutoffDate, @Schema, @Table, @Error

	COMMIT TRAN

END;
GO
USE [master]
GO
ALTER DATABASE [WideWorldImportersDW] SET  READ_WRITE 
GO
