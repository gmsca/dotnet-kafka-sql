USE [CIMS]
GO
/****** Object:  StoredProcedure [dbo].[Bridge_CreateGroup]    Script Date: 07-Jan-2021 9:24:38 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		Josh Wiebe
-- Create date: 07-Jan-2021
-- Description:	Used to create group for the CIMS Bridge
-- =============================================
CREATE PROCEDURE [dbo].[Bridge_CreateGroup]
	-- Add the parameters for the stored procedure here

	-- These Come From Bridge
	@CompanyName VARCHAR(35) , -- REQUIRED INPUT
	@GroupClassification VARCHAR(50), -- Required must be from the GCL_Description from CIMS.Reference.GroupClassification
	@AddressLine1 VARCHAR(50) , -- Required
	@City VARCHAR(50) , -- Required
	@Province VARCHAR(2), -- Required, will query CIMS.Reference.Province to get @ProvID, and @CountryID as well ***SK, MB, ETC...***
	@PostalCode CHAR(6), -- Required
	@UserID VARCHAR(50) , -- REQUIRED

	-- Rest DO NOT need to be declared, but can be if required...

	@PhoneNumber VARCHAR(12) = null, -- Not Required, uses format ***123-456-7890***
	@PhoneType VARCHAR(12) = null, -- Not Required, must be in format ***Home, Work, Fax, Billing '_Contact'(will need to be appended by sproc), Company***
	@EmailAddress VARCHAR(1024) = null, -- Not Required
	@EmailType VARCHAR(50) = null, -- Not Required, must be ***Primary or Administrative*** will lookup in CIMS.Reference.EmailAddressType

	@ContractID INT = -1 OUT, -- NOT Required
    @ContractType INT = 0, -- NOT Required
    @ContactName1 VARCHAR(40) = '', -- NOT Required
    @ContactName2 VARCHAR(40) = '', -- NOT Required
    @AssociationID INT = null, -- NOT Required
    @TimeStamp DATETIME = null, -- NOT Required
    @MaxDependentAge INT = NULL , -- NOT Required
    @MaxStudentAge INT = NULL , -- NOT Required
    @GroupClassificationID INT = NULL , -- NOT Required Set by CIMS.Reference.GroupClassification
    @ExternalDesc VARCHAR(50) = NULL , -- NOT Required
    @GroupFeed BIT = 0, -- NOT Required
    @GroupSendDate DATETIME = null, -- NOT Required

    @EmailID INT = -1, -- NOT Required
    @EmailAddressType INT = null , -- Get from CIMS.Reference.EmailAddressType set by @EmailType

    @PhoneTypeID INT = null, -- Get from CIMS.Reference.PhoneType set by @PhoneType
    @PhoneID INT = -1, -- NOT Required
    @AreaCode SMALLINT = null, -- Set by @PhoneNumber
    @Number VARCHAR(50) = null, -- Set by @PhoneNumber

	@AddressType VARCHAR(50) = 'Company', -- NOT Required (Buisness, Home, Billing, Company) ***Must be these*** Used to set @AddressTypeID
    @AddressTypeID INT = null, -- Get from CIMS.Reference.AddressType Will be infered from @AddressType
    @AddressID INT = -1 , -- NOT Required
    @AddressLine2 VARCHAR(50) = '', -- NOT Required
	@CountryID INT = null, -- Get from CIMS.Reference.Country will be set by @Province
    @ProvID INT = null -- Get from CIMS.Reference.Province, and can set @CountryID as well!!!

AS
BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;

	SET @TimeStamp = (SELECT GETDATE())
	SET @GroupClassificationID = (SELECT GCL_GroupClassificationID FROM CIMS.Reference.GroupClassification WHERE GCL_Description = @GroupClassification)

    -- Insert statements for procedure here
	exec DBO.Customer_SaveGroup 
	@ContractID OUTPUT,
	@ContractType,
	@CompanyName,
	@ContactName1,
	@ContactName2, 
	@AssociationID, 
	@UserID, 
	@TimeStamp, 
	@MaxDependentAge,
    @MaxStudentAge,
    @GroupClassificationID,
    @ExternalDesc,
    @GroupFeed,
    @GroupSendDate;


	SET @CountryID = (select CF_CountryID from CIMS.Reference.Province where PV_Abbreviation = @Province)
	SET @ProvID = (select PV_ProvID from CIMS.Reference.Province where PV_Abbreviation = @Province)
	SET @AddressTypeID = (select AT_AddressTypeID from CIMS.Reference.AddressType WHERE AT_Description = @AddressType)

	IF @ProvID = 1
	BEGIN
		EXEC dbo.Reference_ContractCompanyLink_Insert @ContractID, 1, @UserID, 0, null
	END
	ELSE
	BEGIN
		EXEC dbo.Reference_ContractCompanyLink_Insert @ContractID, 2, @UserID, 0, null
	END

	exec DBO.Customer_SaveContractAddress 
	@ContractID, 
	@AddressTypeID, 
	@AddressID, 
	@AddressLine1, 
	@AddressLine2, 
	@CountryID, 
	@City, 
	@ProvID, 
	@PostalCode, 
	@UserID, 
	@TimeStamp

	IF @PhoneNumber IS NOT NULL AND @PhoneType IS NOT NULL
	BEGIN
		SET @AreaCode = (SELECT CAST((SUBSTRING(@PhoneNumber, 1, 3)) AS SMALLINT))
		SET @Number = (SELECT REPLACE(SUBSTRING(@PhoneNumber, 5, 8), '-', ''))

		-- PHONE NUMBER TYPE CAN'T BE BILLING FOR GROUP, CIMS THROWS NULL ERROR (I'll leave it anyways)
		IF @PhoneType = 'Billing'
		BEGIN
			SET @PhoneType = (@PhoneType+'_Contact')
		END

		SET @PhoneTypeID = (SELECT PT_PhoneTypeID FROM CIMS.Reference.PhoneType WHERE PT_Description = @PhoneType)

		EXEC dbo.Customer_SaveContractPhone 
		@ContractID, 
		@PhoneTypeID, 
		@PhoneID,
		@AreaCode, 
		@Number, 
		@UserID, 
		@TimeStamp
	END

	IF @EmailAddress IS NOT NULL AND @EmailType IS NOT NULL
	BEGIN
		SET @EmailAddressType = (SELECT EAT_EmailAddressTypeID FROM CIMS.Reference.EmailAddressType WHERE EAT_Description = @EmailType)

		EXEC dbo.Customer_SaveContractEmail @ContractID, 
		@EmailID, 
		@EmailAddress, 
		@EmailAddressType, 
		@UserID, 
		@TimeStamp
	END

END