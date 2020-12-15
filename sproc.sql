-- ================================================
-- Template generated from Template Explorer using:
-- Create Procedure (New Menu).SQL
--
-- Use the Specify Values for Template Parameters 
-- command (Ctrl-Shift-M) to fill in the parameter 
-- values below.
--
-- This block of comments will not be included in
-- the definition of the procedure.
-- ================================================
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		Josh Wiebe
-- Create date: 15-Dec-2020
-- Description:	Perform actions to the test table
-- =============================================
CREATE PROCEDURE [dbo].[UpdateTest]
    -- Add the parameters for the stored procedure here
    @ClaimID int,
    @Description varchar(255) = null,
    @FeeSubmitted money,
    @TotalOwed money,
    @State varchar(255),
    @Paid bit,
    @StatementType VARCHAR(20) = ''
AS
BEGIN
    -- SET NOCOUNT ON added to prevent extra result sets from
    -- interfering with SELECT statements.
    SET NOCOUNT ON;

    -- Statements
    IF @StatementType = 'Insert'  
        BEGIN
        INSERT INTO [dbo].[Test]
            (ClaimID,
            Description,
            FeeSubmitted,
            TotalOwed,
            State,
            Paid)
        VALUES
            ( @ClaimID,
                @Description,
                @FeeSubmitted,
                @TotalOwed,
                @State,
                @Paid)
    END

    IF @StatementType = 'Update'  
        BEGIN
        UPDATE [dbo].[Test] 
            SET	   Description = @Description,  
                   FeeSubmitted = @FeeSubmitted,  
                   TotalOwed = @TotalOwed,
				   State = @State,
				   Paid = @Paid
            WHERE  ClaimID = @ClaimID
    END  
      ELSE IF @StatementType = 'Delete'  
        BEGIN
        DELETE FROM [dbo].[Test]  
            WHERE  ClaimID = @ClaimID
    END

END
GO