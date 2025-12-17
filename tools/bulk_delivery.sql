-- Staging table for bulk bill delivery requests
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'Api_Bill_Delivery_Updates' AND schema_id = SCHEMA_ID('dbo'))
BEGIN
    CREATE TABLE dbo.Api_Bill_Delivery_Updates
    (
        Id INT IDENTITY(1,1) PRIMARY KEY,
        Bill_UID NVARCHAR(128) NOT NULL,
        RequestedAt DATETIME2(0) NOT NULL CONSTRAINT DF_Api_Bill_Delivery_Updates_RequestedAt DEFAULT SYSUTCDATETIME()
    );
END
GO

-- Bulk procedure that applies delivery updates and clears the staging table
CREATE OR ALTER PROCEDURE dbo.Api_Mark_Bills_Delivered_Bulk
AS
BEGIN
    SET NOCOUNT ON;

    -- Apply delivery flag to all unique bills staged for processing
    ;WITH PendingBills AS (
        SELECT DISTINCT Bill_UID
        FROM dbo.Api_Bill_Delivery_Updates
    )
    UPDATE so
    SET DeliveryStatus = 'Delivered',
        UpdatedAt = SYSUTCDATETIME()
    FROM dbo.Api_Sales_Outbox AS so
    INNER JOIN PendingBills AS pb
        ON CAST(so.BillId AS NVARCHAR(128)) = pb.Bill_UID;

    -- Remove processed entries
    DELETE FROM dbo.Api_Bill_Delivery_Updates;
END
GO
