IF NOT EXISTS (
    SELECT 1
    FROM sys.tables
    WHERE name = 'Api_Bill_Delivery_Updates'
      AND schema_id = SCHEMA_ID('dbo')
)
BEGIN
    CREATE TABLE dbo.Api_Bill_Delivery_Updates
    (
        Id INT IDENTITY(1,1) PRIMARY KEY,
        Bill_UID NVARCHAR(128) NOT NULL,
        RequestedAt DATETIME2(0) NOT NULL
            CONSTRAINT DF_Api_Bill_Delivery_Updates_RequestedAt
            DEFAULT SYSUTCDATETIME()
    );
END;
GO


CREATE OR ALTER PROCEDURE dbo.Api_Mark_Bills_Delivered_Bulk
    @Ack_Id NVARCHAR(100) = NULL
AS
BEGIN
    SET NOCOUNT ON;

    ;WITH PendingBills AS (
        SELECT DISTINCT Bill_UID
        FROM dbo.Api_Bill_Delivery_Updates
    )
    UPDATE so
    SET
        so.Status = 'DELIVERED',
        so.Ack_Id = COALESCE(@Ack_Id, so.Ack_Id)
    FROM dbo.Api_Sales_Outbox AS so
    INNER JOIN PendingBills AS pb
        ON so.Bill_UID = pb.Bill_UID
    WHERE so.Status IN ('NEW', 'PENDING');

    -- Optional: return deterministic result
    SELECT
        pb.Bill_UID,
        COUNT(*) AS RowsAffected
    FROM dbo.Api_Sales_Outbox so
    INNER JOIN PendingBills pb
        ON so.Bill_UID = pb.Bill_UID
    WHERE so.Status = 'DELIVERED'
    GROUP BY pb.Bill_UID;

    -- Clear staging table
    DELETE FROM dbo.Api_Bill_Delivery_Updates;
END;
GO
