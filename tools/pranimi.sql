CREATE OR ALTER PROCEDURE dbo.Api_GetPranimInvoiceLines_ForApi
AS
BEGIN
    SET NOCOUNT ON;

    SELECT
        /* ================= HEADER ================= */
        f.ID_Fatura,
        f.Emri_Furnitorit,
        f.Data,
        f.Njesia                    AS NjesiaFatura,

        /* ================= LINE =================== */
        p.EmertimiA,
        p.NjesiaM,
        p.ShifraF,

        CAST(p.Sasia AS DECIMAL(18,4))         AS Quantity,
        CAST(p.CmimiDokument AS DECIMAL(18,4)) AS UnitPrice,
        CAST(p.TaxRate AS DECIMAL(9,4))        AS TaxRate,
        CAST(p.Zbritje AS DECIMAL(9,4))        AS DiscountPercent,

        CAST(
            p.Sasia *
            (p.CmimiDokument - (p.CmimiDokument * p.Zbritje / 100.0))
            AS DECIMAL(18,4)
        ) AS LineNet,

        CAST(
            p.Sasia *
            (p.CmimiDokument - (p.CmimiDokument * p.Zbritje / 100.0)) *
            (p.TaxRate / 100.0)
            AS DECIMAL(18,4)
        ) AS LineTax

    FROM dbo.Pranim_FaturaTransfers f
    INNER JOIN dbo.Pranim_InvoiceStatus s
        ON s.ID_Fatura = f.ID_Fatura
    INNER JOIN dbo.Pranim_ProduktTransfers p
        ON p.ID_Fatura = f.ID_Fatura

    WHERE
        s.Status_Api = 'PENDING'

    ORDER BY
        f.ID_Fatura,
        p.EmertimiA;
END;
GO
