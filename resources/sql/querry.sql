SELECT
    "SellerTaxCode" AS "Taxcode",
    "SellerLegalName",
    COUNT(*) AS "InvoiceCount"
FROM
    "MInvoice"."Invoice"
WHERE
    "SendTaxStatus" = 3
    --AND "DateSign" >= NOW() - INTERVAL '6 HOURS'
GROUP BY
    "SellerTaxCode",
    "SellerLegalName"
ORDER BY
    "SellerTaxCode" ASC;


