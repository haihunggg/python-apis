SELECT
  a."SellerTaxCode" AS "Taxcode",
  a."SellerLegalName",
  SUM(CASE WHEN b."IdAction" IS NULL THEN 1 ELSE 0 END) AS "SendingInvoiceHaveRequest",
  SUM(CASE WHEN b."IdAction" IS NOT NULL THEN 1 ELSE 0 END) AS "SendingInvocieNoRequest"
FROM
  "MInvoice"."Invoice" a
LEFT JOIN "MInvoice"."GatewayRequest" b
ON a."TenantId" = b."TenantId"
WHERE
  a."SendTaxStatus" = 3
-- AND a."DateSign" >= NOW() - INTERVAL '6 HOURS'
GROUP BY
  a."SellerTaxCode",
  a."SellerLegalName"
ORDER BY
  a."SellerTaxCode" ASC;
 thô