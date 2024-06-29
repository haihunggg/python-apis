SELECT
  "SellerTaxCode" AS "Taxcode",
  "SellerLegalName",
  SUM(CASE WHEN "SendTaxStatus"=3  THEN 1 ELSE 0 END) AS "SendingInvoice"
FROM
  "MInvoice"."Invoice"
WHERE
  "SellerTaxCode" NOT IN ('0106026495-998', '0106026495-999')
  AND "SendTaxStatus"=3
  AND "DateSign" <= NOW() - INTERVAL '6 hours'
GROUP BY
  "SellerTaxCode",
  "SellerLegalName",
  "TenantId"
ORDER BY
  "SellerTaxCode" ASC;
