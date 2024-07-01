SELECT COUNT(*),
"SellerLegalName",
"SellerTaxCode" 
FROM "MInvoice"."Invoice" 
WHERE "TenantId"='?' and "SendTaxStatus"=3
GROUP BY 
"SellerLegalName",
"SellerTaxCode";