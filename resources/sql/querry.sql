SELECT "DateSign","CreationTime", "SellerTaxCode","SellerLegalName", "InvoiceSerial", "BuyerLegalName","Id"
FROM "MInvoice"."Invoice"
where "SendTaxStatus" = 4
--AND "Id" NOT IN (SELECT "IdAction" FROM "MInvoice"."GatewayRequest")
and "InvoiceDate" BETWEEN '20240101' and '20240531'
--and "CreationTime"='2024-06-08 10:59:48.26969+00'
ORDER BY "SellerTaxCode" ASC
