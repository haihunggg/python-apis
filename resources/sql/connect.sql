SELECT "TenantId", "Name", "Value"
FROM public."AbpTenantConnectionStrings"
WHERE  "TenantId" in  (SELECT "Id" FROM public."AbpTenants")
LIMIT 10;