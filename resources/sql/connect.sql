SELECT a."TenantId", a."Name", a."Value",b."Name"
FROM "public"."AbpTenantConnectionStrings" a JOIN "AbpTenants" b on b."Id" = a."TenantId";