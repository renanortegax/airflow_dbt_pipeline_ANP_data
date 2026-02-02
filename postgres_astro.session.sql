SELECT
table_schema
,table_name
FROM information_schema.tables
WHERE 1=1
    and table_schema NOT IN ('pg_catalog', 'information_schema')
    -- AND table_type = 'BASE TABLE';;


select *
from raw_diesel_gnv
limit 100;


drop table raw_diesel_gnv;
drop table raw_gasolina_etanol;
drop table raw_glp;


SELECT * FROM "postgres"."public"."model1"