--SELECT count(*) FROM "ci_db"."dev"."fct_ventas_dev";
WITH dev AS (
    SELECT fecha, SUM(venta_neta) AS venta_neta FROM "ci_db"."dev"."fct_ventas_dev"
    where fecha>='2022-01-01'
    group by fecha
    ORDER BY fecha ASC
)
,
prod AS (
    SELECT fecha, SUM(venta_neta) AS venta_neta FROM "ci_db"."prod"."fct_ventas"
    where fecha>='2022-01-01'
    group by fecha
    ORDER BY fecha ASC
)
SELECT dev.fecha, dev.venta_neta as venta_neta_dev, prod.venta_neta as venta_neta_prod
FROM dev JOIN prod ON dev.fecha=prod.fecha
ORDER BY fecha ASC;

WITH dev AS (
    SELECT
    DATE_PART(YEAR,fecha) as ano,
    --  EXTRACT(year FROM fecha) AS year,
    SUM(venta_neta) AS venta_neta
    FROM "ci_db"."dev"."fct_ventas_dev"
    GROUP BY ano
)
,
prod as (
    SELECT
    DATE_PART(YEAR,fecha) as ano,
    --  EXTRACT(year FROM fecha) AS year,
    SUM(venta_neta) AS venta_neta
    FROM "ci_db"."prod"."fct_ventas"
    GROUP BY ano
)
SELECT dev.ano, dev.venta_neta as venta_neta_dev, prod.venta_neta as venta_neta_prod
FROM dev JOIN prod ON dev.ano=prod.ano
ORDER BY ano ASC;


-- SELECT
--   DATE_PART(YEAR,fecha) as ano,
-- --  EXTRACT(year FROM fecha) AS year,
--   SUM(venta_neta) AS venta_neta
-- FROM "ci_db"."prod"."fct_ventas"
-- GROUP BY ano;
-- --