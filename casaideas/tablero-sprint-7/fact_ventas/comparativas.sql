--SELECT count(*) FROM "ci_db"."dev"."fct_ventas_dev";
WITH dev AS (
    SELECT fecha, SUM(venta_neta) AS venta_neta FROM "ci_db"."dev"."fct_ventas_dev"
    where fecha>='2022-01-01'
    group by fecha
    ORDER BY fecha DESC
)
,
prod AS (
    SELECT fecha, SUM(venta_neta) AS venta_neta FROM "ci_db"."prod"."fct_ventas"
    where fecha>='2022-01-01'
    group by fecha
    ORDER BY fecha DESC
)
SELECT dev.fecha, dev.venta_neta as venta_neta_dev, prod.venta_neta as venta_neta_prod
FROM dev JOIN prod ON dev.fecha=prod.fecha
ORDER BY fecha DESC;

SELECT
  DATE_PART(YEAR,fecha) as ano,
--  EXTRACT(year FROM fecha) AS year,
  SUM(venta_neta) AS venta_neta
FROM "ci_db"."dev"."fct_ventas_dev"
GROUP BY ano;


SELECT
  DATE_PART(YEAR,fecha) as ano,
--  EXTRACT(year FROM fecha) AS year,
  SUM(venta_neta) AS venta_neta
FROM "ci_db"."prod"."fct_ventas"
GROUP BY ano;
--