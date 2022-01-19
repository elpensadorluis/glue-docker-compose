--SELECT count(*) FROM "ci_db"."dev"."fct_ventas_dev";

--SELECT fecha, SUM(venta_neta) AS venta_neta FROM "ci_db"."dev"."fct_ventas_dev"
--where fecha>='2022-01-01'
--group by fecha
--;
--
--SELECT fecha, SUM(venta_neta) AS venta_neta FROM "ci_db"."prod"."fct_ventas"
--where fecha>='2022-01-01'
--group by fecha
--;

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