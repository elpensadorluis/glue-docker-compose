--create table dev.fct_ventas_dev (
--id_venta VARCHAR,
--id_producto VARCHAR(34),
--id_tiempo VARCHAR(16),
--centro_id_sap VARCHAR(6),
--  --  centro_id_sap_2 VARCHAR(6),
--centro_tipo VARCHAR(6) default 'Otros',
--num_material VARCHAR(18),
--sku VARCHAR(18),
--org_ventas VARCHAR,
--canal VARCHAR,
--canal_desc VARCHAR,
--cluster_operacional VARCHAR,
--flag_recla INT,
--venta_neta DOUBLE PRECISION,
--costo DOUBLE PRECISION,
--contribucion DOUBLE PRECISION,
--unidades DOUBLE PRECISION,
--precio_unitario DOUBLE PRECISION,
--fecha DATE,
--pais VARCHAR,
--moneda VARCHAR
--);

TRUNCATE dev.fct_ventas_dev;

INSERT INTO
	dev.fct_ventas_dev(
       fecha
       ,centro_id_sap
       ,num_material
       ,sku
       ,org_ventas
       ,canal
       ,canal_desc
       ,cluster_operacional
       ,flag_recla
       ,id_venta
       ,id_producto
       ,id_tiempo
       ,venta_neta
       ,costo
       ,contribucion
       ,unidades
       ,precio_unitario
       ,pais
       ,moneda
	)
SELECT 
    v.fkdat AS fecha				--Fecha de factura para el índice de factura e impresión
    ,v.werks_recla  		--centro
    ,v.matnr AS num_material				--num material
    ,LTRIM(v.matnr,'0') AS sku              --sku
    ,v.vkorg_recla AS org_ventas            --Organización de ventas
    ,v.vtweg_recla AS canal 				--Canal de distribución
    ,n.vtext as canal_desc
    ,d.clustert as cluster_operacional
    ,flag_recla_2 as flag_recla_2
    ,(v.werks_recla + '-' + v.matnr  + '-' + v.vkorg_recla + '-' + to_char(fecha, 'YYYYMMDD')) as id_venta 
    ,(to_char(fecha, 'YYYY-MM')  + '-' +  v.matnr) as id_producto
    ,(to_char(fecha, 'YYYYMMDD')) as id_tiempo 
    ,sum(v.kzwi3*100) as venta_neta
    ,sum(v.wavwr*100) as costo
    ,sum(v.kzwi3*100 - v.wavwr*100) as contribucion 
    ,sum(v.fkimg) as unidades 
    ,isNull(sum(v.kzwi3*100)/nullif((sum(v.fkimg)), 0), 0) as precio_unitario
    ,c.pais_id as pais
    ,v.hwaer as moneda
FROM
    prod.fct_2lis_13_vditm_reclasificado v
INNER JOIN prod.dim_centro c
				ON c.centro_id_sap=v.werks_recla 
				AND c.pais_id='CL'
LEFT JOIN prod.dim_m_cluster d ON d.werks = v.werks_recla
LEFT JOIN prod.dim_m_tvtwt n ON n.vtweg = v.vtweg_recla
GROUP BY 1,2,3,4,5,6,7,8,9,18,19
       ;

UPDATE dev.fct_ventas_dev SET centro_tipo='Tienda' where centro_id_sap like 'T%' and centro_id_sap<>'T001';

--dimension de precios
SELECT * FROM "ci_db"."dev"."dim_precios";
--SELECT cast(inicio_validez_reg_condicion as date), cast(fin_validez_reg_condicion as date) FROM "ci_db"."dev"."dim_precios";

ALTER TABLE dev.dim_precios ADD COLUMN new_column date;
UPDATE dev.dim_precios SET new_column = cast(inicio_validez_reg_condicion as date);

ALTER TABLE dev.dim_precios ADD COLUMN new_column2 date;
UPDATE dev.dim_precios SET new_column2 = cast(fin_validez_reg_condicion as date);


--ALTER TABLE dev.dim_precios DROP COLUMN new_column;
--ALTER TABLE dev.dim_precios DROP COLUMN new_column2;


ALTER TABLE dev.dim_precios DROP COLUMN inicio_validez_reg_condicion;
ALTER TABLE dev.dim_precios RENAME COLUMN new_column TO inicio_validez_reg_condicion;
ALTER TABLE dev.dim_precios DROP COLUMN fin_validez_reg_condicion;
ALTER TABLE dev.dim_precios RENAME COLUMN new_column2 TO fin_validez_reg_condicion;

SELECT * FROM "ci_db"."dev"."dim_precios";

-- Fin de la dimension de precios

-- Fact de precios ventas
CREATE TABLE dev.fct_precios_ventas_tmp AS
--TRUNCATE dev.fct_precios_ventas
 
WITH ventas AS (
  SELECT * 
  FROM dev.fct_ventas_dev
--  WHERE 
--  	num_material IN ('000003103562000055', '000003218916000022', '000003213245000026')
),
precios AS (
  SELECT *
  FROM dev.dim_precios
--  WHERE 
--  	num_material IN ('000003103562000055', '000003218916000022', '000003213245000026')
),
ventas_repetidas AS (
  SELECT
    v.id_venta
    ,v.id_producto
    ,v.id_tiempo
    ,v.fecha
    ,v.pais
    ,v.centro_id_sap
    ,v.centro_tipo
    ,v.num_material
    ,v.sku
    ,v.org_ventas
    ,v.canal
    ,v.canal_desc
    ,v.cluster_operacional
    ,v.flag_recla
    ,v.venta_neta
    ,v.costo
    ,v.contribucion
    ,v.unidades
    ,v.precio_unitario
  	,COALESCE(p.moneda,v.moneda) as moneda
    ,p.clase_condicion
    ,p.regular
    ,p.ajuste
    ,p.liquidacion
    ,p.r_a
    ,p.r_l
    ,p.a_l
    ,p.inicio_validez_reg_condicion
    ,p.fin_validez_reg_condicion
  	,RANK() OVER (
      PARTITION BY v.id_venta 
      ORDER BY CASE
        WHEN clase_condicion = 'liquidacion' THEN 1
        WHEN clase_condicion = 'ajuste' THEN 2
        WHEN clase_condicion = 'regular' THEN 3
        ELSE 4 END ASC,
      	p.inicio_validez_reg_condicion DESC,
        p.fin_validez_reg_condicion DESC
      
     ) AS prioridad
  FROM ventas v
  LEFT JOIN precios p
      ON p.num_material = v.num_material
      AND p.moneda = v.moneda
      AND v.fecha >= p.inicio_validez_reg_condicion
      AND v.fecha <= p.fin_validez_reg_condicion
  ORDER BY 
  num_material DESC, fecha DESC
)
    
SELECT 
	--DISTINCT ON (id_venta)-- llave única
	id_venta
    ,id_producto
    ,id_tiempo
    ,fecha
    ,pais
    ,centro_id_sap
    ,centro_tipo
    ,num_material
    ,sku
    ,org_ventas
    ,canal
    ,canal_desc
    ,cluster_operacional
    ,flag_recla
    ,venta_neta
    ,costo
    ,contribucion
    ,unidades
    ,precio_unitario  
    ,moneda
    ,COALESCE(liquidacion, ajuste, regular) as precio
    ,clase_condicion
    ,regular
    ,ajuste
    ,liquidacion
    ,r_a
    ,r_l
    ,a_l
    
FROM ventas_repetidas
WHERE prioridad = 1
ORDER BY num_material DESC, fecha DESC
;

--CREATE TABLE dev.fct_precios_ventas AS
TRUNCATE dev.fct_precios_ventas;
  
INSERT INTO dev.fct_precios_ventas(
    id_venta
    ,id_producto
    ,id_tiempo
    ,fecha
    ,pais
    ,centro_id_sap
    ,centro_tipo
    ,num_material
    ,sku
    ,org_ventas
    ,canal
    ,canal_desc
    ,cluster_operacional
    ,flag_recla
    ,venta_neta
    ,costo
    ,contribucion
    ,unidades
    ,precio_unitario  
    ,moneda
    ,precio
    ,clase_condicion
    ,regular
    ,ajuste
    ,liquidacion
    ,r_a
    ,r_l
    ,a_l
)
SELECT
    id_venta
    ,id_producto
    ,id_tiempo
    ,fecha
    ,pais
    ,centro_id_sap
    ,centro_tipo
    ,num_material
    ,sku
    ,org_ventas
    ,canal
    ,canal_desc
    ,cluster_operacional
    ,flag_recla
    ,venta_neta
    ,costo
    ,contribucion
    ,unidades
    ,precio_unitario  
    ,moneda
    ,precio
    ,clase_condicion
    ,regular
    ,ajuste
    ,liquidacion
    ,r_a
    ,r_l
    ,a_l
FROM dev.fct_precios_ventas_tmp;

DROP TABLE IF EXISTS dev.fct_precios_ventas_tmp;