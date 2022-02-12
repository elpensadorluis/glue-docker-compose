CREATE TABLE dev.fct_precios_ventas AS
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