CREATE MATERIALIZED VIEW prod.mv_final AS (
	---- Considerar sólo SKU's con movimientos desde la fecha de análisis (2016) 
	WITH stock_nov as (
      	select * from prod.fct_stock_final_2021 where almacen='0001' and fecha between '2021-11-01' and '2021-12-01'
      )
  	,ventas_nov as (
      	select * from prod.fct_ventas_2021 where centro_id_sap like 'T%' and fecha between '2021-11-01' and '2021-12-01'
      )
  	,parte1 AS (
		SELECT v.fecha as fecha_ventas
       ,v.centro_id_sap as centro_id_sap_ventas
       ,v.num_material as num_material_ventas
       ,v.sku as sku_ventas
       ,v.org_ventas as org_ventas_ventas
       ,v.canal as canal_ventas
       ,v.canal_desc as canal_desc_ventas
       ,v.cluster_operacional as cluster_operacional_ventas
       ,v.flag_recla as flag_recla_ventas
       ,v.id_venta as id_venta_ventas
       ,v.id_producto as id_producto_ventas
       ,v.id_tiempo as id_tiempo_ventas
       ,v.venta_neta as venta_neta_ventas
       ,v.costo as costo_ventas
       ,v.contribucion as  contribucion_ventas
       ,v.unidades as unidades_ventas
       ,v.precio_unitario as precio_unitario_ventas
       ,s.id_producto as id_producto_stock
       ,s.id_tiempo as id_tiempo_stock
      ,s.num_material as num_material_stock
      ,s.material_desc as material_desc_stock
      ,s.centro_id_sap as centro_id_sap_stock
      ,s.org_ventas as org_ventas_stock
      ,s.canal as canal_stock
      ,s.almacen as almacen_stock
      ,s.stock as stock_stock
      ,s.pendiente_entrega as pendiente_entrega_stock
      ,s.stock_transito as stock_transito_stock
      ,s.stock_disponible as stock_disponible_stock
      ,s.fecha as fecha_stock
      ,s.fuente as fuente_stock
		FROM ventas_nov v
		FULL OUTER JOIN stock_nov s ON 
      	v.centro_id_sap = s.centro_id_sap 
      	and v.num_material = s.num_material
      	and v.fecha = s.fecha    
      	and v.org_ventas=s.org_ventas
      	and v.canal=s.canal
	)
	SELECT COALESCE(fecha_ventas,fecha_stock) as fecha,*
	FROM parte1
);