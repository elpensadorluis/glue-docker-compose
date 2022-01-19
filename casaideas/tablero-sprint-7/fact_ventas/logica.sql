--create table dev.fct_ventas_dev (
--id_venta VARCHAR,
--id_producto VARCHAR(34),
--id_tiempo VARCHAR(16),
--centro_id_sap VARCHAR(6),
--centro_id_sap_2 VARCHAR(6),
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
--fecha DATE
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
FROM
    prod.fct_2lis_13_vditm_reclasificado v
INNER JOIN prod.dim_centro 
				ON centro_id_sap=v.werks_recla 
				AND pais_id='CL'
LEFT JOIN prod.dim_m_cluster d ON d.werks = v.werks_recla
LEFT JOIN prod.dim_m_tvtwt n ON n.vtweg = v.vtweg_recla
GROUP BY 1,2,3,4,5,6,7,8,9
       ;

UPDATE dev.fct_ventas_dev SET centro_tipo='Tienda' where centro_id_sap like 'T%' and centro_id_sap<>'T001';