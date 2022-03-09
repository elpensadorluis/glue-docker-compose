-- DROP TABLE IF EXISTS dev.fct_tracking;
-- CREATE TABLE IF NOT EXISTS dev.fct_tracking(
--     documento_compra VARCHAR(255)
--     ,posicion_compras VARCHAR(255)
--     ,num_material VARCHAR(255)
--     ,cantidad VARCHAR(255)
--     ,cantidad_por_entregar VARCHAR(255)
--     ,porc_pendiente_ingreso VARCHAR(255)
--     ,estado VARCHAR(255)
--     ,texto_estado VARCHAR(255)
--     ,proveedor VARCHAR(255)
--     ,nombre_proveedor VARCHAR(255)
--     ,cpag VARCHAR(255)
--     ,email VARCHAR(255)
--     ,temporada VARCHAR(255)
--     ,texto_temporada VARCHAR(255)
--     ,indicativo_temporada VARCHAR(255)
--     ,precio VARCHAR(255)
--     ,precio_pendiente VARCHAR(255)
--     ,lib VARCHAR(255)
--     ,por VARCHAR(255)
--     ,moneda VARCHAR(255)
--     ,organización_compras VARCHAR(255)
--     ,centro_id_sap VARCHAR(255)
--     ,almacen VARCHAR(255)
--     ,puerto_embarque VARCHAR(255)
--     ,pais_destino VARCHAR(255)
--     ,pais_adquisicion VARCHAR(255)
--     ,comprador VARCHAR(255)
--     ,texto_responsable VARCHAR(255)
--     ,pi_ VARCHAR(255)
--     ,naviera VARCHAR(255)
--     ,nave_1 VARCHAR(255)
--     ,numero_bl VARCHAR(255)
--     ,etd_cierre date
--     ,etd_negociable date
--     ,etd_informada date
--     ,eta_informada date
--     ,eta_cierre date
--     ,anticipo VARCHAR(255)
--     ,saldo VARCHAR(255)
--     ,fecha_pago_anticipo date
--     ,fecha_pago_saldo date
--     ,doc_inv_pl VARCHAR(255)
--     ,inner_ VARCHAR(255)
--     ,master VARCHAR(255)
--     ,fecha_doc date
--     ,texto_po VARCHAR(255)
--     ,numero_lc VARCHAR(255)
--     ,efi VARCHAR(255)
--     ,fecha_db VARCHAR(255)
--     ,id_tiempo VARCHAR(255)
--     ,id_tracking VARCHAR(255)
-- )

truncate dev.fct_tracking;

COPY dev.fct_tracking
FROM 's3://dev-534086549449-analytics/parquet/tables/fct_tracking/'
credentials 'aws_iam_role=arn:aws:iam::534086549449:role/RedshiftRoleCopyUnload'
FORMAT AS PARQUET;