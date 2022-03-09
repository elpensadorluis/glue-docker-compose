-- DROP TABLE IF EXISTS dev.fct_stock;
-- CREATE TABLE IF NOT EXISTS dev.fct_stock(
--     id_stock VARCHAR(255)
--     ,fecha_db date
--     ,id_tiempo VARCHAR(255)
--     ,centro_id_sap VARCHAR(255)
--     ,almacen VARCHAR(255)
--     ,num_material VARCHAR(255)
--     ,stock DOUBLE PRECISION
--     ,stock_transito DOUBLE PRECISION
--     ,stock_bloqueado DOUBLE PRECISION
-- )

truncate dev.fct_stock;

COPY dev.fct_stock
FROM 's3://dev-534086549449-analytics/parquet/tables/fct_stock/'
credentials 'aws_iam_role=arn:aws:iam::534086549449:role/RedshiftRoleCopyUnload'
FORMAT AS PARQUET;