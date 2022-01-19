-- TRUNCATE dev.tmp_tienda_almacen_delta;

COPY dev.fct_ventas_dev
FROM 's3://dev-534086549449-analytics/parquet/tables/fct_ventas/'
credentials 'aws_iam_role=arn:aws:iam::534086549449:role/RedshiftRoleCopyUnload'
FORMAT AS PARQUET;