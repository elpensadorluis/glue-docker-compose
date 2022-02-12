unload ('select * from prod.fct_2lis_13_vditm_reclasificado')
to 's3://dev-534086549449-analytics/parquet/tables/fct_2lis_13_vditm_reclasificado/'
credentials 'aws_iam_role=arn:aws:iam::534086549449:role/RedshiftRoleCopyUnload'
PARQUET;

unload ('select * from dev.fct_ventas_dev')
to 's3://dev-534086549449-analytics/parquet/tables/fct_ventas/'
credentials 'aws_iam_role=arn:aws:iam::534086549449:role/RedshiftRoleCopyUnload'
PARQUET;

unload ('select * from prod.fct_2lis_13_vditm_reclasificado')
to 's3://dev-534086549449-analytics/parquet/tables/fct_2lis_13_vditm_reclasificado/'
credentials 'aws_iam_role=arn:aws:iam::534086549449:role/RedshiftRoleCopyUnload'
PARQUET;