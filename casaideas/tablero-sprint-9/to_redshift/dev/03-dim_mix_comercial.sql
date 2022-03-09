truncate dev.dim_mix_comercial;

COPY dev.dim_mix_comercial
FROM 's3://dev-534086549449-analytics/parquet/tables/dim_mix_comercial/'
credentials 'aws_iam_role=arn:aws:iam::534086549449:role/RedshiftRoleCopyUnload'
FORMAT AS PARQUET;