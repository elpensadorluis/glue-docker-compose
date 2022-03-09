# integration glue job
import awswrangler as wr

# import pandas as pd
from calendar import month
from datetime import datetime
from datetime import timedelta, date
import uuid
import pytz
import requests
import json
import sys
import boto3
import base64

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as f
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.context import SparkContext
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    FloatType,
    BooleanType,
    LongType,
    TimestampType,
)
from pyspark.sql.functions import when, lit, col


import logging

logger = logging.getLogger(name="Prebuc Job")
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = (
    SparkSession.builder.appName("purple_prebuc")
    .config(
        "hive.metastore.client.factory.class",
        "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory",
    )
    .enableHiveSupport()
    .getOrCreate()
)
sqlContext = SQLContext(spark)
spark.conf.set("spark.sql.codegen.wholeStage", False)
hadoop_conf = sc._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
hadoop_conf.set("fs.s3a.canned.acl", "BucketOwnerFullControl")
hadoop_conf.set("fs.s3a.acl.default", "BucketOwnerFullControl")

try:
    event = getResolvedOptions(
        sys.argv,
        ["ENV", "YEAR", "MONTH", "DAY",],  # "dev", "prod"  # "2021"  # "12"  # "06"
    )
except:
    event = {
        "ENV": "dev",
        "YEAR": "-",
        "MONTH": "-",
        "DAY": "-",
    }

env = "prod" if event["ENV"] == "prod" else "dev"
year = event["YEAR"] if event["YEAR"] != "-" else datetime.today().strftime("%Y")
month = event["MONTH"] if event["MONTH"] != "-" else datetime.today().strftime("%m")
day = event["DAY"] if event["DAY"] != "-" else datetime.today().strftime("%d")

date = year + "-" + month + "-" + day
id_date = year + month + day

df_c = spark.read.parquet(
    f"s3://{env}-534086549449-analytics/parquet/tables/fct_2lis_13_vditm_reclasificado/"
)
df_c2 = spark.read.parquet(
    f"s3://{env}-534086549449-analytics/parquet/tables/prod.dim_centro/"
)
df_c3 = spark.read.parquet(
    f"s3://{env}-534086549449-analytics/parquet/tables/prod.dim_m_cluster/"
)
df_c4 = spark.read.parquet(
    f"s3://{env}-534086549449-analytics/parquet/tables/prod.dim_m_tvtwt/"
)


df_c.createOrReplaceTempView("prod.fct_2lis_13_vditm_reclasificado")
df_c2.createOrReplaceTempView("prod.dim_centro")
df_c3.createOrReplaceTempView("prod.dim_m_cluster")
df_c4.createOrReplaceTempView("prod.dim_m_tvtwt")
df_c.printSchema()


query = """


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
    AND v.fkart <> 'ZECL'
LEFT JOIN prod.dim_m_cluster d ON d.werks = v.werks_recla
LEFT JOIN prod.dim_m_tvtwt n ON n.vtweg = v.vtweg_recla
GROUP BY 1,2,3,4,5,6,7,8,9,18,19
       ;

"""

df_a = spark.sql(query)
df_a.printSchema()
df_a.show(10)

print("Grabando a disco")

df_a.repartition(1).write.mode("append").parquet(
    "s3://dev-534086549449-analytics/parquet/tables/fct_ventas_dev/"
)

print("Grabado a disco")

print("Ha terminado satisfactoriamente")
