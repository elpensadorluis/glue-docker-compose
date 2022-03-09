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
    f"s3://{env}-534086549449-analytics/parquet/tables/fct_ventas/"
)
df_d = (
    spark.read.format("parquet")
    .options(header="true")
    .load(f"s3://{env}-534086549449-analytics/parquet/tables/dim_precios/")
)

df_c.createOrReplaceTempView("fct_ventas_dev")
df_d.createOrReplaceTempView("dim_precios")
df_c.printSchema()
df_d.printSchema()


query = """

WITH ventas AS (
  SELECT *
  FROM fct_ventas_dev
--  WHERE
--  	num_material IN ('000003103562000055', '000003218916000022', '000003213245000026')
),
precios AS (
  SELECT *
  FROM dim_precios
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
    ,COALESCE(liquidacion, ajuste, regular, ROUND(precio_unitario, 2)) as precio
    ,clase_condicion
    ,regular
    ,ajuste
    ,liquidacion
    ,r_a
    ,r_l
    ,a_l
    ,CASE WHEN clase_condicion IS NULL THEN True ELSE False END AS precio_calculado

FROM ventas_repetidas
WHERE prioridad = 1
ORDER BY num_material DESC, fecha DESC
;
"""

df_a = spark.sql(query)
df_a.printSchema()
df_a.show(10)

print("Grabando a disco")

df_a.repartition(1).write.mode("append").parquet(
    "s3://dev-534086549449-analytics/parquet/tables/fct_precio_ventas/"
)

print("Grabado a disco")

print("Ha terminado satisfactoriamente")
