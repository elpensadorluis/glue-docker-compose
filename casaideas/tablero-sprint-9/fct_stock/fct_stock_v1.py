# integration glue job
import awswrangler as wr

import pandas as pd

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
spark.conf.set("spark.sql.execution.arrow.enabled", "true")
hadoop_conf = sc._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
hadoop_conf.set("fs.s3a.canned.acl", "BucketOwnerFullControl")
# hadoop_conf.set("fs.s3a.secret.key", "foobarfoo")
# hadoop_conf.set("fs.s3a.access.key", "foobar")
# hadoop_conf.set("fs.s3a.endpoint", "http://s3:9000")
hadoop_conf.set("fs.s3a.acl.default", "BucketOwnerFullControl")

### Inicializacion de variables ###


### Tomando variables de entorno ###
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

df_c = (
    spark.read.format("parquet")
    .options(header="true")
    .load(
        f"s3://{env}-534086549449-data-lake/parquet/sap/tables/marc_transito/year={year}/month={month}/day={day}/"
    )
)

df_d = (
    spark.read.format("parquet")
    .options(header="true")
    .load(
        f"s3://{env}-534086549449-data-lake/parquet/sap/tables/mard/year={year}/month={month}/day={day}/"
    )
)
df_c = df_c.withColumn("fecha_db", lit(date)).withColumn("id_tiempo", lit(id_date))
df_d = df_d.withColumn("fecha_db", lit(date)).withColumn("id_tiempo", lit(id_date))

df_c = df_c.withColumn("fecha_db", f.to_date(df_c.fecha_db, "yyyy-MM-dd"))
df_d = df_d.withColumn("fecha_db", f.to_date(df_d.fecha_db, "yyyy-MM-dd"))

print("Creando tablas ")
df_c.createOrReplaceTempView("marc")
df_d.createOrReplaceTempView("mard")
df_c.printSchema()
df_d.printSchema()

query = f"""
    SELECT
        fecha_db,
        id_tiempo,
        werks as centro_id_sap,
        '0001' as almacen,
        matnr as num_material,
        SUM(trame) as stock_transito
    FROM marc
    GROUP BY 1,2,3,4,5
"""
df_stock_c = spark.sql(query)
df_stock_c.createOrReplaceTempView("stock_c")
print("Creado tabla stock_c")

query = f"""
    SELECT
        fecha_db,
        id_tiempo,
        centro as centro_id_sap,
        almacen,
        num_material,
        SUM(stock_libre_utilizacion) as stock,
        SUM(stock_bloqueado) as stock_bloqueado
    FROM mard
    GROUP BY 1,2,3,4,5
"""

df_stock_d = spark.sql(query)
df_stock_d = df_stock_d.withColumn(
    "id_stock",
    f.concat(
        df_stock_d.id_tiempo,
        lit("-"),
        df_stock_d.num_material,
        lit("-"),
        df_stock_d.centro_id_sap,
        lit("-"),
        df_stock_d.almacen,
    ),
)

df_stock_d.createOrReplaceTempView("stock_d")
print("Creado tabla stock_d")

query = f"""
    SELECT
        d.id_stock,
        d.fecha_db,
        d.id_tiempo,
        d.centro_id_sap,
        d.almacen,
        d.num_material,
        d.stock,
        c.stock_transito,
        d.stock_bloqueado
    FROM stock_d d
    LEFT JOIN stock_c c
        ON d.id_tiempo = c.id_tiempo
        AND d.centro_id_sap = c.centro_id_sap
        AND d.almacen = c.almacen
        AND d.num_material = c.num_material
    ORDER BY fecha_db DESC, centro_id_sap ASC, almacen ASC, num_material ASC
"""

df_a = spark.sql(query)
df_a.printSchema()
df_a.show(10)

print("Grabando a disco")

df_a.repartition(1).write.mode("append").parquet(
    "s3://dev-534086549449-analytics/parquet/tables/fct_stock/"
)

print("Grabado a disco")

print("Ha terminado satisfactoriamente")
