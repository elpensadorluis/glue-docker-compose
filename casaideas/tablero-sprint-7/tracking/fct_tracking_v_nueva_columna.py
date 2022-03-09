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


### DEFINICION DE VARIABLES ###
SCHEMA = '{"documento_compra":"string","posicion_compras":"string","num_material":"string","texto_breve_material":"string","cantidad":"double","cantidad_por_entregar":"double","porc_pendiente_ingreso":"double","estado":"string","texto_estado":"string","proveedor":"string","nombre_proveedor":"string","cpag":"string","email":"string","temporada":"string","texto_temporada":"string","indicativo_temporada":"string","precio":"double","precio_pendiente":"double","lib":"double","por":"double","moneda":"string","organización_compras":"string","centro_id_sap":"string","almacen":"string","puerto_embarque":"string","pais_destino":"string","pais_adquisicion":"string","comprador":"string","texto_responsable":"string","pi_":"string","naviera":"string","nave_1":"string","numero_bl":"string","etd_cierre":"date","etd_negociable":"date","etd_informada":"date","eta_informada":"date","eta_cierre":"date","anticipo":"string","saldo":"string","fecha_pago_anticipo":"date","fecha_pago_saldo":"date","doc_inv_pl":"string","inner_":"double","master":"double","fecha_doc":"date","texto_po":"string","numero_lc":"string","efi":"string","year":"integer","month":"integer","day":"integer"}'


df_new_schema = json.loads(SCHEMA)

### LECTURA DE PURPLE
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

df_t = spark.read.parquet(
    f"s3://dev-534086549449-data-lake/parquet/sap/tables/tracking/year={year}/month={month}/day={day}/"
)
# dfp.show(50, truncate=False)

### limpiar espacios en blanco en la columna posicion_compra ###
df_t = df_t.withColumn(
    "posicion_compras", f.regexp_replace("posicion_compras", " ", "")
)

### agregar un cero para tener dos digitos a columna posicion_compras ###
df_t = df_t.withColumn("posicion_compras", f.lpad(col("posicion_compras"), 2, "0"))

### CAMBIO DE FORMATO DE DATOS DE FECHA ###
df_t = df_t.withColumn("etd_cierre", f.to_date(df_t.etd_cierre, "dd.MM.yyyy"))
df_t = df_t.withColumn("etd_negociable", f.to_date(df_t.etd_negociable, "dd.MM.yyyy"))
df_t = df_t.withColumn("etd_informada", f.to_date(df_t.etd_informada, "dd.MM.yyyy"))
df_t = df_t.withColumn("eta_informada", f.to_date(df_t.eta_informada, "dd.MM.yyyy"))
df_t = df_t.withColumn("eta_cierre", f.to_date(df_t.eta_cierre, "dd.MM.yyyy"))
df_t = df_t.withColumn(
    "fecha_pago_anticipo", f.to_date(df_t.fecha_pago_anticipo, "dd.MM.yyyy")
)
df_t = df_t.withColumn(
    "fecha_pago_saldo", f.to_date(df_t.fecha_pago_saldo, "dd.MM.yyyy")
)
df_t = df_t.withColumn("fecha_doc", f.to_date(df_t.fecha_doc, "dd.MM.yyyy"))
# fecha_ret_a, fecha_des_cd
# TODO: CAMBIAR FORMATO DE FECHA
if not "fecha_ingreso_cd" in df_t.columns:
    df_t = df_t.withColumn("fecha_ingreso_cd", f.lit(""))
    df_t = df_t.withColumn(
        "fecha_ingreso_cd", f.to_date(df_t.fecha_ingreso_cd, "dd.MM.yyyy")
    )
else:
    df_t = df_t.withColumn(
        "fecha_ingreso_cd", f.to_date(df_t.fecha_ingreso_cd, "dd.MM.yyyy")
    )

if not "fecha_ret_a" in df_t.columns:
    df_t = df_t.withColumn("fecha_ret_a", f.lit(""))
    df_t = df_t.withColumn("fecha_ret_a", f.to_date(df_t.fecha_ret_a, "dd.MM.yyyy"))
else:
    df_t = df_t.withColumn("fecha_ret_a", f.to_date(df_t.fecha_ret_a, "dd.MM.yyyy"))

if not "fecha_des_cd" in df_t.columns:
    df_t = df_t.withColumn("fecha_des_cd", f.lit(""))
    df_t = df_t.withColumn("fecha_des_cd", f.to_date(df_t.fecha_des_cd, "dd.MM.yyyy"))
else:
    df_t = df_t.withColumn("fecha_des_cd", f.to_date(df_t.fecha_des_cd, "dd.MM.yyyy"))
### CAMBIOS DE FORMATO DE DATOS DE NUMEROS ###
df_t = df_t.withColumn("fecha_db", lit(date)).withColumn("id_tiempo", lit(id_date))

# eliminar columna texto_breve_material fecha_ingreso_cd
df_t = df_t.drop("texto_breve_material")

# crear columna id_tracking con valor fecha_db + documento_compra + posicion_compras
df = df_t.withColumn(
    "id_tracking",
    f.concat(
        df_t.id_tiempo, lit("-"), df_t.documento_compra, lit("-"), df_t.posicion_compras
    ),
)

# eliminar columna

### TODO FIN


df.createOrReplaceTempView("tracking")
df.printSchema()
df.show(50, truncate=False)

df.repartition(1).write.mode("append").parquet(
    "s3://dev-534086549449-analytics/parquet/tables/fct_tracking/"
)

print("Escritura en parquet finalizada")

print("Ha terminado satisfactoriamente")

