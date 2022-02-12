# integration glue job
# import awswrangler as wr
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

# date = datetime.today().strftime("%Y-%m-%d")
# id_date = datetime.today().strftime("%Y%m%d")

# year = "2022"
# month = "02"
# day = "04"

# date = year + "-" + month + "-" + day
# id_date = year + month + day

df_new_schema = json.loads(SCHEMA)

### LECTURA DE PURPLE
### TODO: PARAMETRIZAR PARA PODER LEER POR DIA 04###
year = "2022"
month = "02"
day = "04"

date = year + "-" + month + "-" + day
id_date = year + month + day

df_4 = spark.read.parquet(
    f"s3://dev-534086549449-data-lake/parquet/sap/tables/tracking/year={year}/month={month}/day={day}/"
)
# dfp.show(50, truncate=False)

### limpiar espacios en blanco en la columna posicion_compra ###
df_4 = df_4.withColumn(
    "posicion_compras", f.regexp_replace("posicion_compras", " ", "")
)

### agregar un cero para tener dos digitos a columna posicion_compras ###
df_4 = df_4.withColumn("posicion_compras", f.lpad(col("posicion_compras"), 2, "0"))

### CAMBIO DE FORMATO DE DATOS DE FECHA ###
df_4 = df_4.withColumn("etd_cierre", f.to_date(df_4.etd_cierre, "dd.MM.yyyy"))
df_4 = df_4.withColumn("etd_negociable", f.to_date(df_4.etd_negociable, "dd.MM.yyyy"))
df_4 = df_4.withColumn("etd_informada", f.to_date(df_4.etd_informada, "dd.MM.yyyy"))
df_4 = df_4.withColumn("eta_informada", f.to_date(df_4.eta_informada, "dd.MM.yyyy"))
df_4 = df_4.withColumn("eta_cierre", f.to_date(df_4.eta_cierre, "dd.MM.yyyy"))
df_4 = df_4.withColumn(
    "fecha_pago_anticipo", f.to_date(df_4.fecha_pago_anticipo, "dd.MM.yyyy")
)
df_4 = df_4.withColumn(
    "fecha_pago_saldo", f.to_date(df_4.fecha_pago_saldo, "dd.MM.yyyy")
)
df_4 = df_4.withColumn("fecha_doc", f.to_date(df_4.fecha_doc, "dd.MM.yyyy"))

### CAMBIOS DE FORMATO DE DATOS DE NUMEROS ###
df_4 = df_4.withColumn("fecha_db", lit(date)).withColumn("id_tiempo", lit(id_date))

# eliminar columna texto_breve_material
df_4 = df_4.drop("texto_breve_material")

# crear columna id_tracking con valor fecha_db + documento_compra + posicion_compras
df_4 = df_4.withColumn(
    "id_tracking",
    f.concat(
        df_4.id_tiempo, lit("-"), df_4.documento_compra, lit("-"), df_4.posicion_compras
    ),
)

### TODO FIN

### TODO: PARAMETRIZAR PARA PODER LEER POR DIA 05###
year = "2022"
month = "02"
day = "05"

date = year + "-" + month + "-" + day
id_date = year + month + day
dfq = spark.read.parquet(
    f"s3://dev-534086549449-data-lake/parquet/sap/tables/tracking/year={year}/month={month}/day={day}/"
)
# dfq.show(50, truncate=False)

### limpiar espacios en blanco en la columna posicion_compra ###
dfq = dfq.withColumn("posicion_compras", f.regexp_replace("posicion_compras", " ", ""))

### agregar un cero para tener dos digitos a columna posicion_compras ###
dfq = dfq.withColumn("posicion_compras", f.lpad(col("posicion_compras"), 2, "0"))

### CAMBIO DE FORMATO DE DATOS DE FECHA ###
dfq = dfq.withColumn("etd_cierre", f.to_date(dfq.etd_cierre, "dd.MM.yyyy"))
dfq = dfq.withColumn("etd_negociable", f.to_date(dfq.etd_negociable, "dd.MM.yyyy"))
dfq = dfq.withColumn("etd_informada", f.to_date(dfq.etd_informada, "dd.MM.yyyy"))
dfq = dfq.withColumn("eta_informada", f.to_date(dfq.eta_informada, "dd.MM.yyyy"))
dfq = dfq.withColumn("eta_cierre", f.to_date(dfq.eta_cierre, "dd.MM.yyyy"))
dfq = dfq.withColumn(
    "fecha_pago_anticipo", f.to_date(dfq.fecha_pago_anticipo, "dd.MM.yyyy")
)
dfq = dfq.withColumn("fecha_pago_saldo", f.to_date(dfq.fecha_pago_saldo, "dd.MM.yyyy"))
dfq = dfq.withColumn("fecha_doc", f.to_date(dfq.fecha_doc, "dd.MM.yyyy"))

### CAMBIOS DE FORMATO DE DATOS DE NUMEROS ###
dfq = dfq.withColumn("fecha_db", lit(date)).withColumn("id_tiempo", lit(id_date))

# eliminar columna texto_breve_material
dfq = dfq.drop("texto_breve_material")

# crear columna id_tracking con valor fecha_db + documento_compra + posicion_compras
dfq = dfq.withColumn(
    "id_tracking",
    f.concat(
        dfq.id_tiempo, lit("-"), dfq.documento_compra, lit("-"), dfq.posicion_compras
    ),
)

### TODO FIN

### TODO: PARAMETRIZAR PARA PODER LEER POR DIA 06###
year = "2022"
month = "02"
day = "06"

date = year + "-" + month + "-" + day
id_date = year + month + day
dfr = spark.read.parquet(
    f"s3://dev-534086549449-data-lake/parquet/sap/tables/tracking/year={year}/month={month}/day={day}/"
)
# dfr.show(50, truncate=False)

### limpiar espacios en blanco en la columna posicion_compra ###
dfr = dfr.withColumn("posicion_compras", f.regexp_replace("posicion_compras", " ", ""))

### agregar un cero para tener dos digitos a columna posicion_compras ###
dfr = dfr.withColumn("posicion_compras", f.lpad(col("posicion_compras"), 2, "0"))

### CAMBIO DE FORMATO DE DATOS DE FECHA ###
dfr = dfr.withColumn("etd_cierre", f.to_date(dfr.etd_cierre, "dd.MM.yyyy"))
dfr = dfr.withColumn("etd_negociable", f.to_date(dfr.etd_negociable, "dd.MM.yyyy"))
dfr = dfr.withColumn("etd_informada", f.to_date(dfr.etd_informada, "dd.MM.yyyy"))
dfr = dfr.withColumn("eta_informada", f.to_date(dfr.eta_informada, "dd.MM.yyyy"))
dfr = dfr.withColumn("eta_cierre", f.to_date(dfr.eta_cierre, "dd.MM.yyyy"))
dfr = dfr.withColumn(
    "fecha_pago_anticipo", f.to_date(dfr.fecha_pago_anticipo, "dd.MM.yyyy")
)
dfr = dfr.withColumn("fecha_pago_saldo", f.to_date(dfr.fecha_pago_saldo, "dd.MM.yyyy"))
dfr = dfr.withColumn("fecha_doc", f.to_date(dfr.fecha_doc, "dd.MM.yyyy"))

### CAMBIOS DE FORMATO DE DATOS DE NUMEROS ###
dfr = dfr.withColumn("fecha_db", lit(date)).withColumn("id_tiempo", lit(id_date))

# eliminar columna texto_breve_material
dfr = dfr.drop("texto_breve_material")

# crear columna id_tracking con valor fecha_db + documento_compra + posicion_compras
dfr = dfr.withColumn(
    "id_tracking",
    f.concat(
        dfr.id_tiempo, lit("-"), dfr.documento_compra, lit("-"), dfr.posicion_compras
    ),
)

### TODO FIN

### TODO: PARAMETRIZAR PARA PODER LEER POR DIA 07###
year = "2022"
month = "02"
day = "07"

date = year + "-" + month + "-" + day
id_date = year + month + day
dfs = spark.read.parquet(
    f"s3://dev-534086549449-data-lake/parquet/sap/tables/tracking/year={year}/month={month}/day={day}/"
)
# dfs.show(50, truncate=False)

### limpiar espacios en blanco en la columna posicion_compra ###
dfs = dfs.withColumn("posicion_compras", f.regexp_replace("posicion_compras", " ", ""))

### agregar un cero para tener dos digitos a columna posicion_compras ###
dfs = dfs.withColumn("posicion_compras", f.lpad(col("posicion_compras"), 2, "0"))

### CAMBIO DE FORMATO DE DATOS DE FECHA ###
dfs = dfs.withColumn("etd_cierre", f.to_date(dfs.etd_cierre, "dd.MM.yyyy"))
dfs = dfs.withColumn("etd_negociable", f.to_date(dfs.etd_negociable, "dd.MM.yyyy"))
dfs = dfs.withColumn("etd_informada", f.to_date(dfs.etd_informada, "dd.MM.yyyy"))
dfs = dfs.withColumn("eta_informada", f.to_date(dfs.eta_informada, "dd.MM.yyyy"))
dfs = dfs.withColumn("eta_cierre", f.to_date(dfs.eta_cierre, "dd.MM.yyyy"))
dfs = dfs.withColumn(
    "fecha_pago_anticipo", f.to_date(dfs.fecha_pago_anticipo, "dd.MM.yyyy")
)
dfs = dfs.withColumn("fecha_pago_saldo", f.to_date(dfs.fecha_pago_saldo, "dd.MM.yyyy"))
dfs = dfs.withColumn("fecha_doc", f.to_date(dfs.fecha_doc, "dd.MM.yyyy"))

### CAMBIOS DE FORMATO DE DATOS DE NUMEROS ###
dfs = dfs.withColumn("fecha_db", lit(date)).withColumn("id_tiempo", lit(id_date))

# eliminar columna texto_breve_material
dfs = dfs.drop("texto_breve_material")

# crear columna id_tracking con valor fecha_db + documento_compra + posicion_compras
dfs = dfs.withColumn(
    "id_tracking",
    f.concat(
        dfs.id_tiempo, lit("-"), dfs.documento_compra, lit("-"), dfs.posicion_compras
    ),
)

### TODO FIN

### TODO: PARAMETRIZAR PARA PODER LEER POR DIA 08###
year = "2022"
month = "02"
day = "08"

date = year + "-" + month + "-" + day
id_date = year + month + day
dft = spark.read.parquet(
    f"s3://dev-534086549449-data-lake/parquet/sap/tables/tracking/year={year}/month={month}/day={day}/"
)
# dft.show(50, truncate=False)

### limpiar espacios en blanco en la columna posicion_compra ###
dft = dft.withColumn("posicion_compras", f.regexp_replace("posicion_compras", " ", ""))

### agregar un cero para tener dos digitos a columna posicion_compras ###
dft = dft.withColumn("posicion_compras", f.lpad(col("posicion_compras"), 2, "0"))

### CAMBIO DE FORMATO DE DATOS DE FECHA ###
dft = dft.withColumn("etd_cierre", f.to_date(dft.etd_cierre, "dd.MM.yyyy"))
dft = dft.withColumn("etd_negociable", f.to_date(dft.etd_negociable, "dd.MM.yyyy"))
dft = dft.withColumn("etd_informada", f.to_date(dft.etd_informada, "dd.MM.yyyy"))
dft = dft.withColumn("eta_informada", f.to_date(dft.eta_informada, "dd.MM.yyyy"))
dft = dft.withColumn("eta_cierre", f.to_date(dft.eta_cierre, "dd.MM.yyyy"))
dft = dft.withColumn(
    "fecha_pago_anticipo", f.to_date(dft.fecha_pago_anticipo, "dd.MM.yyyy")
)
dft = dft.withColumn("fecha_pago_saldo", f.to_date(dft.fecha_pago_saldo, "dd.MM.yyyy"))
dft = dft.withColumn("fecha_doc", f.to_date(dft.fecha_doc, "dd.MM.yyyy"))

### CAMBIOS DE FORMATO DE DATOS DE NUMEROS ###
dft = dft.withColumn("fecha_db", lit(date)).withColumn("id_tiempo", lit(id_date))

# eliminar columna texto_breve_material
dft = dft.drop("texto_breve_material")

# crear columna id_tracking con valor fecha_db + documento_compra + posicion_compras
dft = dft.withColumn(
    "id_tracking",
    f.concat(
        dft.id_tiempo, lit("-"), dft.documento_compra, lit("-"), dft.posicion_compras
    ),
)

### TODO FIN

### TODO: PARAMETRIZAR PARA PODER LEER POR DIA 09###
year = "2022"
month = "02"
day = "09"

date = year + "-" + month + "-" + day
id_date = year + month + day
dft = spark.read.parquet(
    f"s3://dev-534086549449-data-lake/parquet/sap/tables/tracking/year={year}/month={month}/day={day}/"
)
# dft.show(50, truncate=False)

### limpiar espacios en blanco en la columna posicion_compra ###
dft = dft.withColumn("posicion_compras", f.regexp_replace("posicion_compras", " ", ""))

### agregar un cero para tener dos digitos a columna posicion_compras ###
dft = dft.withColumn("posicion_compras", f.lpad(col("posicion_compras"), 2, "0"))

### CAMBIO DE FORMATO DE DATOS DE FECHA ###
dft = dft.withColumn("etd_cierre", f.to_date(dft.etd_cierre, "dd.MM.yyyy"))
dft = dft.withColumn("etd_negociable", f.to_date(dft.etd_negociable, "dd.MM.yyyy"))
dft = dft.withColumn("etd_informada", f.to_date(dft.etd_informada, "dd.MM.yyyy"))
dft = dft.withColumn("eta_informada", f.to_date(dft.eta_informada, "dd.MM.yyyy"))
dft = dft.withColumn("eta_cierre", f.to_date(dft.eta_cierre, "dd.MM.yyyy"))
dft = dft.withColumn(
    "fecha_pago_anticipo", f.to_date(dft.fecha_pago_anticipo, "dd.MM.yyyy")
)
dft = dft.withColumn("fecha_pago_saldo", f.to_date(dft.fecha_pago_saldo, "dd.MM.yyyy"))
dft = dft.withColumn("fecha_doc", f.to_date(dft.fecha_doc, "dd.MM.yyyy"))

### CAMBIOS DE FORMATO DE DATOS DE NUMEROS ###
dft = dft.withColumn("fecha_db", lit(date)).withColumn("id_tiempo", lit(id_date))

# eliminar columna texto_breve_material
dft = dft.drop("texto_breve_material")

# crear columna id_tracking con valor fecha_db + documento_compra + posicion_compras
dft = dft.withColumn(
    "id_tracking",
    f.concat(
        dft.id_tiempo, lit("-"), dft.documento_compra, lit("-"), dft.posicion_compras
    ),
)

### TODO FIN

### TODO: PARAMETRIZAR PARA PODER LEER POR DIA 10###
year = "2022"
month = "02"
day = "10"

date = year + "-" + month + "-" + day
id_date = year + month + day
dft = spark.read.parquet(
    f"s3://dev-534086549449-data-lake/parquet/sap/tables/tracking/year={year}/month={month}/day={day}/"
)
# dft.show(50, truncate=False)

### limpiar espacios en blanco en la columna posicion_compra ###
dft = dft.withColumn("posicion_compras", f.regexp_replace("posicion_compras", " ", ""))

### agregar un cero para tener dos digitos a columna posicion_compras ###
dft = dft.withColumn("posicion_compras", f.lpad(col("posicion_compras"), 2, "0"))

### CAMBIO DE FORMATO DE DATOS DE FECHA ###
dft = dft.withColumn("etd_cierre", f.to_date(dft.etd_cierre, "dd.MM.yyyy"))
dft = dft.withColumn("etd_negociable", f.to_date(dft.etd_negociable, "dd.MM.yyyy"))
dft = dft.withColumn("etd_informada", f.to_date(dft.etd_informada, "dd.MM.yyyy"))
dft = dft.withColumn("eta_informada", f.to_date(dft.eta_informada, "dd.MM.yyyy"))
dft = dft.withColumn("eta_cierre", f.to_date(dft.eta_cierre, "dd.MM.yyyy"))
dft = dft.withColumn(
    "fecha_pago_anticipo", f.to_date(dft.fecha_pago_anticipo, "dd.MM.yyyy")
)
dft = dft.withColumn("fecha_pago_saldo", f.to_date(dft.fecha_pago_saldo, "dd.MM.yyyy"))
dft = dft.withColumn("fecha_doc", f.to_date(dft.fecha_doc, "dd.MM.yyyy"))

### CAMBIOS DE FORMATO DE DATOS DE NUMEROS ###
dft = dft.withColumn("fecha_db", lit(date)).withColumn("id_tiempo", lit(id_date))

# eliminar columna texto_breve_material
dft = dft.drop("texto_breve_material")

# crear columna id_tracking con valor fecha_db + documento_compra + posicion_compras
dft = dft.withColumn(
    "id_tracking",
    f.concat(
        dft.id_tiempo, lit("-"), dft.documento_compra, lit("-"), dft.posicion_compras
    ),
)

### TODO FIN

df = dfp.unionByName(dfq)
df = df.unionByName(dfr)
df = df.unionByName(dfs)
df = df.unionByName(dft)

df.createOrReplaceTempView("tracking")
df.printSchema()
df.show(50, truncate=False)

df.repartition(1).write.mode("overwrite").parquet(
    "s3://dev-534086549449-analytics/parquet/tables/fct_tracking/"
)

print("Escritura en parquet finalizada")

