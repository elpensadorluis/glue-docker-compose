import json
import logging
import boto3
import argparse
import awswrangler as wr
import pandas as pd
import sys
from datetime import datetime
from boto3.dynamodb.conditions import Key

############
import pyspark
import pyspark.sql as sql
from pyspark.sql import functions as f
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, lit, col
from pyspark.sql.functions import current_date
from pyspark.sql.types import *
from pyspark.sql.functions import *

############
from awsglue.dynamicframe import DynamicFrame
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

logger = logging.getLogger()
logger = logging.getLogger(name="Transversal Job Starting")
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)

# --- Inicializacion configuraciones de Spark ---
try:
    args = getResolvedOptions(sys.argv, ["JOB_NAME"])
except:
    # print("Error: no se pudo obtener los argumentos")
    args = {
        "JOB_NAME": "ficha_corta",
    }
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

conf = pyspark.SparkConf()


date = datetime.today().strftime("%Y-%m-%d")

# dfp=spark.read.schema(Schema).parquet("s3://dev-534086549449-data-raw/csv/tables/dim_producto_hist/")
print("Leyendo tabla dim_producto_hist desde csv")
dfp = (
    spark.read.option("header", True)
    .option("encoding", "ISO-8859-1")
    .csv("s3://dev-534086549449-data-raw/csv/tables/dim_producto_hist/")
)

dfp.createOrReplaceTempView("dim_producto_hist")

dfp.show(50, truncate=False)
# dfp.describe().show()
dfp.printSchema()

print("Carga lista. Cambiando nombre de columnas: ")

dfc = (
    dfp.withColumnRenamed("Tiempo_Feria.tiempo_agno_mes", "fecha")
    .withColumnRenamed("Mes", "mes")
    .withColumnRenamed("Clave_Temporada", "clave_temporada")
    .withColumnRenamed("Agno_Mes", "tiempo_ano_mes")
    .withColumnRenamed("AGNO", "ano")
    .withColumnRenamed("Temporada", "temporada")
    .withColumnRenamed("STATUS.COMPRA", "status_compra")
    .withColumnRenamed("SKU", "sku")
    .withColumnRenamed("DESCRIPCION.SKU", "sku_desc")
    .withColumnRenamed("MADRE", "id_madre")
    .withColumnRenamed("DESCRIPCION.MADRE", "madre_desc")
    .withColumnRenamed("NIVEL.1", "nivel_1")
    .withColumnRenamed("NIVEL.2", "nivel_2")
    .withColumnRenamed("NIVEL.3", "nivel_3")
    .withColumnRenamed("NIVEL.4", "nivel_4")
    .withColumnRenamed("GRUPO.ANALISIS", "grupo_analisis")
    .withColumnRenamed("FERIA.ID", "id_feria")
    .withColumnRenamed("FOB", "fob")
    .withColumnRenamed("TIPO.MONEDA", "tipo_moneda")
    .withColumnRenamed("PP", "pp")
    .withColumnRenamed("MOQ", "moq")
    .withColumnRenamed("TIPO.MOQ", "tipo_moq")
    .withColumnRenamed("COND.MOQ", "cond_moq")
    .withColumnRenamed("DISPLAY", "display")
    .withColumnRenamed("PAIS.ORIGEN", "pais_origen")
    .withColumnRenamed("LEADTIME", "leadtime")
    .withColumnRenamed("VOLUMEN", "volumen")
    .withColumnRenamed("PESO", "peso")
    .withColumnRenamed("INNER", "inner")
    .withColumnRenamed("MASTER", "master")
    .withColumnRenamed("DURACION", "duracion")
    .withColumnRenamed("FECHA.INICIO", "fecha_ini")
    .withColumnRenamed("FECHA.TERMINO", "fecha_ter")
    .withColumnRenamed("FECHA.INICIO.2", "fecha_ini_2")
    .withColumnRenamed("FECHA.TERMINO.2", "fecha_ter_2")
    .withColumnRenamed("num_material", "num_material")
    .withColumnRenamed("Clave", "id_producto")
)

print("Cambio de nombre de columnas realizadas")
dfc.createOrReplaceTempView("dim_producto_hist_col")
dfc.show(5, truncate=False)
dfc.describe().show()

dff = dfc.filter(dfc.fecha.startswith("2"))
dff.show(5, truncate=False)
dff.describe().show()
# dfc.show(50, truncate=False)
# dfc.describe().show()
dff.createOrReplaceTempView("dim_producto_hist_pd")
dff.printSchema()

dff.repartition(1).write.mode("overwrite").parquet(
    "s3://dev-534086549449-analytics/parquet/tables/dim_producto_hist/"
)
print("Todo a terminado")
