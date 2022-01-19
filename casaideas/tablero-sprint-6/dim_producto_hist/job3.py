import json
import logging
import boto3
import argparse
import awswrangler as wr
import pandas as pd
import sys
from datetime import datetime
from boto3.dynamodb.conditions import Key

from pyspark import SparkConf, SparkContext

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

# Create new config
conf = SparkConf().set("spark.driver.maxResultSize", "4g")

# --- Inicializacion configuraciones de Spark ---
try:
    args = getResolvedOptions(sys.argv, ["JOB_NAME"])
except:
    # print("Error: no se pudo obtener los argumentos")
    args = {
        "JOB_NAME": "ficha_corta",
    }
# sc = SparkContext()
sc = SparkContext(conf=conf)
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
    .withColumnRenamed("INNER", "inner_")
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

# dff = dfc.filter(dfc.fecha.startswith("2021"))
dfh = dfc.filter(dfc.clave_temporada.startswith("2022VERANO 3"))
dfg = dfh.filter(dfh.id_producto.startswith("2"))
dfi = dfg.filter(dfg.mes.startswith("1"))

# dfh = dfc.filter(dfc.clave_temporada.startswith("2022VERANO 3"))

dfg.show(5, truncate=False)
dfg.describe().show()

# ### UUID ###
# dff.createOrReplaceTempView("tabla_final")
# dff.printSchema()

# query = f"""
#             Select uuid() as id_uuid
#                  , a.*


#             from tabla_final a
#             """

# dff = spark.sql(query)

print("Convirtiendo dataframe a pandas")
dataframe = dfg.toPandas()

print("Conversion a pandas realizada")
print(dataframe.head(15))
print(dataframe.info())

DATA_TYPES = {
    "_c0": "string",
    "fecha": "string",
    "mes": "string",
    "clave_temporada": "string",
    "tiempo_ano_mes": "string",
    "ano": "string",
    "temporada": "string",
    "status_compra": "string",
    "sku": "string",
    "sku_desc": "string",
    "id_madre": "string",
    "madre_desc": "string",
    "nivel_1": "string",
    "nivel_2": "string",
    "nivel_3": "string",
    "nivel_4": "string",
    "grupo_analisis": "string",
    "id_feria": "string",
    "fob": "string",
    "tipo_moneda": "string",
    "pp": "string",
    "moq": "string",
    "tipo_moq": "string",
    "cond_moq": "string",
    "display": "string",
    "pais_origen": "string",
    "leadtime": "string",
    "volumen": "string",
    "peso": "string",
    "inner_": "string",
    "master": "string",
    "duracion": "string",
    "fecha_ini": "string",
    "fecha_ter": "string",
    "fecha_ini_2": "string",
    "fecha_ter_2": "string",
    "num_material": "string",
    "id_producto": "string",
}

print("Iniciando Carga al Redshift")
TABLE = "feria_vigente_01"
SCHEMA = "dev"
PATH_TMP = "s3://aws-glue-scripts-534086549449/prueba_s3_to_redshift/tmp00/"
PRIMARY_KEY = "_c0"


def check_Table(con, schema, table):
    stmt = (
        "SELECT EXISTS (SELECT 1 FROM   information_schema.tables WHERE  table_schema = '%s' AND table_name = '%s');"
        % (schema, table)
    )
    with con.cursor() as cursor:
        cursor.execute(stmt)
        s = cursor.fetchone()
        s = " ".join(map(str, s))
    if s == "False":
        return False
    else:
        return True


print("Iniciando Conexion con Redshift")
con = wr.redshift.connect("redshift")
data_types = DATA_TYPES

print(dataframe.info())

if check_Table(con, SCHEMA, TABLE) is False:
    print("Creando tabla {} en db {}.".format(TABLE, SCHEMA))

    wr.redshift.copy(
        df=dataframe,
        path=PATH_TMP,
        con=con,
        schema=SCHEMA,
        table=TABLE,
        dtype=data_types,
        mode="overwrite",
        primary_keys=[PRIMARY_KEY],
    )

else:
    print("Actualizando tabla {} en db {}.".format(TABLE, SCHEMA))

    wr.redshift.copy(
        df=dataframe,
        path=PATH_TMP,
        con=con,
        schema=SCHEMA,
        table=TABLE,
        dtype=data_types,
        mode="upsert",
        primary_keys=[PRIMARY_KEY],
    )

con.close()

print("Todo salio bien")
