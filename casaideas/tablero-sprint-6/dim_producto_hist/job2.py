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

from pyspark import SparkConf, SparkContext

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
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
# sc = SparkContext()
sc = SparkContext(conf=conf)
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

conf = pyspark.SparkConf()


date = datetime.today().strftime("%Y-%m-%d")
####################################
###########LECTURA###########

# s3://dev-534086549449-landing/casaideas/sybase/dim_tiempo_sybase.csv
# s3://dev-534086549449-landing/casaideas/sybase/
SOURCE_PATH = "s3://dev-534086549449-analytics/parquet/tables/dim_producto_hist/"

print("Iniciando carga de datos desde s3")

df = spark.read.option("header", "true").option("delimiter", ",").parquet(SOURCE_PATH)
# df = glueContext.create_dynamic_frame.from_options(
#     format_options={"withHeader": True, "separator": ","},
#     connection_type="s3",
#     format="csv",
#     connection_options={"paths": [SOURCE_PATH], "recurse": True},
#     transformation_ctx="df",
#     quoteChar='"',
# )
# df = df.toDF()
print(df)


# ----- Por Reparar ------
dataframe = df.toPandas()


# ---

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
    "inner": "string",
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
TABLE = "dim_producto_hist"
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
