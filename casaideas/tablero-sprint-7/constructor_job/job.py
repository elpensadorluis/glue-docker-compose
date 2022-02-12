import json
import logging
import boto3
import argparse

import awswrangler as wr
import pandas as pd
import sys
from datetime import datetime
import time
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

#############
from ast import literal_eval as safe_eval

# Start function main
def save_log(
    stage,
    origin,
    env,
    status,
    error,
    quantity,
    path,
    prefix,
    last_modified,
    enviromment,
):
    now = datetime.now()
    data = {
        "key": origin + "-" + path + "-" + stage + "-" + env,
        "date": now.strftime("%Y-%m-%d"),
        "time": now.strftime("%H:%M:%S"),
        "stage": stage,  # possible values: raw, staging, analytics or redshift
        "origin": origin,  # possible values: sap. In the future: magento, omnyx, zendesk, easystore, etc
        "environment": env,  # possible values: dev, qa or prod
        "status": status,  # possible values: True or False
        "error": error,  # error: error description, null if all is ok, failed and concatenate error description
        "quantity": quantity,  # number of items processed
        "path": path,  # full S3 path of the object
        "prefix": prefix,  # path of the object from S3 root folder
        "file_last_modified": last_modified,  # object's last modified date
    }
    send_to_dynamo(data, enviromment)
    return True


def send_to_dynamo(data, enviromment):
    print("Enviando a Dynamo de Tracking")
    dynamo = boto3.resource("dynamodb", endpoint_url="http://dynamodb-local:8000")
    # dynamo = boto3.resource("dynamodb")
    ambiente = enviromment
    table = dynamo.Table(f"casaideas_datalake_tracking_process_{ambiente}")
    now = time.strftime("%Y-%m-%d %H:%M:%S")
    table.put_item(Item=data)
    print("Se envio de forma correcta")
    print(f"casaideas_datalake_tracking_process_{ambiente}")
    return True


# Search objects in bucket + prefix with today's date
def search_todays_objects(env, stage, prefix, year, month, day):
    # Today's objects
    # FIXED: pensar en caso de uso diario, también pensar en rangos y personalización de día puntual
    todays_objects = []

    # Get current date
    # today = datetime.today() - timedelta(days=1)
    # today = today.date()

    # Get current date
    today = datetime(int(year), int(month), int(day)).date()
    # today = datetime.today().date()
    print(today)
    # Search bucket
    # session = boto3.session.Session(
    #     aws_access_key_id="foobar", aws_secret_access_key="foobarfoo"
    # )
    s3 = boto3.resource("s3", endpoint_url="http://s3:9000")
    # s3 = boto3.resource("s3")
    bucket = s3.Bucket(env + "-" + ACCOUNT_ID + "-" + stage)
    print("Imprimiendo el Bucket")
    print("prefijo: " + prefix)
    print(bucket)
    # Search in bucket objects with prefix and today's last modified date
    for object in bucket.objects.filter(Prefix=prefix):
        last = object.last_modified
        if last.date() == today:
            todays_objects.append(object.key)
    # Return today's objects
    return todays_objects


def check_glueTable(TABLE, DATABASE):
    glue_client = boto3.client("glue")
    try:
        glue_client.get_table(DatabaseName=DATABASE, Name=TABLE)
        return True
    except:
        print("La tabla {} no existe en la base de datos {}".format(TABLE, DATABASE))

        return False


# Start variables

logger = logging.getLogger()
logger = logging.getLogger(name="Transversal Job Starting")
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)

print("Iniciando el job: acceso a datos dynamodb")
dynamodb = boto3.resource(
    "dynamodb", region_name="us-east-1", endpoint_url="http://dynamodb-local:8000"
)

# dynamodb = boto3.resource("dynamodb")

client = boto3.client("dynamodb", endpoint_url="http://dynamodb-local:8000")
# client = boto3.client("dynamodb")
table = dynamodb.Table("casaideas_datalake_sap_qa")
date = datetime.today().strftime("%Y-%m-%d")
try:
    args = getResolvedOptions(sys.argv, ["JOB_NAME"])
except:
    print("Error: no se pudo obtener los argumentos")
    args = {
        "JOB_NAME": "casaideas-etl-sap-qa",
    }

print(date)
print(table)


# --- Inicializacion configuraciones de Spark ---

sc = SparkContext()
# # Para uso Local
# hc = sc._jsc.hadoopConfiguration()
# hc.set("fs.s3a.secret.key", "foobarfoo")
# hc.set("fs.s3a.access.key", "foobar")
# hc.set("fs.s3a.endpoint", "http://s3:9000")
# hc.set("fs.s3a.connection.ssl.enabled", "false")
# hc.set("fs.s3a.path.style.access", "true")

glueContext = GlueContext(sc)
spark = glueContext.spark_session
# spark = SparkSession.builder.appName("Test") # revisar si es necesario.
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Preparing Spark Context
conf = pyspark.SparkConf()

# FIXED: ubicar estos parámetros desde step functions
# year = datetime.today().strftime("%Y")
# month = datetime.today().strftime("%m")
# day = datetime.today().strftime("%d")

try:
    event = getResolvedOptions(
        sys.argv,
        [
            "ENV",  # "dev", "prod"
            "STAGE",  # "analytics", "staging", "raw"
            "PREFIX",  # "csv/tables/0co_pc_act_05"
            "ORIGIN",  # "sap"
            "TABLE",  # "0co_pc_act_05"
            "ENVIROMMENT",  # "qa", "prd"
            "EL_STRING",  # "[]"
            "YEAR",  # "2021"
            "MONTH",  # "12"
            "DAY",  # "06"
        ],
    )
except:
    event = {
        "ENV": "dev",
        "STAGE": "raw-2",
        "PREFIX": "parquet/sap/tables/tracking",
        "ORIGIN": "sap",
        "TABLE": "tracking",
        "ENVIROMMENT": "qa",
        "EL_STRING": "[]",
        "YEAR": "-",
        "MONTH": "-",
        "DAY": "-",
    }

print("Variables de entorno cargadas correctamente")
print(event)
print(event["TABLE"])

enviromment = event["ENVIROMMENT"]

print("Iniciando el job: acceso a datos dynamodb en la tabla {}".format(event["TABLE"]))
# scan all table dynamodb
# response = client.list_tables()
# response = client.get_item(
#     TableName=f"casaideas_datalake_sap_{enviromment}",
#     Key={"table": {"S": str(event["TABLE"])}},
# )
# print(response)

# test = json.dumps(response)
ACCOUNT_ID = "534086549449"
# FIXED: tanto producción como dev debe funcionar

# ACCOUNT_ID = "534086549449"

env = "prod" if event["ENV"] == "prod" else "dev"
if event["STAGE"] == "analytics":
    # Analytics
    stage = "redshift"
elif event["STAGE"] == "staging":
    # Staging
    stage = "data-lake"
elif event["STAGE"] == "raw-2":
    stage = "data-raw-2"
else:
    # Raw
    stage = "data-raw"
bucket = env + "-" + ACCOUNT_ID + "-" + stage
year = event["YEAR"] if event["YEAR"] != "-" else datetime.today().strftime("%Y")
month = event["MONTH"] if event["MONTH"] != "-" else datetime.today().strftime("%m")
day = event["DAY"] if event["DAY"] != "-" else datetime.today().strftime("%d")
prefix = event["PREFIX"] + "/year=" + year + "/month=" + month + "/day=" + day
print("prefijo: " + prefix)
origin = event["ORIGIN"] if event["ORIGIN"] != "" else "sap"

# FIXED: agregar Fuente en la tabla de dynamo
print("Iniciando el carga desde dynamodb")

STAGES = "data-raw,data-raw-2,data-lake,analytics,redshift"
STAGES = STAGES.split(",")
print(STAGES)
print("stages: " + STAGES[0])
INPUT_BUCKET = env + "-" + ACCOUNT_ID + "-" + STAGES[1]
OUTPUT_BUCKET = env + "-" + ACCOUNT_ID + "-" + STAGES[2]
FORMAT = "csv,parquet/sap"
FORMAT = FORMAT.split(",")
print("format: " + FORMAT[0] + " " + FORMAT[1])
PREFIX_IN = (
    FORMAT[1]
    + "/tables/tracking/"
    + "/year="
    + year
    + "/month="
    + month
    + "/day="
    + day
)
PREFIX_OUT = FORMAT[1] + "/tables/tracking/"
DATABASE = "sap_test_arkho"
TABLE = "tracking"
COLUMNS = '{"Doccompr":"documento_compra","Pos":"posicion_compras","Material":"num_material","Texto breve material":"texto_breve_material","Cantidad":"cantidad","Por Entregar Ctda":"cantidad_por_entregar","pendiente de Ingre":"porc_pendiente_ingreso","Estado":"estado","Texto Estado":"texto_estado","Proveedor":"proveedor","Nombre proveedor":"nombre_proveedor","CPag":"cpag","Email":"email","Temporada Pedido":"temporada","Texto Temporada":"texto_temporada","Indicativo temporada":"indicativo_temporada","Precionet ped UN":"precio","Pend Ingreso":"precio_pendiente","Lib":"lib","Por":"por","Mon":"moneda","OrgC":"organización_compras","Ce":"centro_id_sap","Alm":"almacen","Pto Emb":"puerto_embarque","Pais dest":"pais_destino","País Adq":"pais_adquisicion","Comprador":"comprador","Texto Responsable":"texto_responsable","PI":"pi_","Naviera":"naviera","Nave 1":"nave_1","Numero del BL":"numero_bl","ETD Cierre":"etd_cierre","ETD Negoci":"etd_negociable","ETD Inform":"etd_informada","ETA inform":"eta_informada","ETA Cierre":"eta_cierre","Anticipo":"anticipo","Saldo":"saldo","F pgo Ant":"fecha_pago_anticipo","F pgo Sal":"fecha_pago_saldo","Doc INV PL":"doc_inv_pl","INNER":"inner_","MASTER":"master","Fecha doc":"fecha_doc","Texto PO":"texto_po","Numero LC":"numero_lc","Efi":"efi"}'
SCHEMA = '{"documento_compra":"string","posicion_compras":"string","num_material":"string","texto_breve_material":"string","cantidad":"double","cantidad_por_entregar":"double","porc_pendiente_ingreso":"double","estado":"string","texto_estado":"string","proveedor":"string","nombre_proveedor":"string","cpag":"string","email":"string","temporada":"string","texto_temporada":"string","indicativo_temporada":"string","precio":"double","precio_pendiente":"double","lib":"double","por":"double","moneda":"string","organización_compras":"string","centro_id_sap":"string","almacen":"string","puerto_embarque":"string","pais_destino":"string","pais_adquisicion":"string","comprador":"string","texto_responsable":"string","pi_":"string","naviera":"string","nave_1":"string","numero_bl":"string","etd_cierre":"date","etd_negociable":"date","etd_informada":"date","eta_informada":"date","eta_cierre":"date","anticipo":"string","saldo":"string","fecha_pago_anticipo":"date","fecha_pago_saldo":"date","doc_inv_pl":"string","inner_":"double","master":"double","fecha_doc":"date","texto_po":"string","numero_lc":"string","efi":"string"}'
SEPARADOR = ";"  # FIXED: agregar a dynamo
QUOTECHAR = "'"  # FIXED: agregar a dynamo
EL_STRING = '[["etd_cierre", ".", "-"],]'  # FIXED: agregar a dynamo

print("seleccionando ultimo objeto en el bucket")

objects = search_todays_objects(env, stage, prefix, year, month, day)
archivo = []
for obj in objects:
    if obj.find(".parquet") != -1:
        archivo.append(obj)
# End routine
print("Imprimiendo la respuesta de la funcion:")
print(objects)
print("archivo: " + archivo[-1])

print("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<")
print(SCHEMA)
# schema_df = SCHEMA.replace("'", '"')
df_new_schema = json.loads(SCHEMA)
df_new_column = json.loads(COLUMNS)
df_new_string = safe_eval(EL_STRING) if EL_STRING != "" else []

# new_expr = [
#     "cast(cantidad as double) cantidad",
#     "cast(cantidad_por_entregar as double) cantidad_por_entregar",
#     "cast(porc_pendiente_ingreso as double) porc_pendiente_ingreso",
#     "cast(precio as double) precio",
#     "cast(precio_pendiente as double) precio_pendiente",
#     "cast(lib as double) lib",
#     "cast(por as double) por",
# ]
new_expr = []

SOURCE_PATH = "s3://{}/{}/".format(INPUT_BUCKET, archivo[-1])
TARGET_PATH = "s3://{}/{}/year={}/month={}/day={}/".format(
    OUTPUT_BUCKET, PREFIX_OUT, year, month, day
)
print("SOURCE_PATH: {}".format(SOURCE_PATH))
print("TARGET_PATH: {}".format(TARGET_PATH))
# FIXED: parametrizar la variable del separator y quoteChar en dynamodb
df_count = 0
if archivo:
    print("Construccion del dataframe")
    # TODO: tomar en cuenta tomar mas de un archivo
    df = glueContext.create_dynamic_frame.from_options(
        format_options={"withHeader": True, "separator": SEPARADOR},
        connection_type="s3",
        format="parquet",
        connection_options={"paths": [SOURCE_PATH], "recurse": True},
        transformation_ctx="df",
    )

    # df = spark.read.parquet(SOURCE_PATH)
else:
    df = None
    print("Error: No hay ningun objeto")

df = df.toDF()
print("Tamano del dataframe entrante: {}".format(df.count()))


count_tmp = df.count()
df_count = df_count + count_tmp
print("El objecto es un parquet")
print(df.show(5))
print(df.printSchema())
df_map = df.dtypes
df_columns_types = dict(df_map)

print("inicio de impresion de nombres de columnas")

# for col in df.columns:
#     df = df.withColumnRenamed(col, col.lower())

# for col in df.columns:
#     # print("{}: {}".format(col, df_new_schema[col]))
#     df = df.withColumnRenamed(col, df_new_column[col])

# print(df.printSchema())

if df_new_string:
    for a in df_new_string:
        df = df.withColumn(a[0], regexp_replace(a[0], a[1], a[2]))

# for col in df.columns:
#     df = df.withColumn(col, regexp_replace(col, "'", ""))

df = df.withColumn("etd_cierre", to_date(df.etd_cierre, "dd.MM.yyyy"))
df = df.withColumn("etd_negociable", to_date(df.etd_negociable, "dd.MM.yyyy"))
df = df.withColumn("etd_informada", to_date(df.etd_informada, "dd.MM.yyyy"))
df = df.withColumn("eta_informada", to_date(df.eta_informada, "dd.MM.yyyy"))
df = df.withColumn("eta_cierre", to_date(df.eta_cierre, "dd.MM.yyyy"))
df = df.withColumn("fecha_pago_anticipo", to_date(df.fecha_pago_anticipo, "dd.MM.yyyy"))
df = df.withColumn("fecha_pago_saldo", to_date(df.fecha_pago_saldo, "dd.MM.yyyy"))
df = df.withColumn("fecha_doc", to_date(df.fecha_doc, "dd.MM.yyyy"))

for column in df.columns:
    # print(column)
    # print(df_new_schema[column])
    # print(df_columns_types_a[column])
    new_expr.append("cast(" + column + " as " + df_new_schema[column] + ") " + column)


print(new_expr)

try:
    df = df.selectExpr(new_expr)
except:
    print("Problemas con la aplicacion del esquema de typo de datos")

df = (
    df.withColumn("year", lit(year))
    .withColumn("month", lit(month))
    .withColumn("day", lit(day))
)

print(df.show(5))
print(df.printSchema())

print("Tamano del dataframe saliente: {}".format(df.count()))


df.repartition(1).write.mode("overwrite").parquet(TARGET_PATH)

print("Escritura en parquet finalizada")
print(df.show(5))


print(df.printSchema())

job.commit()

# if check_glueTable(TABLE, DATABASE) is False:
#     print("Creando tabla {} en db {}.".format(TABLE, DATABASE))
#     wr.catalog.create_parquet_table(
#         database=DATABASE,
#         table=TABLE,
#         path="s3://{}/{}/".format(OUTPUT_BUCKET, PREFIX_OUT),
#         columns_types=df_columns_types,
#         # partitions_types=df_partition_types,
#         compression="snappy",
#         projection_enabled=False,
#     )
print(df.show(5))

# wr.catalog.add_parquet_partitions(
#     database=DATABASE, table=TABLE, partitions_values={TARGET_PATH: [year, month, day]}
# )


print("Rutina Completada: Felicidades!!!")
