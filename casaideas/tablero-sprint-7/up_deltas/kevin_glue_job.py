import sys
import logging

import awswrangler as wr
from datetime import datetime

# import pyspark
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

import pandas as pd


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


logger = logging.getLogger(name="Job Starting")
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)

# --- Inicializacion configuraciones de Spark ---
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = (
    SparkSession.builder.appName("dim_mix_comercial")
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

# --- Inicializacion de Variables ---
try:
    event = getResolvedOptions(sys.argv, ["ENV", "YEAR", "MONTH", "DAY",],)
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

account_id = "534086549449"
r_stage = "data-lake"
w_stage = "analytics"
r_formato = "parquet"
w_formato = "parquet"
r_source = "marc"
table = "dim_mix_comercial"
path_tmp = "s3://aws-glue-scripts-534086549449/prueba_s3_to_redshift/tmp00/"
output_s3_path = f"s3://{env}-{account_id}-{w_stage}/{w_formato}/tables/{table}/"

df = (
    spark.read.format("parquet")
    .options(header="true")
    .load(
        f"s3://{env}-{account_id}-{r_stage}/{r_formato}/sap/tables/{r_source}/year={year}/month={month}/day={day}/"
    )
)

df.createOrReplaceTempView("marc")

query = f"""
        SELECT
            concat({year}, {month}, {day},'-',werks,'-',matnr) AS id_mix,
            concat({year}, {month}, {day}) as id_tiempo,
            concat({year},'-', {month},'-', {day}) as fecha_db,
            werks as centro_id_sap,
            matnr as num_material,
            mmsta as status_material
        FROM marc
"""
dim_mix = spark.sql(query)
dim_mix.printSchema()

print("Grabando a disco")
dim_mix.repartition(1).write.format("parquet").options(header="true").mode(
    "overwrite"
).save(output_s3_path)
print("Guardado en Analitycs")

# df = dim_mix.toPandas()
# df["fecha_db"] = pd.to_datetime(df.fecha_db, format="%Y-%m-%d")

# con = wr.redshift.connect("redshift")

# #  ---------------- LLAVES -------------------
# d_style = "KEY"  # ["AUTO", "EVEN", "ALL", "KEY"].
# dk = "num_material"
# sk = ["fecha_db", "num_material", "centro_id_sap"]
# pk = ["id_mix"]

# data_types = {
#     "id_mix": "string",
#     "fecha_db": "date",
#     "id_tiempo": "string",
#     "centro_id_sap": "string",
#     "num_material": "string",
#     "status_material": "string",
# }

# if check_Table(con, env, table) is False:

#     print(f"Creando tabla {table} en db {env}.")

#     wr.redshift.copy(
#         df=df,
#         path=path_tmp,
#         con=con,
#         schema=env,
#         table=table,
#         dtype=data_types,
#         mode="overwrite",
#         diststyle=d_style,
#         distkey=dk,
#         sortkey=sk,
#         primary_keys=pk,
#     )

# else:
#     print(f"Actualizando tabla {table} en db {env}.")

#     wr.redshift.copy(
#         df=df,
#         path=path_tmp,
#         con=con,
#         schema=env,
#         table=table,
#         dtype=data_types,
#         mode="append",
#     )

# con.close()
print("Ha terminado satisfactoriamente")
