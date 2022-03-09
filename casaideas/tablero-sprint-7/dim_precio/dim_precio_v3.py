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

# Inicializacion de variables
condiciones = ["ZP03", "ZP02", "ZP01"]

col_dim_precios = [
    "num_material",
    "num_reg_condicion",
    "uni_condicion",
    "clase_condicion",
    "inicio_validez_reg_condicion",
    "fin_validez_reg_condicion",
    "ZP03",
    "ZP02",
    "ZP01",
]

col_orden_precios = [
    "num_material",
    "uni_condicion",
    "inicio_validez_reg_condicion",
    "fin_validez_reg_condicion",
]

orden_condicion = ["liquidacion", "ajuste", "regular"]

dict_cols = {
    "uni_condicion": "moneda",
    "ZP03": "regular",
    "ZP02": "ajuste",
    "ZP01": "liquidacion",
    "inicio_validez_reg_condicion": "inicio",
    "fin_validez_reg_condicion": "fin",
}

### CARGA TABLAS KONP / A004 como precios y condicion
# print("Cargando tablas...")
# precios = pd.read_parquet(
#     "s3://dev-534086549449-data-lake/parquet/sap/tables/knop/year=2022/month=01/day=18/"
# )
# condicion = pd.read_parquet(
#     "s3://dev-534086549449-data-lake/parquet/sap/tables/a004/year=2022/month=01/day=18/"
# )

# df_p = spark.read.option("header", "true").parquet(
#     "s3://dev-534086549449-data-lake/parquet/sap/tables/knop/year=2022/month=01/day=18/"
# )
# df_c = spark.read.option("header", "true").parquet(
#     "s3://dev-534086549449-data-lake/parquet/sap/tables/a004/year=2022/month=01/day=18/"
# )


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


df_p = (
    spark.read.format("parquet")
    .options(header="true")
    .load(
        f"s3://{env}-534086549449-data-lake/parquet/sap/tables/knop/year={year}/month={month}/day={day}/"
    )
)
df_c = (
    spark.read.format("parquet")
    .options(header="true")
    .load(
        f"s3://{env}-534086549449-data-lake/parquet/sap/tables/a004/year={year}/month={month}/day={day}/"
    )
)
print("Creando tablas ")
df_p.createOrReplaceTempView("precios")
df_c.createOrReplaceGlobalTempView("condicion")
df_p.printSchema()
df_c.printSchema()


# precios = df_p.select("*").toPandas()
# condicion = df_c.select("*").toPandas()

# precios = df_p.toDF()
# condicion = df_c.toDF()

## Ajustes y filtros a precios
# pandas: precios = precios[precios.clase_condicion.isin(condiciones)]  # Filtro Condiciones
# pyspark: df[df.name.isin("Bob", "Mike")].collect()

# df_p.show()
precios = df_p.filter(df_p.clase_condicion.isin(condiciones))
# precios.show()
# pandas: condicion = condicion[condicion.clase_condicion.isin(condiciones)]
# df_c.show()
condicion = df_c.filter(df_c.clase_condicion.isin(condiciones))
# condicion.show()

# pandas: precios.loc[:, "importe_porcentaje_cond"] = precios["importe_porcentaje_cond"].apply(
#     lambda x: x * 100
# )  # Ajuste al precio.
# pyspark:
# precios.show()
# precios.loc[:, "importe_porcentaje_cond"] = precios["importe_porcentaje_cond"].apply(
#     lambda x: x * 100
# )
# multiplicar por 100 la columna importe_porcentaje_cond en precios con lambda
# precios = precios.withColumn(
#     "importe_porcentaje_cond", precios["importe_porcentaje_cond"] * 100
# )
# precios.show()

# # Pivote de valores de tipos de precio a columnas
# precios_pv = (
#     precios.pivot_table(
#         index=["num_reg_condicion", "uni_condicion"],
#         values="importe_porcentaje_cond",
#         columns="clase_condicion",
#     )
#     .reset_index()
#     .set_index("num_reg_condicion")
# )

condicion.createOrReplaceTempView("condicion")
precios.createOrReplaceTempView("precio")

query = f"""
    SELECT * FROM
    (SELECT 
      num_reg_condicion
      , aplicacion
      , uni_condicion
      , clase_condicion
      , importe_porcentaje_cond
      FROM precio)
      PIVOT 
        (
          SUM(importe_porcentaje_cond*100) 
          FOR clase_condicion IN ('ZP03' AS regular, 'ZP02' AS ajuste, 'ZP01' AS liquidacion)
    	)
      """

df_a = spark.sql(query)
df_a.printSchema()
# df_a.show(10)

df_a.createOrReplaceTempView("precio_pivot")

query = f"""
SELECT
  c.num_material
  , p.uni_condicion as moneda
  , p.num_reg_condicion
  , c.inicio_validez_reg_condicion
  , c.fin_validez_reg_condicion
  , p.aplicacion
--  , c.clase_condicion  
  , ROUND(p.regular,4) regular
  , ROUND(p.ajuste,4) as ajuste
  , ROUND(p.liquidacion,4) as liquidacion
  ,
CASE
  WHEN c.clase_condicion = 'ZP03' THEN 'regular'
  WHEN c.clase_condicion = 'ZP02' THEN 'ajuste'
  WHEN c.clase_condicion = 'ZP01' THEN 'liquidacion'
END AS clase_condicion  
FROM 
	 precio_pivot p
      
inner join condicion c 
on p.num_reg_condicion =  c.num_reg_condicion
order by num_material, moneda, inicio_validez_reg_condicion, fin_validez_reg_condicion
"""
print("aplicando query precio_pivot")
df_b = spark.sql(query)

df_b.printSchema()
# df_b.show()

# # Probando suerte

# from pyspark.sql.functions import to_timestamp

# print("convirtiendo date spark a pandas")
# inicio_validez_reg_condicion: date (nullable = true)
# fin_validez_reg_condicion: date (nullable = true)
# df_precios = df_b.withColumn(
#     "inicio_validez_reg_condicion",
#     to_timestamp(df_b.inicio_validez_reg_condicion, "yyyy-MM-dd"),
# )
# df_precios = df_precios.withColumn(
#     "fin_validez_reg_condicion",
#     to_timestamp(df_b.fin_validez_reg_condicion, "yyyy-MM-dd"),
# )

print("convirtiendo date spark a string")

from pyspark.sql.types import StringType

df_precios = df_b.withColumn(
    "inicio_validez_reg_condicion",
    df_b["inicio_validez_reg_condicion"].cast(StringType()),
)
df_precios = df_precios.withColumn(
    "fin_validez_reg_condicion",
    df_precios["fin_validez_reg_condicion"].cast(StringType()),
)


df_precios.printSchema()
# df_precios.show()

print("convirtiendo a pandas")
dim_precios = df_precios.toPandas()
print("finalizando conversion")


# # Inner Join
# dim_precios = condicion.merge(
#     precios_pv, how="inner", left_on="num_reg_condicion", right_on="num_reg_condicion"
# )

# # Orden de columnas útiles + orden de tabla
# dim_precios = df_b[col_dim_precios].sort_values(col_orden_precios)

# dim_precios = df_b.select(col_dim_precios).sort_values(col_orden_precios)
# dim_precios.show(truncate=False)

# # Renombrar columnas
# dim_precios.rename(columns=dict_cols, inplace=True)

# TODO: para la siguiente etapa #  Asignar clase_condicion como categoria organizada.
# dim_precios["clase_condicion"] = pd.Categorical(
#     dim_precios.clase_condicion.map(dict_cols), orden_condicion
# )

# TODO: # Null Fillers

print("Inicio de filtrado")
dim_precios["regular"] = dim_precios.groupby(["num_material", "moneda"], sort=False)[
    "regular"
].apply(lambda x: x.ffill().bfill())

pd.pandas.set_option("display.max_columns", None)
dim_precios.info()
print(dim_precios.head(10))

dim_precios.loc[
    dim_precios.clase_condicion == "liquidacion", "ajuste"
] = dim_precios.groupby(["num_material", "moneda"], sort=False)["ajuste"].apply(
    lambda x: x.ffill()
)
print("fin de filtrado")

dim_precios.info()
print(dim_precios.head(10))
# Null fillers in pyspark

print("Inicio calculo descuentos y ajustes Nulos = 0")
# TODO: # Calcular Descuentos. Nulos = 0
dim_precios["r_a"] = (round(1 - (dim_precios.ajuste / dim_precios.regular), 4)).fillna(
    0
)
dim_precios["r_l"] = (
    round(1 - (dim_precios.liquidacion / dim_precios.regular), 4)
).fillna(0)
dim_precios["a_l"] = (
    round(1 - (dim_precios.liquidacion / dim_precios.ajuste), 4)
).fillna(0)

print(dim_precios.head(10))

print("Grabando a disco")
# dim_precios.to_parquet("s3://dev-534086549449-analytics/parquet/tables/dim_precios/part-00000-cfbee89f-6128-44f0-ae6f-f0b14a65a244-c000.snappy.parquet")

# ---

DATA_TYPES = {
    "num_material": "string",
    "moneda": "string",
    "num_reg_condicion": "string",
    "inicio_validez_reg_condicion": "string",
    "fin_validez_reg_condicion": "string",
    "aplicacion": "string",
    "regular": "double",
    "ajuste": "double",
    "liquidacion": "double",
    "clase_condicion": "string",
    "r_a": "double",
    "r_l": "double",
    "a_l": "double",
}

TABLE = "dim_precios"
SCHEMA = "dev"
PATH_TMP = "s3://aws-glue-scripts-534086549449/prueba_s3_to_redshift/tmp00/"
PRIMARY_KEY = "num_material"


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
        stmt = "DROP TABLE IF EXISTS %s.%s;" % (schema, table)
        with con.cursor() as cursor:
            cursor.execute(stmt)
        return False


con = wr.redshift.connect("redshift")
data_types = DATA_TYPES

print(dim_precios.info())

if check_Table(con, SCHEMA, TABLE) is False:
    print("Creando tabla {} en db {}.".format(TABLE, SCHEMA))

    wr.redshift.copy(
        df=dim_precios,
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
    print("Algun Error en la actualizacion")
    # wr.redshift.copy(
    #     df=dim_precios,
    #     path=PATH_TMP,
    #     con=con,
    #     schema=SCHEMA,
    #     table=TABLE,
    #     dtype=data_types,
    #     mode="overwrite",
    #     overwrite_method="truncate",
    #     primary_keys=[PRIMARY_KEY],
    # )

con.close()


print("Ha terminado satisfactoriamente")
