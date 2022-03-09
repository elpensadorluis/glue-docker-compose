import json
from datetime import datetime
from os import rename

from pyspark.sql import SparkSession
import pyspark
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    array,
    ArrayType,
    DateType,
    DecimalType,
)
from pyspark.sql.functions import *
from pyspark.sql.functions import concat, lit, col
from pyspark.sql import functions as f

from awsglue.utils import getResolvedOptions

spark = (
    pyspark.sql.SparkSession.builder.appName("Product_Price_Tracking")
    .config(
        "spark.jars",
        "s3://aws-glue-hudi-534086549449/v2/hudi-spark-bundle_2.11-0.5.3-rc2.jar,s3://aws-glue-hudi-534086549449/v2/spark-avro_2.11-2.4.4.jar",
    )
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.sql.hive.convertMetastoreParquet", "false")
    .getOrCreate()
)

COLUMNS = '{"_c1": "_c0","_c2": "_c1","_c3": "_c2","_c4": "_c3","_c5": "_c4","_c6": "_c5","_c7": "_c6","_c8": "_c7","_c9": "_c8","_c10": "_c9","_c11": "_c10","_c12": "_c11","_c13": "_c12","_c14": "_c13","_c15": "_c14","_c16": "_c15","_c17": "_c16","_c18": "_c17","_c19": "_c18","_c20": "_c19","_c21": "_c20","_c22": "_c21","_c23": "_c22","_c24": "_c23","_c25": "_c24","_c26": "_c25","_c27": "_c26","_c28": "_c27","_c29": "_c28","_c30": "_c29","_c31": "_c30","_c32": "_c31","_c33": "_c32","_c34": "_c33","_c35": "_c34","_c36": "_c35","_c37": "_c36","_c38": "_c37","_c39": "_c38","_c40": "_c39","_c41": "_c40","_c42": "_c41","_c43": "_c42","_c44": "_c43","_c45": "_c44","_c46": "_c45","_c47": "_c46","_c48": "_c47","_c49": "_c48","_c50": "_c49","_c51": "_c50","_c52": "_c51","_c53": "_c52","_c54": "_c53","_c55": "_c54","_c56": "_c55","_c57": "_c56","_c58": "_c57","_c59": "_c58","_c60": "_c59","_c61": "_c60","_c62": "_c61","_c63": "_c62","_c64": "_c63","_c65": "_c64","_c66": "_c65","_c67": "_c66","_c68": "_c67","_c69": "_c68","_c70": "_c69","_c71": "_c70","_c72": "_c71","_c73": "_c72","_c74": "_c73","_c75": "_c74","_c76": "_c75","_c77": "_c76","_c78": "_c77","_c79": "_c78","_c80": "_c79","_c81": "_c80","_c82": "_c81","_c83": "_c82","_c84": "_c83","_c85": "_c84","_c86": "_c85","_c87": "_c86","_c88": "_c87","_c89": "_c88","_c90":"_c89","_c91":"_c90","_c92":"_c91","_c93":"_c92","_c94":"_c93","_c95":"_c94","_c96":"_c95","_c97":"_c96","_c98":"_c97","_c99":"_c98","_c100":"_c99","update_ts_dms": "update_ts_dms","schema_name":"schema_name","table_name":"table_name"}'
numero_col = 92
final_col = [
    "_c0",
    "_c1",
    "_c2",
    "_c3",
    "_c4",
    "_c5",
    "_c6",
    "_c7",
    "_c8",
    "_c9",
    "_c10",
    "_c11",
    "_c12",
    "_c13",
    "_c14",
    "_c15",
    "_c16",
    "_c17",
    "_c18",
    "_c19",
    "_c20",
    "_c21",
    "_c22",
    "_c23",
    "_c24",
    "_c25",
    "_c26",
    "_c27",
    "_c28",
    "_c29",
    "_c30",
    "_c31",
    "_c32",
    "_c33",
    "_c34",
    "_c35",
    "_c36",
    "_c37",
    "_c38",
    "_c39",
    "_c40",
    "_c41",
    "_c42",
    "_c43",
    "_c44",
    "_c45",
    "_c46",
    "_c47",
    "_c48",
    "_c49",
    "_c50",
    "_c51",
    "_c52",
    "_c53",
    "_c54",
    "_c55",
    "_c56",
    "_c57",
    "_c58",
    "_c59",
    "_c60",
    "_c61",
    "_c62",
    "_c63",
    "_c64",
    "_c65",
    "_c66",
    "_c67",
    "_c68",
    "_c69",
    "_c70",
    "_c71",
    "_c72",
    "_c73",
    "_c74",
    "_c75",
    "_c76",
    "_c77",
    "_c78",
    "_c79",
    "_c80",
    "_c81",
    "_c82",
    "_c83",
    "_c84",
    "_c85",
    "_c86",
    "_c87",
    "_c88",
    "update_ts_dms",
    "schema_name",
    "table_name",
]

try:
    event = getResolvedOptions(
        sys.argv,
        ["ENV", "YEAR", "MONTH", "DAY",],  # "dev", "prod"  # "2021"  # "12"  # "06"
    )
except:
    event = {
        "ENV": "dev",
        "YEAR": "2022",
        "MONTH": "02",
        "DAY": "17",
    }

env = "prod" if event["ENV"] == "prod" else "dev"
year = event["YEAR"] if event["YEAR"] != "-" else datetime.today().strftime("%Y")
month = event["MONTH"] if event["MONTH"] != "-" else datetime.today().strftime("%m")
day = event["DAY"] if event["DAY"] != "-" else datetime.today().strftime("%d")

date = year + "-" + month + "-" + day
id_date = year + month + day + "-"

table_exist = False  # True or False
df_new_column = json.loads(COLUMNS)
TABLE_NAME = "DESCUENTOS"
S3_RAW_DATA = f"s3://{env}-534086549449-data-raw-2/parquet/easy_store/ESTORE/DESCUENTOS/historico/"
S3_HUDI_DATA = f"s3://{env}-534086549449-data-raw-2/hudi/data/DESCUENTOS"

# TODO: Hacer que funcione cada dia con las entradas year, month, day
S3_INCR_RAW_DATA = f"s3://{env}-534086549449-data-raw-2/parquet/easy_store/ESTORE/DESCUENTOS/year={year}/month={month}/day={day}/"

if not table_exist:
    print("Creating table...")
    df_coal_prod = (
        spark.read.format("parquet").options(header="false").load(S3_RAW_DATA)
    )

    df_coal_prod.show(5, truncate=False)

    df_coal_prod.write.format("org.apache.hudi").option(
        "hoodie.table.name", TABLE_NAME
    ).option("hoodie.datasource.write.storage.type", "COPY_ON_WRITE").option(
        "hoodie.datasource.write.operation", "bulk_insert"
    ).option(
        "hoodie.datasource.write.recordkey.field", "_c0"
    ).option(
        "hoodie.datasource.write.precombine.field", "update_ts_dms"
    ).mode(
        "overwrite"
    ).save(
        S3_HUDI_DATA
    )

    df_final = spark.read.format("org.apache.hudi").load(
        "s3://dev-534086549449-data-raw-2/hudi/data/DESCUENTOS/default/*.parquet"
    )
    df_final.registerTempTable("DESCUENTOS")
    spark.sql("select count(*) from DESCUENTOS").show(5)

else:
    print("Table already exists")
    df_coal_prod_incr = (
        spark.read.format("parquet").options(header="true").load(S3_INCR_RAW_DATA)
    )

    # Rutina para verificar si existe una columna en la tabla
    cuenta_fila = int(df_coal_prod_incr.count())
    print(f"Number of rows in the dataframe: {cuenta_fila}")
    cuenta_columna = len(df_coal_prod_incr.columns)
    i = 0 + cuenta_columna - 3
    while i <= numero_col:
        df_coal_prod_incr = df_coal_prod_incr.withColumn(
            f"_c{i}", lit(None).cast(StringType())
        )
        i += 1

    # df_coal_prod_incr = df_coal_prod_incr.select([column for column in final_col])

    df_coal_prod_incr.show(5, truncate=False)

    # Filtra los datos que ya existen en la tabla con update and insert en la columna _c0
    df_coal_prod_incr_u_i = df_coal_prod_incr.filter("_c0 IN ('U', 'I')")

    df_coal_prod_incr_u_i.show(5, truncate=False)
    df_coal_prod_incr_u_i_f = df_coal_prod_incr_u_i.drop(df_coal_prod_incr_u_i._c0)

    # TODO: cambiar nombre de columnas
    print("renaming columns...")
    for col in df_coal_prod_incr_u_i_f.columns:
        # print("{}: {}".format(col, df_new_schema[col]))

        df_coal_prod_incr_u_i_f = df_coal_prod_incr_u_i_f.withColumnRenamed(
            col, df_new_column[col]
        )

    # TODO: eliminar columna que no se usa
    print("dropping columns...")
    for col in df_coal_prod_incr_u_i_f.columns:
        # print("{}: {}".format(col, df_new_schema[col]))
        if not col in final_col:
            df_coal_prod_incr_u_i_f = df_coal_prod_incr_u_i_f.drop(col)

    # if "_c89" in df_coal_prod_incr_u_i_f.columns:
    #     df_coal_prod_incr_u_i_f = df_coal_prod_incr_u_i_f.drop("_c89")

    # df_coal_prod_incr_u_i_f = df_coal_prod_incr_u_i_f.select(
    #     [column for column in final_col]
    # )

    df_coal_prod_incr_u_i_f = df_coal_prod_incr_u_i_f.select(
        [column for column in final_col]
    )

    df_coal_prod_incr_u_i_f.printSchema()

    print("Save data to hudi UPDATE and INSERT")

    # TODO: prueba de nuevas columnas primera carga UPDATE INSERT
    df_coal_prod_incr_u_i_f.write.format("org.apache.hudi").option(
        "hoodie.table.name", TABLE_NAME
    ).option("hoodie.datasource.write.storage.type", "COPY_ON_WRITE").option(
        "hoodie.datasource.write.operation", "upsert"
    ).option(
        "hoodie.upsert.shuffle.parallelism", 20
    ).option(
        "hoodie.datasource.write.recordkey.field", "_c0"
    ).option(
        "hoodie.datasource.write.precombine.field", "update_ts_dms"
    ).mode(
        "append"
    ).save(
        S3_HUDI_DATA
    )

    df_final = spark.read.format("org.apache.hudi").load(
        "s3://dev-534086549449-data-raw-2/hudi/data/DESCUENTOS/default/*.parquet"
    )
    df_final.registerTempTable("coal_prod")
    spark.sql("select count(*) from coal_prod").show(5)

    df_coal_prod_incr_d = df_coal_prod_incr.filter("_c0 IN ('D')")

    df_coal_prod_incr_d_f = df_coal_prod_incr_d.drop(df_coal_prod_incr_u_i._c0)

    # TODO: cambiar nombre de columnas
    print("renaming columns...")
    for col in df_coal_prod_incr_d_f.columns:
        # print("{}: {}".format(col, df_new_schema[col]))

        df_coal_prod_incr_d_f = df_coal_prod_incr_d_f.withColumnRenamed(
            col, df_new_column[col]
        )

    # TODO: eliminar columna que no se usa
    print("dropping columns...")
    for col in df_coal_prod_incr_d_f.columns:
        # print("{}: {}".format(col, df_new_schema[col]))
        if not col in final_col:
            df_coal_prod_incr_d_f = df_coal_prod_incr_d_f.drop(col)

    # df_coal_prod_incr_d_f = df_coal_prod_incr_d_f.select(
    #     [column for column in final_col]
    # )

    # if "_c89" in df_coal_prod_incr_d_f.columns:
    #     df_coal_prod_incr_d_f = df_coal_prod_incr_d_f.drop("_c89")

    df_coal_prod_incr_d_f = df_coal_prod_incr_d_f.select(
        [column for column in final_col]
    )

    df_coal_prod_incr_d_f.show()

    # Se anexa los datos de borrado
    print("Save data to hudi DELETE")
    df_coal_prod_incr_d_f.write.format("org.apache.hudi").option(
        "hoodie.table.name", TABLE_NAME
    ).option("hoodie.datasource.write.storage.type", "COPY_ON_WRITE").option(
        "hoodie.datasource.write.operation", "upsert"
    ).option(
        "hoodie.upsert.shuffle.parallelism", 20
    ).option(
        "hoodie.datasource.write.recordkey.field", "_c0"
    ).option(
        "hoodie.datasource.write.precombine.field", "update_ts_dms"
    ).option(
        "hoodie.datasource.write.payload.class",
        "org.apache.hudi.EmptyHoodieRecordPayload",
    ).mode(
        "append"
    ).save(
        S3_HUDI_DATA
    )


print("Hasta aqui todo bien")
