# integration glue job
# rutina para primera carga de datos con delta lake
# Import the necessary packages
from delta import *
from pyspark.sql.session import SparkSession

# Initialize Spark Session along with configs for Delta Lake
spark = (
    SparkSession.builder.config(
        "spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"
    )
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    .getOrCreate()
)

### Inicio de funciones ###
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
    # s3 = boto3.resource("s3", endpoint_url="http://s3:9000")
    s3 = boto3.resource("s3")
    bucket = s3.Bucket("prod" + "-" + ACCOUNT_ID + "-" + stage)
    print("Imprimiendo el Bucket")
    print("prefijo: " + prefix)
    print(bucket)
    # Search in bucket objects with prefix and today's last modified date
    for object in bucket.objects.filter(Prefix=prefix):
        todays_objects.append(object.key)
        # last = object.last_modified
        # if last.date() == today:
        #     todays_objects.append(object.key)
    # Return today's objects
    return todays_objects


### Inicializacion de variables ###
ACCOUNT_ID = "534086549449"
stage = ["data-raw", "data-raw-2"]
prefix = "ficha_corta/corporativo/madre/"
# create array columnas from _c0 to _c46


### Tomando variables de entorno ###

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

source = "s3://prod" + "-" + ACCOUNT_ID + "-" + stage[0] + "/"
target = (
    "s3://"
    + env
    + "-"
    + ACCOUNT_ID
    + "-"
    + stage[1]
    + "/parquet/"
    + prefix
    + year
    + "/"
    + month
    + "/"
    + day
    + "/"
)
### codigo ###
### Crea una copia fisica en s3 de la tabla full y la carga desde tmp
spark.sql("use ficha_corta")
spark.sql("show tables").show()
spark.sql("describe formatted madre").show(100, truncate=False)
spark.sql("ALTER TABLE madre SET TBLPROPERTIES ('transactional' ='true')")

df_f = spark.read.parquet(
    f"s3://{env}-534086549449-data-raw-2/parquet/ficha_corta/corporativo/madre/full/"
)
df_f.show()

df_f.repartition(1).write.option(
    "path",
    f"s3://{env}-534086549449-data-raw-2/parquet/ficha_corta/corporativo/madre/tmp/",
).mode("overwrite").saveAsTable("ficha_corta.madre")


# df_f.repartition(1).write.mode("overwrite").parquet(
#     f"s3://{env}-534086549449-data-raw-2/parquet/ficha_corta/corporativo/madre/tmp/"
# )
# df_full = spark.read.parquet(
#     f"s3://{env}-534086549449-data-raw-2/parquet/ficha_corta/corporativo/madre/tmp/"
# )
# df_full.repartition(1).write.option("path")
# filtered.repartition(1).write.option("path","s3://testbucket/testpath/").mode("append").saveAsTable("emrdb.testtableemr")

# df_full.createOrReplaceTempView("full")
# df_f.crea
df_f.printSchema()
df_f.show(50, truncate=False)
df_f.describe().show()

objects = search_todays_objects(env, stage[0], prefix, year, month, day)

print("Imprimiendo la respuesta de la funcion:")
print(objects)

print("año: " + year + " mes: " + month + " dia: " + day)

for object in objects:
    if object.find("20220111-") != -1:
        print(f"[{object}],")

        df = spark.read.format("csv").options(header="false").load(source + object)

        df.printSchema()
        df.show(truncate=False)
        data_collect = df.collect()
        for row in data_collect:
            # while looping through each
            # row printing the data of Id, Name and City
            if row["_c0"] == "U":
                # print(row["_c0"], row["_c1"], "  ", row["_c2"])
                query = f"""
                update ficha_corta.madre
                set _c1 = "{row["_c2"]}",
                    _c2 = "{row["_c3"]}",
                    _c3 = "{row["_c4"]}",
                    _c4 = "{row["_c5"]}",
                    _c5 = "{row["_c6"]}",
                    _c6 = "{row["_c7"]}",
                    _c7 = "{row["_c8"]}",
                    _c8 = "{row["_c9"]}",
                    _c9 = "{row["_c10"]}",
                    _c10 = "{row["_c11"]}",
                    _c11 = "{row["_c12"]}",
                    _c12 = "{row["_c13"]}",
                    _c13 = "{row["_c14"]}",
                    _c14 = "{row["_c15"]}",
                    _c15 = "{row["_c16"]}",
                    _c16 = "{row["_c17"]}",
                    _c17 = "{row["_c18"]}",
                    _c18 = "{row["_c19"]}",
                    _c19 = "{row["_c20"]}",
                    _c20 = "{row["_c21"]}",
                    _c21 = "{row["_c22"]}",
                    _c22 = "{row["_c23"]}",
                    _c23 = "{row["_c24"]}",
                    _c24 = "{row["_c25"]}",
                    _c25 = "{row["_c26"]}",
                    _c26 = "{row["_c27"]}",
                    _c27 = "{row["_c28"]}",
                    _c28 = "{row["_c29"]}",
                    _c29 = "{row["_c30"]}",
                    _c30 = "{row["_c31"]}",
                    _c31 = "{row["_c32"]}",
                    _c32 = "{row["_c33"]}",
                    _c33 = "{row["_c34"]}",
                    _c34 = "{row["_c35"]}",
                    _c35 = "{row["_c36"]}",
                    _c36 = "{row["_c37"]}",
                    _c37 = "{row["_c38"]}",
                    _c38 = "{row["_c39"]}",
                    _c39 = "{row["_c40"]}",
                    _c40 = "{row["_c41"]}",
                    _c41 = "{row["_c42"]}",
                    _c42 = "{row["_c43"]}",
                    _c43 = "{row["_c44"]}",
                    _c44 = "{row["_c45"]}",
                    _c45 = "{row["_c46"]}",
                    _c46 = "{row["_c47"]}"
                where _c0 = "{row["_c1"]}";
                """

                spark.sql(query)

            if row["_c0"] == "I":
                query = f"""
                insert into ficha_corta.madre (_c1,_c2,_c3,_c4,_c5,_c6,_c7,_c8,_c9,_c10,_c11,_c12,_c13,_c14,_c15,_c16,_c17,_c18,_c19,_c20,_c21,_c22,_c23,_c24,_c25,_c26,_c27,_c28,_c29,_c30,_c31,_c32,_c33,_c34,_c35,_c36,_c37,_c38,_c39,_c40,_c41,_c42,_c43,_c44,_c45,_c46)
                values ("{row["_c2"]}","{row["_c3"]}","{row["_c4"]}","{row["_c5"]}","{row["_c6"]}","{row["_c7"]}","{row["_c8"]}","{row["_c9"]}","{row["_c10"]}","{row["_c11"]}","{row["_c12"]}","{row["_c13"]}","{row["_c14"]}","{row["_c15"]}","{row["_c16"]}","{row["_c17"]}","{row["_c18"]}","{row["_c19"]}","{row["_c20"]}","{row["_c21"]}","{row["_c22"]}","{row["_c23"]}","{row["_c24"]}","{row["_c25"]}","{row["_c26"]}","{row["_c27"]}","{row["_c28"]}","{row["_c29"]}","{row["_c30"]}","{row["_c31"]}","{row["_c32"]}","{row["_c33"]}","{row["_c34"]}","{row["_c35"]}","{row["_c36"]}","{row["_c37"]}","{row["_c38"]}","{row["_c39"]}","{row["_c40"]}","{row["_c41"]}","{row["_c42"]}","{row["_c43"]}","{row["_c44"]}","{row["_c45"]}","{row["_c46"]}","{row["_c47"]}")
                """
                spark.sql(query)

            if row["_c0"] == "D":
                query = f"""
                delete from ficha_corta.madre
                where _c0 = "{row["_c1"]}";
                """
                spark.sql(query)

        df_f.repartition(1).write.mode("overwrite").parquet(
            f"s3://{env}-534086549449-data-raw-2/parquet/ficha_corta/corporativo/madre/test/"
        )

df_f.describe().show()
print("Ha terminado satisfactoriamente")

