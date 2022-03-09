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

### Inicializacion de variables ###

ACCOUNT_ID = "534086549449"
stage = ["data-raw", "data-raw-2"]
prefix = "parquet/ficha_corta/corporativo/madre/"
object = "ficha_corta/corporativo/madre/LOAD00000001.csv"
source = "s3://prod" + "-" + ACCOUNT_ID + "-" + stage[0] + "/"
target = "s3://dev" + "-" + ACCOUNT_ID + "-" + stage[1] + "/" + prefix + "full/"

### codigo ###
# Read Source
print("Reading Source")
inputDF = spark.read.format("csv").option("header", "false").load(source + object)
print("Reading Source Finished")
# Write data as a DELTA TABLE
print("Writing data as a DELTA TABLE")
inputDF.write.format("delta").mode("overwrite").save(target)
print("Writing data as a DELTA TABLE Finished")
# Generate MANIFEST file for Athena/Catalog
print("Generating MANIFEST file for Athena/Catalog")
deltaTable = DeltaTable.forPath(spark, target)
deltaTable.generate("symlink_format_manifest")
print("Generating MANIFEST file for Athena/Catalog Finished")
print("Finished")
