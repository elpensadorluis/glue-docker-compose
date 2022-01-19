# integration glue job
# import awswrangler as wr
# import pandas as pd
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
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, BooleanType, LongType, TimestampType
from pyspark.sql.functions import when, lit, col


import logging
logger = logging.getLogger(name="Prebuc Job")
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = SparkSession.builder.appName("purple_prebuc") \
    .config("hive.metastore.client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory") \
    .enableHiveSupport().getOrCreate()
sqlContext = SQLContext(spark)
spark.conf.set("spark.sql.codegen.wholeStage", False)
hadoop_conf = sc._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
hadoop_conf.set("fs.s3a.canned.acl", "BucketOwnerFullControl")
hadoop_conf.set("fs.s3a.acl.default", "BucketOwnerFullControl")


### DEFINICION DE ESQUEMA ###


Schema = StructType([
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("email_contactable", StringType(), True),
        StructField("phone_prefix", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("edad", StringType(), True),
        StructField("date_of_birth", StringType(), True),
        StructField("id_country", StringType(), True),
        StructField("location", StringType(), True),
        StructField("locale", StringType(), True),
        StructField("unsubscribed_at", StringType(), True),
        StructField("unsubscribed", StringType(), True),
        StructField("habeas_data", StringType(), True),
        StructField("facebook_id", StringType(), True),
        StructField("mac", StringType(), True),
        StructField("tipo_documento", StringType(), True),
        StructField("numero_documento", StringType(), True),
        StructField("visit_date", StringType(), True),
        StructField("min_visit_date", StringType(), True),
        StructField("max_visit_date", StringType(), True),
        StructField("tiene_hijos", StringType(), True),
        StructField("patente_vehiculo", StringType(), True),
        StructField("tiene_auto", StringType(), True),
        StructField("localidad", StringType(), True),
        StructField("nacionalidad", StringType(), True),
        StructField("pais", StringType(), True),
        StructField("intereses", StringType(), True),
        StructField("year", StringType(), True),
        StructField("month", StringType(), True),
        StructField("day", StringType(), True),
        StructField("country", StringType(), True)]
        )



### LECTURA DE PURPLE
### PARAMETRIZAR PARA PODER LEER POR DIA ###

dfp=spark.read.schema(Schema).parquet("s3://parauco-analytics-datalake-corp-dev-transformation/enrichment/int/clientes/purple/prebuc-visitors/")
#df1=spark.read.parquet("s3://parauco-analytics-datalake-corp-dev-transformation/enrichment/int/clientes/purple/prebuc-visitors/country=dco/year=2021/month=11/day=01/")
#df2=spark.read.parquet("s3://parauco-analytics-datalake-corp-dev-transformation/enrichment/int/clientes/purple/prebuc-visitors/country=dcl/year=2021/month=11/day=01/")
#df3=spark.read.parquet("s3://parauco-analytics-datalake-corp-dev-transformation/enrichment/int/clientes/purple/prebuc-visitors/country=dpe/year=2021/month=11/day=01/")
#df = df1.unionByName(df2)
#df = df.unionByName(df3)
#df1.show(50, truncate=False)
dfp.createOrReplaceTempView('purple_prebuc')
dfp.printSchema()

#query = f'select distinct id_country from purple_prebuc'
#pdf = spark.sql(query)
#pdf.show(50, truncate=False)


### LECTURA DE QUALTRICS


#dfq=spark.read.option("mergeSchema", "true").parquet("s3://parauco-analytics-datalake-corp-dev-transformation/enrichment/int/clientes/qualtrics/prebuc-visitors/")
dfq=spark.read.schema(Schema).parquet("s3://parauco-analytics-datalake-corp-dev-transformation/enrichment/int/clientes/qualtrics/prebuc-visitors/")
#df1=spark.read.parquet("s3://parauco-analytics-datalake-corp-dev-transformation/enrichment/int/clientes/qualtrics/prebuc-visitors/country=DCO/year=2021/month=11/day=01/")
#df2=spark.read.parquet("s3://parauco-analytics-datalake-corp-dev-transformation/enrichment/int/clientes/qualtrics/prebuc-visitors/country=DCL/year=2021/month=11/day=01/")
#df3=spark.read.parquet("s3://parauco-analytics-datalake-corp-dev-transformation/enrichment/int/clientes/qualtrics/prebuc-visitors/country=DPE/year=2021/month=11/day=01/")
#df = df1.unionByName(df2)
#df = df.unionByName(df3)
dfq.createOrReplaceTempView('qualtrics_prebuc')
dfq.printSchema()
#df2.show(10, truncate=False)



### LECTURA DE SOM

print('SOM no se esta tomando en cuenta\n')
#dfs=spark.read.parquet("s3://parauco-analytics-datalake-corp-dev-transformation/enrichment/int/clientes/som/prebuc-visitors/") #year=2021/month=09/day=05/")
#dfs = dfs.drop("extraction_date")
#dfs = dfs.filter("year = '2021' and month = '11'")
#dfs.createOrReplaceTempView('som_prebuc')
#dfs.printSchema()


### UNION DE DATAFRAMES ###

print('Purple\n')
print(dfp.count())
print('Qualtrics\n')
print(dfq.count())
print('SOM\n')
print(dfs.count())
dff = dfp.unionByName(dfq)
#dff = dff.unionByName(dfs)
dff = dff.na.fill("")
print('Union')
print(dff.count())
#dff = dff.withColumn("visit_date",dff.visit_date.cast('string'))
dff.createOrReplaceTempView('prebuc_unificada1')




######### SEPARACION DE DATAFRAMES #########

### REGISTROS CON EMAIL Y DOCUMENTO ###

query = f"""
         Select first_name
              , last_name
              , email
              , email_contactable
              , phone_prefix
              , phone
              , gender
              , edad
              , date_of_birth
              , id_country
              , location
              , locale
              , unsubscribed_at
              , unsubscribed
              , habeas_data
              , facebook_id
              , mac
              , tipo_documento
              , numero_documento
              , visit_date
              , min_visit_date
              , max_visit_date
              , tiene_hijos
              , patente_vehiculo
              , tiene_auto
              , localidad
              , nacionalidad
              , pais
              , intereses
              , country
              , year
              , month
              , day
                
            from prebuc_unificada1
            where email <> '' and numero_documento <> ''
            """
            
            
pdf = spark.sql(query)
pdf.createOrReplaceTempView('email_docu')
print('Query Email not null y documento not null')
print(pdf.count())
#pdf.show(10, truncate = False)


### REGISTROS CON EMAIL Y DOCUMENTO -- APLANADO ###

query = f"""
         Select first_name
              , last_name
              , max(email) as email
              , max(email_contactable) as email_contactable
              , max(phone_prefix) as phone_prefix
              , max(phone) as phone
              , gender
              , max(edad) as edad
              , max(date_of_birth) as date_of_birth
              , id_country
              , max(location) as location
              , max(locale) as locale
              , max(unsubscribed_at) as unsubscribed_at
              , max(unsubscribed) as unsubscribed
              , max(habeas_data) as habeas_data
              , max(facebook_id) as facebook_id
              , max(mac) as mac
              , max(tipo_documento) as tipo_documento
              , max(numero_documento) as numero_documento
              , visit_date
              , min(min_visit_date) as min_visit_date
              , max(max_visit_date) as max_visit_date
              , max(tiene_hijos) as tiene_hijos
              , max(patente_vehiculo) as patente_vehiculo
              , max(tiene_auto) as tiene_auto
              , max(localidad) as localidad
              , max(nacionalidad) as nacionalidad
              , max(pais) as pais
              , max(intereses) as intereses
              , max(country) as country
              , year
              , month
              , day
                
            from email_docu
            group by first_name
              , last_name
              , gender
              , id_country
              , visit_date
              , year
              , month
              , day
            """
            
            
pdf = spark.sql(query)
pdf.createOrReplaceTempView('email_docu_aplanada')
print('Query Email not null y documento not null APLANADA')
print(pdf.count())





### REGISTROS SIN EMAIL Y CON DOCUMENTO ###

query = f"""
            Select first_name
              , last_name
              , email
              , email_contactable
              , phone_prefix
              , phone
              , gender
              , edad
              , date_of_birth
              , id_country
              , location
              , locale
              , unsubscribed_at
              , unsubscribed
              , habeas_data
              , facebook_id
              , mac
              , tipo_documento
              , numero_documento
              , visit_date
              , min_visit_date
              , max_visit_date
              , tiene_hijos
              , patente_vehiculo
              , tiene_auto
              , localidad
              , nacionalidad
              , pais
              , intereses
              , country
              , year
              , month
              , day
                
            from prebuc_unificada1
            where email = '' and numero_documento <> ''
            """
            
            
pdf = spark.sql(query)
pdf.createOrReplaceTempView('notemail_docu')
print('Query Email null y documento not null')
print(pdf.count())
#pdf.show(30, truncate = False)



### REGISTROS SIN EMAIL Y CON DOCUMENTO -- APLANADO ###

query = f"""
         Select first_name
              , last_name
              , max(email) as email
              , max(email_contactable) as email_contactable
              , max(phone_prefix) as phone_prefix
              , max(phone) as phone
              , gender
              , max(edad) as edad
              , max(date_of_birth) as date_of_birth
              , id_country
              , max(location) as location
              , max(locale) as locale
              , max(unsubscribed_at) as unsubscribed_at
              , max(unsubscribed) as unsubscribed
              , max(habeas_data) as habeas_data
              , max(facebook_id) as facebook_id
              , max(mac) as mac
              , max(tipo_documento) as tipo_documento
              , max(numero_documento) as numero_documento
              , visit_date
              , min(min_visit_date) as min_visit_date
              , max(max_visit_date) as max_visit_date
              , max(tiene_hijos) as tiene_hijos
              , max(patente_vehiculo) as patente_vehiculo
              , max(tiene_auto) as tiene_auto
              , max(localidad) as localidad
              , max(nacionalidad) as nacionalidad
              , max(pais) as pais
              , max(intereses) as intereses
              , max(country) as country
              , year
              , month
              , day
                
            from notemail_docu
            group by first_name
              , last_name
              , gender
              , id_country
              , visit_date
              , year
              , month
              , day
            """
            
            
pdf = spark.sql(query)
pdf.createOrReplaceTempView('notemail_docu_aplanada')
print('Query Email not null y documento not null APLANADA')
print(pdf.count())





### REGISTROS CON EMAIL Y SIN DOCUMENTO ###


query = f"""
            Select first_name
              , last_name
              , email
              , email_contactable
              , phone_prefix
              , phone
              , gender
              , edad
              , date_of_birth
              , id_country
              , location
              , locale
              , unsubscribed_at
              , unsubscribed
              , habeas_data
              , facebook_id
              , mac
              , tipo_documento
              , numero_documento
              , visit_date
              , min_visit_date
              , max_visit_date
              , tiene_hijos
              , patente_vehiculo
              , tiene_auto
              , localidad
              , nacionalidad
              , pais
              , intereses
              , country
              , year
              , month
              , day
                
            from prebuc_unificada1
            where email <> '' and numero_documento = ''
            """
            
            
pdf = spark.sql(query)
pdf.createOrReplaceTempView('email_notdocu')
print('Query Email not null y documento null')
print(pdf.count())
#pdf.show(30, truncate = False)



### REGISTROS CON EMAIL Y SIN DOCUMENTO -- APLANADO ###

query = f"""
         Select first_name
              , last_name
              , max(email) as email
              , max(email_contactable) as email_contactable
              , max(phone_prefix) as phone_prefix
              , max(phone) as phone
              , gender
              , max(edad) as edad
              , max(date_of_birth) as date_of_birth
              , id_country
              , max(location) as location
              , max(locale) as locale
              , max(unsubscribed_at) as unsubscribed_at
              , max(unsubscribed) as unsubscribed
              , max(habeas_data) as habeas_data
              , max(facebook_id) as facebook_id
              , max(mac) as mac
              , max(tipo_documento) as tipo_documento
              , max(numero_documento) as numero_documento
              , visit_date
              , min(min_visit_date) as min_visit_date
              , max(max_visit_date) as max_visit_date
              , max(tiene_hijos) as tiene_hijos
              , max(patente_vehiculo) as patente_vehiculo
              , max(tiene_auto) as tiene_auto
              , max(localidad) as localidad
              , max(nacionalidad) as nacionalidad
              , max(pais) as pais
              , max(intereses) as intereses
              , max(country) as country
              , year
              , month
              , day
                
            from email_notdocu
            group by first_name
              , last_name
              , gender
              , id_country
              --, tipo_documento
              , visit_date
              , year
              , month
              , day
            """
            
            
pdf = spark.sql(query)
pdf.createOrReplaceTempView('email_notdocu_aplanada')
print('Query Email not null y documento null APLANADA')
print(pdf.count())



### JOIN DE DATAFRAMES ###

### DF EMAIL NN Y DOCU NN JOIN DF EMAIL NULL Y DOCU NN ###

query = f"""
            Select a.first_name
                 , a.last_name
                 , a.email
                 , a.email_contactable
                 , a.phone_prefix
                 , a.phone
                 , a.gender
                 , a.edad
                 , a.date_of_birth
                 , a.id_country
                 , a.location
                 , a.locale
                 , a.unsubscribed_at
                 , a.unsubscribed
                 , a.habeas_data
                 , a.facebook_id
                 , a.mac
                 , a.tipo_documento
                 , a.numero_documento
                 , a.visit_date
                 , a.min_visit_date
                 , a.max_visit_date
                 , a.tiene_hijos
                 , a.patente_vehiculo
                 , a.tiene_auto
                 , a.localidad
                 , a.nacionalidad
                 , a.pais
                 , a.intereses
                 , a.country
                 , a.year
                 , a.month
                 , a.day
                 , b.numero_documento as numero_documento_b
                 , b.phone_prefix as phone_prefix_b
                 , b.phone as phone_b
                 , b.edad as edad_b
                 , b.location as location_b
                 , b.locale as locale_b
                 , b.unsubscribed_at as unsubscribed_at_b
                 , b.unsubscribed as unsubscribed_b
                 , b.habeas_data as habeas_data_b
                 , b.facebook_id as facebook_id_b
                 , b.mac as mac_b
                 , b.tiene_hijos as tiene_hijos_b
                 , b.patente_vehiculo as patente_vehiculo_b
                 , b.tiene_auto as tiene_auto_b
                 , b.localidad as localidad_b
                 , b.nacionalidad as nacionalidad_b
                 , b.pais as pais_b
                 , b.intereses as intereses_b
                 
                
            from email_docu_aplanada a
            left join notemail_docu_aplanada b on (a.numero_documento = b.numero_documento)
            """
            
            
pdf = spark.sql(query)
pdf = pdf.na.fill("")


### UPDATES DE REGISTROS QUE CRUZAN###

pdf = pdf.withColumn("phone_prefix",when(col("numero_documento_b").isNotNull() & (col("phone_prefix")==''),col("phone_prefix_b")).otherwise(col("phone_prefix")))
pdf = pdf.withColumn("phone",when(col("numero_documento_b").isNotNull() & (col("phone")==''),col("phone_b")).otherwise(col("phone")))
pdf = pdf.withColumn("edad",when(col("numero_documento_b").isNotNull() & (col("edad")==''),col("edad_b")).otherwise(col("edad")))
pdf = pdf.withColumn("location",when(col("numero_documento_b").isNotNull() & (col("location")==''),col("location_b")).otherwise(col("location")))
pdf = pdf.withColumn("locale",when(col("numero_documento_b").isNotNull() & (col("locale")==''),col("locale_b")).otherwise(col("locale")))
pdf = pdf.withColumn("unsubscribed_at",when(col("numero_documento_b").isNotNull() & (col("unsubscribed_at")==''),col("unsubscribed_at_b")).otherwise(col("unsubscribed_at")))
pdf = pdf.withColumn("unsubscribed",when(col("numero_documento_b").isNotNull() & (col("unsubscribed")==''),col("unsubscribed_b")).otherwise(col("unsubscribed")))
pdf = pdf.withColumn("unsubscribed",when(col("numero_documento_b").isNotNull() & (col("unsubscribed")=='false') & (col("unsubscribed_b")=='true'),col("unsubscribed_b")).otherwise(col("unsubscribed")))
pdf = pdf.withColumn("habeas_data",when(col("numero_documento_b").isNotNull() & (col("habeas_data")==''),col("habeas_data_b")).otherwise(col("habeas_data")))
pdf = pdf.withColumn("habeas_data",when(col("numero_documento_b").isNotNull() & (col("habeas_data")=='false') & (col("habeas_data_b")=='true'),col("habeas_data_b")).otherwise(col("habeas_data")))
pdf = pdf.withColumn("facebook_id",when(col("numero_documento_b").isNotNull() & (col("facebook_id")=='') ,col("facebook_id_b")).otherwise(col("facebook_id")))
pdf = pdf.withColumn("mac",when(col("numero_documento_b").isNotNull() & (col("mac")==''),col("mac_b")).otherwise(col("mac")))
pdf = pdf.withColumn("tiene_hijos",when(col("numero_documento_b").isNotNull() & (col("tiene_hijos")==''),col("tiene_hijos_b")).otherwise(col("tiene_hijos")))
pdf = pdf.withColumn("patente_vehiculo",when(col("numero_documento_b").isNotNull() & (col("patente_vehiculo")==''),col("patente_vehiculo_b")).otherwise(col("patente_vehiculo")))
pdf = pdf.withColumn("tiene_auto",when(col("numero_documento_b").isNotNull() & (col("tiene_auto")==''),col("tiene_auto_b")).otherwise(col("tiene_auto")))
pdf = pdf.withColumn("localidad",when(col("numero_documento_b").isNotNull() & (col("localidad")==''),col("localidad_b")).otherwise(col("localidad")))
pdf = pdf.withColumn("nacionalidad",when(col("numero_documento_b").isNotNull() & (col("nacionalidad")==''),col("nacionalidad_b")).otherwise(col("nacionalidad")))
pdf = pdf.withColumn("pais",when(col("numero_documento_b").isNotNull() & (col("pais")==''),col("pais_b")).otherwise(col("pais")))
pdf = pdf.withColumn("intereses",when(col("numero_documento_b").isNotNull() & (col("intereses")==''),col("intereses_b")).otherwise(col("intereses")))



### DROPS ###

pdf = pdf.drop("phone_prefix_b")
pdf = pdf.drop("phone_b")
pdf = pdf.drop("edad_b")
pdf = pdf.drop("location_b")
pdf = pdf.drop("locale_b")
pdf = pdf.drop("unsubscribed_at_b")
pdf = pdf.drop("unsubscribed_b")
pdf = pdf.drop("habeas_data_b")
pdf = pdf.drop("facebook_id_b")
pdf = pdf.drop("mac_b")
pdf = pdf.drop("tiene_hijos_b")
pdf = pdf.drop("patente_vehiculo_b")
pdf = pdf.drop("tiene_auto_b")
pdf = pdf.drop("localidad_b")
pdf = pdf.drop("nacionalidad_b")
pdf = pdf.drop("pais_b")
pdf = pdf.drop("intereses_b")
pdf = pdf.drop("numero_documento_b")

pdf = pdf.na.fill("")
pdf.createOrReplaceTempView('email_docu_aplanada_j1')

query = f"""
            Select a.first_name
                 , a.last_name
                 , a.email
                 , a.email_contactable
                 , a.phone_prefix
                 , a.phone
                 , a.gender
                 , a.edad
                 , a.date_of_birth
                 , a.id_country
                 , a.location
                 , a.locale
                 , a.unsubscribed_at
                 , a.unsubscribed
                 , a.habeas_data
                 , a.facebook_id
                 , a.mac
                 , a.tipo_documento
                 , a.numero_documento
                 , a.visit_date
                 , a.min_visit_date
                 , a.max_visit_date
                 , a.tiene_hijos
                 , a.patente_vehiculo
                 , a.tiene_auto
                 , a.localidad
                 , a.nacionalidad
                 , a.pais
                 , a.intereses
                 , a.country
                 , a.year
                 , a.month
                 , a.day
                 , b.numero_documento as numero_documento_b
                
            from notemail_docu_aplanada a 
            left join email_docu_aplanada_j1 b on (a.numero_documento = b.numero_documento)
            where b.numero_documento is null
            """
            
            
pdf_notemail_docu = spark.sql(query)
pdf_notemail_docu = pdf_notemail_docu.drop("numero_documento_b")
pdf = pdf.unionByName(pdf_notemail_docu)
integrado_1 = pdf
#pdf.show(30)
pdf.createOrReplaceTempView('integrado_1')
print('tabla integrada')


### INTEGRACION ANTERIOR JOIN DF EMAIL NN Y DOCU NULL ###

query = f"""
            Select a.first_name
                 , a.last_name
                 , a.email
                 , a.email_contactable
                 , a.phone_prefix
                 , a.phone
                 , a.gender
                 , a.edad
                 , a.date_of_birth
                 , a.id_country
                 , a.location
                 , a.locale
                 , a.unsubscribed_at
                 , a.unsubscribed
                 , a.habeas_data
                 , a.facebook_id
                 , a.mac
                 , a.tipo_documento
                 , a.numero_documento
                 , a.visit_date
                 , cast(date(a.min_visit_date) as string) as min_visit_date
                 , cast(date(a.max_visit_date) as string) as max_visit_date
                 , a.tiene_hijos
                 , a.patente_vehiculo
                 , a.tiene_auto
                 , a.localidad
                 , a.nacionalidad
                 , a.pais
                 , a.intereses
                 , a.country
                 , a.year
                 , a.month
                 , a.day
                 , b.email as email_b
                 
                 
                
            from integrado_1 b
            left join email_notdocu_aplanada a on (a.email = b.email)
            where b.email is null
            """
            
            
pdf = spark.sql(query)
pdf = pdf.drop("email_b")
integrado_1 = integrado_1.unionByName(pdf)
integrado_1.createOrReplaceTempView('tabla_final')
#print('test erika leal')
#integrado_1.filter("first_name = 'Erika' and last_name = 'Leal'").show(truncate=False)
#where first_name = 'Erika' and last_name = 'Leal'


query = f"""
            Select a.*
                 , ROW_NUMBER() over (partition by first_name, last_name, email, numero_documento  order by visit_date desc) row_number
                 
                
            from tabla_final a
            """
            
            
final = spark.sql(query)
#print('TEST CON ROWNUMBER')
#final.filter("first_name = 'Juan' and last_name = 'OteroQuintero'").show(truncate=False)
final = final.filter("row_number == 1")
final = final.drop('row_number')
final.createOrReplaceTempView('tabla_final')


### UUID ###

query = f"""
            Select uuid() as id_cliente
                 , a.*
                 
                
            from tabla_final a
            """
            
            
final = spark.sql(query)

print('Registros finales: ')
print(final.count())
print('Esquema final: ')
final.printSchema()


#final.show(20)
#print('test Juan OteroQuintero')
#final.filter("first_name = 'Juan' and last_name = 'OteroQuintero'").show(truncate=False)
#final.filter("phone = '057130899'").show(truncate=False)


#########  ELIMINAR LOS REGISTRO CON EMAIL Y DOCUMENTO NULO  (PANCHO)#########

final = final.filter(~((final["email"]=='') & (final["numero_documento"]=='')))
final.createOrReplaceTempView('tabla_final')


#########  LECTURA DE LA BUC EXISTENTE  ######### 
try:
    buc_customers = spark.read.parquet("s3://parauco-analytics-datalake-corp-dev-transformation/enrichment/int/clientes/buc/dim-clf-buc-customers/")
    buc_email = spark.read.parquet("s3://parauco-analytics-datalake-corp-dev-transformation/enrichment/int/clientes/buc/dim-clf-buc-email/")
    buc_phone = spark.read.parquet("s3://parauco-analytics-datalake-corp-dev-transformation/enrichment/int/clientes/buc/dim-clf-buc-phone/")
    buc_interests = spark.read.parquet("s3://parauco-analytics-datalake-corp-dev-transformation/enrichment/int/clientes/buc/dim-clf-buc-interests/")
    
    buc_customers.createOrReplaceTempView('dim_customer')
    buc_email.createOrReplaceTempView('dim_email')
    buc_phone.createOrReplaceTempView('dim_phone')
    buc_interests.createOrReplaceTempView('dim_interests')
    
    print('SE LEYO LA BUC ANTERIOR')
    print('dim_customer')
    print('dim_email')
    print('dim_phone')
    print('dim_interests')
    
    
    
    #########  SI EXISTE SE HACE JOIN DE TODAS LAS TABLAS EXISTENTES  #########
    
    print('SI EXISTE SE HACE JOIN DE TODAS LAS TABLAS EXISTENTES')
    
    query = f"""
            Select a.client_id
                 , a.first_name
                 , a.last_name
                 , a.gender
                 , a.document_number
                 , a.date_of_birth
                 , a.age
                 , a.location
                 , a.habeas_data
                 , a.min_visit_date
                 , a.max_visit_date
                 , a.has_children
                 , a.car_plate
                 , a.has_car
                 , a.nationality
                 , a.country
                 , a.client_country
                 , a.id_country
                 , a.id_platform
                 , a.creation_date 
                 , a.update_date
                 
                 , b.email
                 , b.reachable_email
                 , b.visit_date
                 , b.unsubscribed_at
                 , b.unsubscribed
                 , b.valid_email
                 
                 , c.phone_prefix
                 , c.phone
                 , c.locale
                 , c.mac
                 , c.facebook_id
                 
                 , d.interests
                 
                
            from dim_customer a 
            left join dim_email b on (a.client_id = b.client_id)
            left join dim_phone c on (a.client_id = c.client_id)
            left join dim_interests d on (a.client_id = d.client_id)
            """
            
            
    buc_ant = spark.sql(query)
    buc_ant = buc_ant.na.fill("")
    buc_ant.createOrReplaceTempView('buc_anterior')
    print('BUC ANTERIOR POST JOIN')
    #buc_ant.show(20)
    
    #########  LEFT JOIN DE LA NUEVA CON LA EXISTENTE, POR EMAIL Y DOCUMENTO, LOS QUE NO CRUCEN SERAN MARCADOS COMO INSERTAR  (PANCHO)#########
    
    print('LEFT JOIN DE LA NUEVA CON LA EXISTENTE, POR EMAIL Y DOCUMENTO, LOS QUE NO CRUCEN SERAN MARCADOS COMO INSERTAR')
    
    query = f"""
            Select a.id_cliente as client_id
                 , a.first_name
                 , a.last_name
                 , a.gender
                 , a.numero_documento as document_number
                 , a.date_of_birth
                 , a.edad as age
                 , a.location
                 , a.habeas_data
                 , a.min_visit_date
                 , a.max_visit_date
                 , a.tiene_hijos as has_children
                 , a.patente_vehiculo as car_plate
                 , a.tiene_auto as has_car
                 , a.nacionalidad as nationality
                 , a.country
                 , a.pais as client_country
                 , a.id_country
                 , 'BUC' as id_platform
                 , current_date() as creation_date
                 , current_date() as update_date
                 , a.email
                 , a.email_contactable as reachable_email
                 , a.visit_date
                 , a.unsubscribed_at
                 , a.unsubscribed
                 , '' as valid_email
                 , a.phone_prefix
                 , a.phone
                 , a.locale
                 , a.mac
                 , a.facebook_id
                 , a.intereses as interests
                 
                 , case when b.document_number is null and b.email is null then 'I' else 'U' end as insert_update_flag
             
            
        from tabla_final  a 
        left join buc_anterior b on (a.numero_documento = b.document_number and a.email = b.email)
            """
            
    temp = spark.sql(query)
    temp = temp.na.fill("")
    insert_1 = temp.filter("insert_update_flag = 'I'") #.show(truncate=False)
    update_1 = temp.filter("insert_update_flag = 'U'") #.show(truncate=False)
    print('BUC nueva vs anterior - Marcados las inserciones email y documento')
    #temp.show(13) # OK
    
    #########  LEFT JOIN DE LA NUEVA CON LA EXISTENTE, POR EMAIL Y DOCUMENTO NULO, LOS QUE NO CRUCEN SERAN MARCADOS COMO INSERTAR  (PANCHO)#########
    
    print('LEFT JOIN DE LA NUEVA CON LA EXISTENTE, POR EMAIL Y DOCUMENTO NULO, LOS QUE NO CRUCEN SERAN MARCADOS COMO INSERTAR') ### REVISAR, SE CAE EN ESTA PARTE

    query = f"""
            Select a.id_cliente as client_id
                 , a.first_name
                 , a.last_name
                 , a.gender
                 , a.numero_documento as document_number
                 , a.date_of_birth
                 , a.edad as age
                 , a.location
                 , a.habeas_data
                 , a.min_visit_date
                 , a.max_visit_date
                 , a.tiene_hijos as has_children
                 , a.patente_vehiculo as car_plate
                 , a.tiene_auto as has_car
                 , a.nacionalidad as nationality
                 , a.country
                 , a.pais as client_country
                 , a.id_country
                 , 'BUC' as id_platform
                 , current_date() as creation_date
                 , current_date() as update_date
                 , a.email
                 , a.email_contactable as reachable_email
                 , a.visit_date
                 , a.unsubscribed_at
                 , a.unsubscribed
                 , '' as valid_email
                 , a.phone_prefix
                 , a.phone
                 , a.locale
                 , a.mac
                 , a.facebook_id
                 , a.intereses as interests
                
                 , case when b.email is null then 'I' else 'U' end as insert_update_flag
             
            
        from tabla_final  a 
        left join buc_anterior b on (a.numero_documento = '' and a.email = b.email)
            """
            
    temp = spark.sql(query)
    temp = temp.na.fill("")
    insert_2 = temp.filter("insert_update_flag = 'I'") #.show(truncate=False)
    ## EL CASO DE LOS DOCUMENTOS EN NULO NO SE HACE DF DE ACTUALIZACIONES
    print('BUC nueva vs anterior - Marcados las inserciones email y documento nulo')
    #temp.show(23)
    
    #########  LEFT JOIN DE LA NUEVA CON LA EXISTENTE, POR EMAIL NULO Y DOCUMENTO, LOS QUE NO CRUCEN SERAN MARCADOS COMO INSERTAR  (PANCHO)#########
    
    print('LEFT JOIN DE LA NUEVA CON LA EXISTENTE, POR EMAIL NULO Y DOCUMENTO, LOS QUE NO CRUCEN SERAN MARCADOS COMO INSERTAR') #OK
    
    query = f"""
            Select a.id_cliente as client_id
                 , a.first_name
                 , a.last_name
                 , a.gender
                 , a.numero_documento as document_number
                 , a.date_of_birth
                 , a.edad as age
                 , a.location
                 , a.habeas_data
                 , a.min_visit_date
                 , a.max_visit_date
                 , a.tiene_hijos as has_children
                 , a.patente_vehiculo as car_plate
                 , a.tiene_auto as has_car
                 , a.nacionalidad as nationality
                 , a.country
                 , a.pais as client_country
                 , a.id_country
                 , 'BUC' as id_platform
                 , current_date() as creation_date
                 , current_date() as update_date
                 , a.email
                 , a.email_contactable as reachable_email
                 , a.visit_date
                 , a.unsubscribed_at
                 , a.unsubscribed
                 , '' as valid_email
                 , a.phone_prefix
                 , a.phone
                 , a.locale
                 , a.mac
                 , a.facebook_id
                 , a.intereses as interests
                 
                 , case when b.document_number is null then 'I' else 'U' end as insert_update_flag
             
            
        from tabla_final  a 
        left join buc_anterior b on (a.email = '' and a.numero_documento = b.document_number)
            """
    print('test post')        
    temp = spark.sql(query)
    temp = temp.na.fill("")
    insert_3 = temp.filter("insert_update_flag = 'I'") #.show(truncate=False)
    update_3 = temp.filter("insert_update_flag = 'U'") #.show(truncate=False)
    print('BUC nueva vs anterior - Marcados las inserciones email nulo y documento')
    #temp.show(27)
    
    ######### UNION DE LOS DF DE INSERCION Y ACTUALIZACION  #########  
    
    print('UNION DE LOS DF DE INSERCION Y ACTUALIZACION')
    
    insert = insert_1.unionByName(insert_2)
    insert = insert.unionByName(insert_3)
    insert.createOrReplaceTempView('buc_actual_insert')
    
    update = update_1.unionByName(update_3)
    update.createOrReplaceTempView('buc_actual_update')
    
    
    #########  LOS REGISTROS NO MARCADOS COMO INSERTAR SE MARCAN COMO ACTUALIZAR (PANCHO) ######### DONE
    
    
    #########  SEPARAR EL DF FINAL EN ACTUALIZAR E INSERTAR (PANCHO) ######### DONE
    
    
    #########  JOIN POR DOCU: SI EL CORREO ANTERIOR ES VACIO Y EL NUEVO NO ES VACIO -> SE ACTUALIZA (PANCHO)#########
    
    print('JOIN POR DOCU: SI EL CORREO ANTERIOR ES VACIO Y EL NUEVO NO ES VACIO -> SE ACTUALIZA')
    
    query = f"""
    Select a.client_id
                 , a.first_name
                 , a.last_name
                 , a.gender
                 , a.document_number
                 , a.date_of_birth
                 , a.age
                 , a.location
                 , a.habeas_data
                 , a.min_visit_date
                 , a.max_visit_date
                 , a.has_children
                 , a.car_plate
                 , a.has_car
                 , a.nationality
                 , a.country
                 , a.client_country
                 , a.id_country
                 , a.id_platform
                 , a.creation_date
                 , a.update_date
                 , a.email
                 , a.reachable_email
                 , a.visit_date
                 , a.unsubscribed_at
                 , a.unsubscribed
                 , a.valid_email
                 , a.phone_prefix
                 , a.phone
                 , a.locale
                 , a.mac
                 , a.facebook_id
                 , a.interests
                 
                 --, b.client_id as client_id_ba
                 --, b.first_name as first_name_ba
                 --, b.last_name as last_name_ba
                 --, b.gender as gender_ba
                 --, b.document_number as document_number_ba
                 --, b.date_of_birth as date_of_birth_ba
                 --, b.age as age_ba
                 --, b.location as location_ba
                 --, b.habeas_data as habeas_data_ba
                 ----, b.min_visit_date
                 ----, b.max_visit_date
                 --, b.has_children as has_children_ba
                 --, b.car_plate as car_plate_ba
                 --, b.has_car as has_car_ba
                 --, b.nationality as nationality_ba
                 --, b.country as country_ba
                 --, b.client_country as client_country_ba
                 --, b.id_country as id_country_ba
                 --, b.id_platform as id_platform_ba
                 --, b.creation_date as creation_date_ba
                 --, b.update_date as update_date_ba
                 , b.email as email_ba
                 --, b.reachable_email as reachable_email_ba
                 --, b.visit_date as visit_date_ba
                 --, b.unsubscribed_at as unsubscribed_at_ba
                 --, b.unsubscribed as unsubscribed_ba
                 --, b.valid_email as valid_email_ba
                 --, b.phone as phone_ba
                 --, b.locale as locale_ba
                 --, b.mac as mac_ba
                 --, b.facebook_id as facebook_id_ba
                 --, b.interests as interests_ba
                 
                 , current_date() as update_date_ba
                 , b.insert_update_flag
             
            
        from buc_anterior a 
        left join buc_actual_update b on (a.document_number = b.document_number)
        """
            
    temp = spark.sql(query)
    temp = temp.na.fill("")
    
    print('BUC nueva vs anterior - Actualizacion de email')
    temp = temp.withColumn("email",when(col("email_ba").isNotNull() & (col("email")==''),col("email_ba")).otherwise(col("email")))
    temp = temp.withColumn("update_date",when(col("email_ba").isNotNull() & (col("email")==''),col("update_date_ba")).otherwise(col("update_date")))
    temp = temp.drop("email_ba")
    temp = temp.drop("insert_update_flag")
    temp.createOrReplaceTempView('buc_anterior')
        
        
        
        
    #########  JOIN POR DOCU: SI EL CORREO ANTERIOR NO ES VACIO Y EL NUEVO NO ES VACIO -> SE MARCA COMO INSERTAR Y SE CONSERVA EL UUID DE LA BUC ANTERIOR  (PANCHO)#########
    
    print('JOIN POR DOCU: SI EL CORREO ANTERIOR NO ES VACIO Y EL NUEVO NO ES VACIO -> SE MARCA COMO INSERTAR Y SE CONSERVA EL UUID DE LA BUC ANTERIOR')
    
    query = f"""
    Select a.client_id
                 , a.first_name
                 , a.last_name
                 , a.gender
                 , a.document_number
                 , a.date_of_birth
                 , a.age
                 , a.location
                 , a.habeas_data
                 , a.min_visit_date
                 , a.max_visit_date
                 , a.has_children
                 , a.car_plate
                 , a.has_car
                 , a.nationality
                 , a.country
                 , a.client_country
                 , a.id_country
                 , a.id_platform
                 , a.creation_date
                 , a.update_date
                 , a.email
                 , a.reachable_email
                 , a.visit_date
                 , a.unsubscribed_at
                 , a.unsubscribed
                 , a.valid_email
                 , a.phone_prefix
                 , a.phone
                 , a.locale
                 , a.mac
                 , a.facebook_id
                 , a.interests
                 
                 ----, b.min_visit_date
                 ----, b.max_visit_date
                 , b.email as email_ba
                 , a.insert_update_flag
             
            
        from buc_actual_update a 
        left join buc_anterior b on (a.document_number = b.document_number)
        """
            
    temp = spark.sql(query)
    temp = temp.na.fill("")
    
    temp = temp.withColumn("insert_update_flag",when((col("email_ba")!='') & (col("email")!=''),'I').otherwise(col("insert_update_flag")))
    temp = temp.drop("email_ba")
    
    ### SE GUARDAN LOS REGISTROS MARCADOS DE NUEVO COMO INSERT ###
    
    print('SE GUARDAN LOS REGISTROS MARCADOS DE NUEVO COMO INSERT')
    insert_4 = temp.filter("insert_update_flag = 'I'") #.show(truncate=False)
    insert = insert.unionByName(insert_4)
    
    #temp = temp.drop("insert_update_flag")
    #temp.createOrReplaceTempView('buc_anterior')
    
    
    
    #########  JOIN SOLO POR CORREO: SE ACTUALIZA EL UNSUBSCRIBED (PANCHO)#########
    
    print('JOIN SOLO POR CORREO: SE ACTUALIZA EL UNSUBSCRIBED')
    
    query = f"""
    Select a.client_id
                 , a.first_name
                 , a.last_name
                 , a.gender
                 , a.document_number
                 , a.date_of_birth
                 , a.age
                 , a.location
                 , a.habeas_data
                 , a.min_visit_date
                 , a.max_visit_date
                 , a.has_children
                 , a.car_plate
                 , a.has_car
                 , a.nationality
                 , a.country
                 , a.client_country
                 , a.id_country
                 , a.id_platform
                 , a.creation_date
                 , a.update_date
                 , a.email
                 , a.reachable_email
                 , a.visit_date
                 , a.unsubscribed_at
                 , a.unsubscribed
                 , a.valid_email
                 , a.phone_prefix
                 , a.phone
                 , a.locale
                 , a.mac
                 , a.facebook_id
                 , a.interests
                 
                 ----, b.min_visit_date
                 ----, b.max_visit_date
                 , b.unsubscribed as unsubscribed_ba
                 , current_date() as update_date_ba
             
            
        from buc_anterior a 
        left join buc_actual_update b on (a.email = b.email)
        """
            
    temp = spark.sql(query)
    temp = temp.na.fill("")
    
    temp = temp.withColumn("unsubscribed",when((col("unsubscribed")=='false') & (col("unsubscribed_ba")=='true'),'I').otherwise(col("unsubscribed")))
    temp = temp.withColumn("update_date",when((col("unsubscribed")=='false') & (col("unsubscribed_ba")=='true'),col("update_date_ba")).otherwise(col("update_date")))
    
    temp = temp.drop("unsubscribed_ba")
    temp = temp.drop("update_date_ba")
    temp.createOrReplaceTempView('buc_anterior')
    
    
    #########  JOIN POR DOCUMENTO: SI EL CORREO ANTERIOR NO ES VACIO Y EL ACTUAL ES VACIO -> SE ACTUALIZAN LOS DATOS (PANCHO)#########
    
    print('JOIN POR DOCUMENTO: SI EL CORREO ANTERIOR NO ES VACIO Y EL ACTUAL ES VACIO -> SE ACTUALIZAN LOS DATOS') ## OK
    
    query = f"""
    Select a.client_id
                 , a.first_name
                 , a.last_name
                 , a.gender
                 , a.document_number
                 , a.date_of_birth
                 , a.age
                 , a.location
                 , a.habeas_data
                 , a.min_visit_date
                 , a.max_visit_date
                 , a.has_children
                 , a.car_plate
                 , a.has_car
                 , a.nationality
                 , a.country
                 , a.client_country
                 , a.id_country
                 , a.id_platform
                 , a.creation_date
                 , a.update_date
                 , a.email
                 , a.reachable_email
                 , a.visit_date
                 , a.unsubscribed_at
                 , a.unsubscribed
                 , a.valid_email
                 , a.phone_prefix
                 , a.phone
                 , a.locale
                 , a.mac
                 , a.facebook_id
                 , a.interests
                 
                 --, b.client_id as client_id_ba
                 , b.first_name as first_name_ba
                 , b.last_name as last_name_ba
                 , b.gender as gender_ba
                 --, b.document_number as document_number_ba
                 , b.date_of_birth as date_of_birth_ba
                 , b.age as age_ba
                 , b.location as location_ba
                 , b.habeas_data as habeas_data_ba
                 , b.min_visit_date as min_visit_date_ba
                 , b.max_visit_date as max_visit_date_ba
                 , b.has_children as has_children_ba
                 , b.car_plate as car_plate_ba
                 , b.has_car as has_car_ba
                 , b.nationality as nationality_ba
                 , b.country as country_ba
                 , b.client_country as client_country_ba
                 , b.id_country as id_country_ba
                 , b.id_platform as id_platform_ba
                 , b.creation_date as creation_date_ba
                 , b.update_date as update_date_ba
                 , b.email as email_ba
                 --, b.reachable_email as reachable_email_ba
                 , b.visit_date as visit_date_ba
                 , b.unsubscribed_at as unsubscribed_at_ba
                 , b.unsubscribed as unsubscribed_ba
                 , b.valid_email as valid_email_ba
                 , b.phone_prefix as phone_prefix_ba
                 , b.phone as phone_ba
                 , b.locale as locale_ba
                 , b.mac as mac_ba
                 , b.facebook_id as facebook_id_ba
                 , b.interests as interests_ba
                 
                 , b.insert_update_flag
                 --, current_date() as update_date_ba
             
            
        from buc_anterior a 
        left join buc_actual_update b on (a.document_number = b.document_number)
        """
            
    temp = spark.sql(query)
    temp = temp.na.fill("")
    
    temp = temp.withColumn("first_name",when((col("first_name")=='') & (col("first_name_ba")!=''),col("first_name_ba")).otherwise(col("first_name")))
    temp = temp.withColumn("last_name",when((col("last_name")=='') & (col("last_name_ba")!=''),col("last_name_ba")).otherwise(col("last_name")))
    temp = temp.withColumn("gender",when((col("gender")=='') & (col("gender_ba")!=''),col("gender_ba")).otherwise(col("gender")))
    temp = temp.withColumn("date_of_birth",when((col("date_of_birth")=='') & (col("date_of_birth_ba")!=''),col("date_of_birth_ba")).otherwise(col("date_of_birth")))
    temp = temp.withColumn("age",when((col("age_ba")!=''),col("age_ba")).otherwise(col("age")))
    temp = temp.withColumn("location",when((col("location_ba")!=''),col("location_ba")).otherwise(col("location")))
    temp = temp.withColumn("habeas_data",when((col("habeas_data_ba")!=''),col("habeas_data_ba")).otherwise(col("habeas_data")))
    temp = temp.withColumn("has_children",when((col("has_children_ba")!=''),col("has_children_ba")).otherwise(col("has_children")))
    temp = temp.withColumn("car_plate",when((col("car_plate_ba")!=''),col("car_plate_ba")).otherwise(col("car_plate")))
    temp = temp.withColumn("has_car",when((col("has_car_ba")!=''),col("has_car_ba")).otherwise(col("has_car")))
    temp = temp.withColumn("nationality",when((col("nationality")=='') & (col("nationality_ba")!=''),col("nationality_ba")).otherwise(col("nationality")))
    temp = temp.withColumn("country",when((col("country")=='') & (col("country_ba")!=''),col("country_ba")).otherwise(col("country")))
    temp = temp.withColumn("client_country",when((col("client_country_ba")!=''),col("client_country_ba")).otherwise(col("client_country")))
    temp = temp.withColumn("unsubscribed_at",when((col("unsubscribed_at")=='') & (col("unsubscribed_at_ba")!=''),col("unsubscribed_at_ba")).otherwise(col("unsubscribed_at")))
    temp = temp.withColumn("unsubscribed",when((col("unsubscribed")=='false') & (col("unsubscribed_ba")=='true'),col("unsubscribed_ba")).otherwise(col("unsubscribed")))
    temp = temp.withColumn("phone_prefix",when((col("phone_prefix_ba")!=''),col("phone_prefix_ba")).otherwise(col("phone_prefix")))
    temp = temp.withColumn("phone",when((col("phone")=='') & (col("phone_ba")!=''),col("phone_ba")).otherwise(col("phone")))
    temp = temp.withColumn("locale",when((col("locale")=='') & (col("locale_ba")!=''),col("locale_ba")).otherwise(col("locale")))
    temp = temp.withColumn("mac",when((col("mac_ba")!=''),col("mac_ba")).otherwise(col("mac")))
    temp = temp.withColumn("visit_date",when((col("visit_date_ba")!=''),col("visit_date_ba")).otherwise(col("visit_date")))
    temp = temp.withColumn("facebook_id",when((col("facebook_id_ba")!=''),col("facebook_id_ba")).otherwise(col("facebook_id")))
    temp = temp.withColumn("interests",when((col("interests_ba")!=''),col("interests_ba")).otherwise(col("interests")))
    temp = temp.withColumn("min_visit_date",when((col("min_visit_date_ba")<col('min_visit_date')),col("min_visit_date_ba")).otherwise(col("min_visit_date")))
    temp = temp.withColumn("max_visit_date",when((col("max_visit_date_ba")>col('max_visit_date')),col("max_visit_date_ba")).otherwise(col("max_visit_date")))
    temp = temp.withColumn("update_date",when((col("update_date_ba")!=''),col("update_date_ba")).otherwise(col("update_date")))
    
    temp = temp.drop("first_name_ba")
    temp = temp.drop("last_name_ba")
    temp = temp.drop("gender_ba")
    temp = temp.drop("date_of_birth_ba")
    temp = temp.drop("location_ba")
    temp = temp.drop("age_ba")
    temp = temp.drop("habeas_data_ba")
    temp = temp.drop("has_children_ba")
    temp = temp.drop("has_car_ba")
    temp = temp.drop("nationality_ba")
    temp = temp.drop("country_ba")
    temp = temp.drop("client_country_ba")
    temp = temp.drop("unsubscribed_at_ba")
    temp = temp.drop("unsubscribed_ba")
    temp = temp.drop("phone_prefix_ba")
    temp = temp.drop("phone_ba")
    temp = temp.drop("locale_ba")
    temp = temp.drop("mac_ba")
    temp = temp.drop("visit_date_ba")
    temp = temp.drop("facebook_id_ba")
    temp = temp.drop("interests_ba")
    temp = temp.drop("update_date_ba")
    temp = temp.drop("id_country_ba")
    temp = temp.drop("id_platform_ba")
    temp = temp.drop("creation_date_ba")
    temp = temp.drop("email_ba")
    temp = temp.drop("valid_email_ba")
    
    #print('fin updates deletes')
    
    temp.createOrReplaceTempView('buc_anterior')
    
    
    #########  JOIN POR DOCUMENTO Y CORREO: SE ACTUALIZAN LOS DATOS (PANCHO)#########
    
    print('JOIN POR DOCUMENTO Y CORREO: SE ACTUALIZAN LOS DATOS') ## OK
    
    query = f"""
    Select a.client_id
                 , a.first_name
                 , a.last_name
                 , a.gender
                 , a.document_number
                 , a.date_of_birth
                 , a.age
                 , a.location
                 , a.habeas_data
                 , a.min_visit_date
                 , a.max_visit_date
                 , a.has_children
                 , a.car_plate
                 , a.has_car
                 , a.nationality
                 , a.country
                 , a.client_country
                 , a.id_country
                 , a.id_platform
                 , a.creation_date
                 , a.update_date
                 , a.email
                 , a.reachable_email
                 , a.visit_date
                 , a.unsubscribed_at
                 , a.unsubscribed
                 , a.valid_email
                 , a.phone_prefix
                 , a.phone
                 , a.locale
                 , a.mac
                 , a.facebook_id
                 , a.interests
                 
                 --, b.client_id as client_id_ba
                 , b.first_name as first_name_ba
                 , b.last_name as last_name_ba
                 , b.gender as gender_ba
                 --, b.document_number as document_number_ba
                 , b.date_of_birth as date_of_birth_ba
                 , b.age as age_ba
                 , b.location as location_ba
                 , b.habeas_data as habeas_data_ba
                 , b.min_visit_date as min_visit_date_ba
                 , b.max_visit_date as max_visit_date_ba
                 , b.has_children as has_children_ba
                 , b.car_plate as car_plate_ba
                 , b.has_car as has_car_ba
                 , b.nationality as nationality_ba
                 , b.country as country_ba
                 , b.client_country as client_country_ba
                 , b.id_country as id_country_ba
                 , b.id_platform as id_platform_ba
                 , b.creation_date as creation_date_ba
                 , b.update_date as update_date_ba
                 , b.email as email_ba
                 --, b.reachable_email as reachable_email_ba
                 , b.visit_date as visit_date_ba
                 , b.unsubscribed_at as unsubscribed_at_ba
                 , b.unsubscribed as unsubscribed_ba
                 , b.valid_email as valid_email_ba
                 , b.phone_prefix as phone_prefix_ba
                 , b.phone as phone_ba
                 , b.locale as locale_ba
                 , b.mac as mac_ba
                 , b.facebook_id as facebook_id_ba
                 , b.interests as interests_ba
                 
                 , b.insert_update_flag
                 --, current_date() as update_date_ba
             
            
        from buc_anterior a 
        left join buc_actual_update b on (a.document_number = b.document_number and a.email = b.email)
        """
            
    temp = spark.sql(query)
    temp = temp.na.fill("")
    
    temp = temp.withColumn("first_name",when((col("first_name")=='') & (col("first_name_ba")!=''),col("first_name_ba")).otherwise(col("first_name")))
    temp = temp.withColumn("last_name",when((col("last_name")=='') & (col("last_name_ba")!=''),col("last_name_ba")).otherwise(col("last_name")))
    temp = temp.withColumn("gender",when((col("gender")=='') & (col("gender_ba")!=''),col("gender_ba")).otherwise(col("gender")))
    temp = temp.withColumn("date_of_birth",when((col("date_of_birth")=='') & (col("date_of_birth_ba")!=''),col("date_of_birth_ba")).otherwise(col("date_of_birth")))
    temp = temp.withColumn("age",when((col("age_ba")!=''),col("age_ba")).otherwise(col("age")))
    temp = temp.withColumn("location",when((col("location_ba")!=''),col("location_ba")).otherwise(col("location")))
    temp = temp.withColumn("habeas_data",when((col("habeas_data_ba")!=''),col("habeas_data_ba")).otherwise(col("habeas_data")))
    temp = temp.withColumn("has_children",when((col("has_children_ba")!=''),col("has_children_ba")).otherwise(col("has_children")))
    temp = temp.withColumn("car_plate",when((col("car_plate_ba")!=''),col("car_plate_ba")).otherwise(col("car_plate")))
    temp = temp.withColumn("has_car",when((col("has_car_ba")!=''),col("has_car_ba")).otherwise(col("has_car")))
    temp = temp.withColumn("nationality",when((col("nationality")=='') & (col("nationality_ba")!=''),col("nationality_ba")).otherwise(col("nationality")))
    temp = temp.withColumn("country",when((col("country")=='') & (col("country_ba")!=''),col("country_ba")).otherwise(col("country")))
    temp = temp.withColumn("client_country",when((col("client_country_ba")!=''),col("client_country_ba")).otherwise(col("client_country")))
    temp = temp.withColumn("unsubscribed_at",when((col("unsubscribed_at")=='') & (col("unsubscribed_at_ba")!=''),col("unsubscribed_at_ba")).otherwise(col("unsubscribed_at")))
    temp = temp.withColumn("unsubscribed",when((col("unsubscribed")=='false') & (col("unsubscribed_ba")=='true'),col("unsubscribed_ba")).otherwise(col("unsubscribed")))
    temp = temp.withColumn("phone_prefix",when((col("phone_prefix_ba")!=''),col("phone_prefix_ba")).otherwise(col("phone_prefix")))
    temp = temp.withColumn("phone",when((col("phone")=='') & (col("phone_ba")!=''),col("phone_ba")).otherwise(col("phone")))
    temp = temp.withColumn("locale",when((col("locale")=='') & (col("locale_ba")!=''),col("locale_ba")).otherwise(col("locale")))
    temp = temp.withColumn("mac",when((col("mac_ba")!=''),col("mac_ba")).otherwise(col("mac")))
    temp = temp.withColumn("visit_date",when((col("visit_date_ba")!=''),col("visit_date_ba")).otherwise(col("visit_date")))
    temp = temp.withColumn("facebook_id",when((col("facebook_id_ba")!=''),col("facebook_id_ba")).otherwise(col("facebook_id")))
    temp = temp.withColumn("interests",when((col("interests_ba")!=''),col("interests_ba")).otherwise(col("interests")))
    temp = temp.withColumn("min_visit_date",when((col("min_visit_date_ba")<col('min_visit_date')),col("min_visit_date_ba")).otherwise(col("min_visit_date")))
    temp = temp.withColumn("max_visit_date",when((col("max_visit_date_ba")>col('max_visit_date')),col("max_visit_date_ba")).otherwise(col("max_visit_date")))
    temp = temp.withColumn("update_date",when((col("update_date_ba")!=''),col("update_date_ba")).otherwise(col("update_date")))
    
    temp = temp.drop("first_name_ba")
    temp = temp.drop("last_name_ba")
    temp = temp.drop("gender_ba")
    temp = temp.drop("date_of_birth_ba")
    temp = temp.drop("location_ba")
    temp = temp.drop("age_ba")
    temp = temp.drop("habeas_data_ba")
    temp = temp.drop("has_children_ba")
    temp = temp.drop("has_car_ba")
    temp = temp.drop("nationality_ba")
    temp = temp.drop("country_ba")
    temp = temp.drop("client_country_ba")
    temp = temp.drop("unsubscribed_at_ba")
    temp = temp.drop("unsubscribed_ba")
    temp = temp.drop("phone_prefix_ba")
    temp = temp.drop("phone_ba")
    temp = temp.drop("locale_ba")
    temp = temp.drop("mac_ba")
    temp = temp.drop("visit_date_ba")
    temp = temp.drop("facebook_id_ba")
    temp = temp.drop("interests_ba")
    temp = temp.drop("update_date_ba")
    temp = temp.drop("id_country_ba")
    temp = temp.drop("id_platform_ba")
    temp = temp.drop("creation_date_ba")
    temp = temp.drop("email_ba")
    temp = temp.drop("valid_email_ba")
    
    #print('fin updates deletes')
    
    temp.createOrReplaceTempView('buc_anterior')
    
    #########  JOIN POR DOCU: SI EL PHONE ANTERIOR ES VACIO Y EL NUEVO NO ES VACIO -> SE ACTUALIZA (PANCHO)#########
    
    print('JOIN POR DOCU: SI EL PHONE ANTERIOR ES VACIO Y EL NUEVO NO ES VACIO -> SE ACTUALIZA')
    
    query = f"""
    Select a.client_id
                 , a.first_name
                 , a.last_name
                 , a.gender
                 , a.document_number
                 , a.date_of_birth
                 , a.age
                 , a.location
                 , a.habeas_data
                 , a.min_visit_date
                 , a.max_visit_date
                 , a.has_children
                 , a.car_plate
                 , a.has_car
                 , a.nationality
                 , a.country
                 , a.client_country
                 , a.id_country
                 , a.id_platform
                 , a.creation_date
                 , a.update_date
                 , a.email
                 , a.reachable_email
                 , a.visit_date
                 , a.unsubscribed_at
                 , a.unsubscribed
                 , a.valid_email
                 , a.phone_prefix
                 , a.phone
                 , a.locale
                 , a.mac
                 , a.facebook_id
                 , a.interests
                 
                 
                 , b.update_date as update_date_ba
                 , b.phone as phone_ba
                 , b.insert_update_flag
             
            
        from buc_anterior a 
        left join buc_actual_update b on (a.document_number = b.document_number)
        """
            
    temp = spark.sql(query)
    temp = temp.na.fill("")
    
    temp = temp.withColumn("phone",when((col("phone")=='') & (col("phone_ba")!=''),col("phone_ba")).otherwise(col("phone")))
    temp = temp.withColumn("update_date",when((col("update_date_ba")!=''),col("update_date_ba")).otherwise(col("update_date")))
    
    temp = temp.drop("phone_ba")
    temp = temp.drop("update_date_ba")
    temp = temp.drop("insert_update_flag")
    final_preinsert = temp
    
    print('Esquema final post updates')
    temp.printSchema()
    
    temp.createOrReplaceTempView('buc_anterior')
    
    #########  JOIN POR DOCU: SI EL PHONE ANTERIOR NO ES VACIO Y EL NUEVO NO ES VACIO PERO ES DISTINTO -> SE MARCA COMO INSERTAR (PANCHO)#########
    
    print('JOIN POR DOCU: SI EL PHONE ANTERIOR NO ES VACIO Y EL NUEVO NO ES VACIO PERO ES DISTINTO -> SE MARCA COMO INSERTAR')
    
    query = f"""
    Select a.client_id
         , a.first_name
         , a.last_name
         , a.gender
         , a.document_number
         , a.date_of_birth
         , a.age
         , a.location
         , a.habeas_data
         , a.min_visit_date
         , a.max_visit_date
         , a.has_children
         , a.car_plate
         , a.has_car
         , a.nationality
         , a.country
         , a.client_country
         , a.id_country
         , a.id_platform
         , a.creation_date
         , a.update_date
         , a.email
         , a.reachable_email
         , a.visit_date
         , a.unsubscribed_at
         , a.unsubscribed
         , a.valid_email
         , a.phone_prefix
         , a.phone
         , a.locale
         , a.mac
         , a.facebook_id
         , a.interests
                 
         , b.phone as phone_ba
         , a.insert_update_flag
             
            
        from buc_actual_update a 
        left join buc_anterior b on (a.document_number = b.document_number)
        """
            
    temp = spark.sql(query)
    temp = temp.na.fill("")
    
    temp = temp.withColumn("insert_update_flag",when((col("phone")!='') & (col("phone_ba")!=''),'I').otherwise(col("phone")))
    
    temp = temp.drop("phone_ba")
    
    insert_5 = temp.filter("insert_update_flag = 'I'") #.show(truncate=False)
    insert = insert.unionByName(insert_5)
    insert = insert.drop("insert_update_flag")
    
    print('fin ultima seleccion de inserts')
    
    final_postinsert = final_preinsert.unionByName(insert)
    
    print('Esquema final post insert')
    final_postinsert.printSchema()
    #print(final_postinsert.show())
    
    final_postinsert.createOrReplaceTempView('buc_anterior')
    
    ##############  FORMATO DE CAMPOS FINALES  ##############  
    
    print('FORMATO DE CAMPOS FINALES')
    query = f"""
    Select a.client_id
         , a.first_name
         , a.last_name
         , a.gender
         , a.document_number
         , a.date_of_birth
         , case when a.age <> '' then a.age else cast(cast(round(datediff(current_date(), date_of_birth)/365,0) as integer) as string) end as age
         , a.location
         , a.habeas_data
         , cast(date(a.min_visit_date) as string) as min_visit_date
         , cast(date(a.max_visit_date) as string) as max_visit_date
         , case when a.has_children in ('Si','si') then 'si' else 'no' end as has_children
         , a.car_plate
         , case when a.has_car in ('Si','si') then 'si' else 'no' end as has_car
         , a.nationality
         , a.country
         , a.client_country
         , a.id_country
         , a.id_platform
         , a.creation_date
         , a.update_date
         , a.email
         , a.reachable_email
         , a.visit_date
         , a.unsubscribed_at
         , a.unsubscribed
         , a.valid_email
         , a.phone_prefix
         , a.phone
         , a.locale
         , a.mac
         , a.facebook_id
         , a.interests
         
         from buc_anterior a 
                 
        """
    final_postinsert = spark.sql(query)
    final_postinsert.createOrReplaceTempView('buc_final')
    #final_postinsert.show(10)
    
    #########  DIVISION DE DATAFRAME FINAL EN TABLAS PARA ESCRITURA (PANCHO)#########
    
    print('DIVISION DE DATAFRAME FINAL EN TABLAS PARA ESCRITURA -- FINAL')
    
    ### dim_clf_buc_customers ###
    
    query = f"""
            Select client_id
                 , first_name
                 , last_name
                 , gender
                 --, tipo_documento
                 , hash(document_number) as document_number_hash
                 , document_number
                 , date_of_birth
                 , age
                 , location
                 , habeas_data
                 , min_visit_date
                 , max_visit_date
                 , has_children
                 , car_plate
                 , has_car
                 --, localidad as location
                 , nationality
                 , client_country
                 , id_country
                 , country
                 , id_platform
                 , creation_date
                 , update_date
               
                 
                
            from buc_final
            group by client_id
                 , first_name
                 , last_name
                 , gender
                 , document_number
                 , date_of_birth
                 , age
                 , location
                 , habeas_data
                 , min_visit_date
                 , max_visit_date
                 , has_children
                 , car_plate
                 , has_car
                 , nationality
                 , client_country
                 , id_country
                 , country
                 , id_platform
                 , creation_date
                 , update_date
            order by first_name, last_name
            """
            
            
    buc_customers = spark.sql(query)
    print('dim customers final')
    buc_customers.show(5)
    ### PRUEBA DE DUPLICIDAD DE CLIENTES ###
    #x = buc_customers.groupBy("first_name" , "last_name" , "numero_documento").count()
    #x.filter("count > 1").show(truncate=False)
    
    
    
    
    ### dim_clf_buc_email ###
    
    query = f"""
            Select client_id
                 , hash(email) as email_hash
                 , email
                 , reachable_email
                 , visit_date
                 , unsubscribed_at
                 , unsubscribed
                 , valid_email
                 , id_country
                 , 'BUC' as id_platform
                 , creation_date
                 , update_date
                 , ROW_NUMBER() over (partition by email  order by visit_date desc) row_number
                 
                 
                
            from buc_final
            """
            
    buc_email = spark.sql(query)
    buc_email = buc_email.filter("row_number == 1")
    buc_email = buc_email.drop('row_number')
    ### PRUEBA DE DUPLICIDAD DE CLIENTES ###
    #x = buc_email.groupBy("email").count().show(truncate=False)
    #x.filter("count > 1").show(truncate=False)
    print('dim email')
    buc_email.show(5)



    ### dim_clf_buc_phone ###
    
    query = f"""
            Select client_id
                 , phone_prefix
                 , phone
                 , visit_date
                 , locale
                 , mac
                 , facebook_id
                 , id_platform
                 , creation_date --fecha_creacion
                 , update_date --fecha_actualizacion
                 , ROW_NUMBER() over (partition by phone  order by visit_date desc) row_number
                 
                 
                
            from buc_final
            """
            
    buc_phone = spark.sql(query)
    buc_phone = buc_phone.filter("row_number == 1")
    buc_phone = buc_phone.drop('row_number')
    buc_phone = buc_phone.drop('visit_date')
    ### PRUEBA DE DUPLICIDAD DE CLIENTES ###
    #x = buc_phone.groupBy("phone").count().show(truncate=False)
    #x.filter("count > 1").show(truncate=False)
    print('dim phone final')
    buc_phone.show(5)
    
    
    
    ### dim_clf_buc_interests ###
    
    query = f"""
            Select client_id
                 , interests
                 , id_platform
                 , creation_date --fecha_creacion
                 , update_date --fecha_actualizacion
                 
                 
                
            from buc_final
            """
            
    buc_interests = spark.sql(query)
    ### PRUEBA DE DUPLICIDAD DE CLIENTES ###
    #x = buc_interests.groupBy("intereses").count().show(truncate=False)
    #x.filter("count > 1").show(truncate=False)
    print('dim interests')
    buc_interests.show(5)
    
    
    print('FIN CREACION DIMENSIONES')
    
    
except:
    ##############################################################################################################################################
    
    ##############################################################################################################################################
    
    
    #########  SI NO EXISTE SE DIVIDE LA NUEVA EN TABLAS Y SE ESCRIBE EN S3  (PANCHO)#########   
    print('NUEVA BUC')
    final.createOrReplaceTempView('tabla_final')
    
    ### dim_clf_buc_customers ###
    
    query = f"""
            Select id_cliente as client_id
                 , first_name
                 , last_name
                 , gender
                 --, tipo_documento
                 , hash(numero_documento) as document_number_hash
                 , numero_documento as document_number
                 , date_of_birth
                 , case when edad <> '' then edad else cast(cast(round((datediff(current_date(), date_of_birth))/365,0) as integer) as string) end as age
                 , location
                 , habeas_data
                 , cast(date(min_visit_date) as string) as min_visit_date
                 , cast(date(max_visit_date) as string) as max_visit_date
                 , case when tiene_hijos = 'Si' then 'si' else 'no' end as has_children
                 , patente_vehiculo as car_plate
                 , case when tiene_auto = 'Si' then 'si' else 'no' end as has_car
                 --, localidad as location
                 , nacionalidad as nationality
                 , pais as client_country
                 , id_country
                 , country
                 , 'BUC' as id_platform
                 , current_date() as creation_date --fecha_creacion
                 , current_date() as update_date --fecha_actualizacion
               
                 
                
            from tabla_final
            group by id_cliente
                 , first_name
                 , last_name
                 , gender
                 , numero_documento
                 , date_of_birth
                 , edad
                 , location
                 , habeas_data
                 , min_visit_date
                 , max_visit_date
                 , tiene_hijos
                 , patente_vehiculo
                 , tiene_auto
                 , nacionalidad
                 , pais
                 , id_country
                 , country
            order by first_name, last_name
            """
            
            
    buc_customers = spark.sql(query)
    print('dim customers')
    print(buc_customers.count())
    ### PRUEBA DE DUPLICIDAD DE CLIENTES ###
    #x = buc_customers.groupBy("first_name" , "last_name" , "numero_documento").count()
    #x.filter("count > 1").show(truncate=False)
    
    
    
    
    ### dim_clf_buc_email ###
    
    query = f"""
            Select id_cliente as client_id
                 , email
                 , email_contactable as reachable_email
                 , visit_date
                 , unsubscribed_at
                 , unsubscribed
                 , '' as valid_email
                 , id_country
                 , 'BUC' as id_platform
                 , current_date() as creation_date --fecha_creacion
                 , current_date() as update_date --fecha_actualizacion
                 , ROW_NUMBER() over (partition by email  order by visit_date desc) row_number
                 
                 
                
            from tabla_final
            """
            
    buc_email = spark.sql(query)
    buc_email = buc_email.filter("row_number == 1")
    buc_email = buc_email.drop('row_number')
    ### PRUEBA DE DUPLICIDAD DE CLIENTES ###
    #x = buc_email.groupBy("email").count().show(truncate=False)
    #x.filter("count > 1").show(truncate=False)
    print('dim email')
    #print(buc_email.show())



    ### dim_clf_buc_phone ###
    
    query = f"""
            Select id_cliente as client_id
                 , phone_prefix
                 , phone
                 , visit_date
                 , locale
                 , mac
                 , facebook_id
                 , 'BUC' as id_platform
                 , current_date() as creation_date --fecha_creacion
                 , current_date() as update_date --fecha_actualizacion
                 , ROW_NUMBER() over (partition by phone  order by visit_date desc) row_number
                 
                 
                
            from tabla_final
            """
            
    buc_phone = spark.sql(query)
    buc_phone = buc_phone.filter("row_number == 1")
    buc_phone = buc_phone.drop('row_number')
    buc_phone = buc_phone.drop('visit_date')
    ### PRUEBA DE DUPLICIDAD DE CLIENTES ###
    #x = buc_phone.groupBy("phone").count().show(truncate=False)
    #x.filter("count > 1").show(truncate=False)
    print('dim phone')
    print(buc_phone.count())
    
    
    
    ### dim_clf_buc_interests ###
    
    query = f"""
            Select id_cliente as client_id
                 , intereses as interests
                 , 'BUC' as id_platform
                 , current_date() as creation_date --fecha_creacion
                 , current_date() as update_date --fecha_actualizacion
                 
                 
                
            from tabla_final
            """
            
    buc_interests = spark.sql(query)
    ### PRUEBA DE DUPLICIDAD DE CLIENTES ###
    #x = buc_interests.groupBy("intereses").count().show(truncate=False)
    #x.filter("count > 1").show(truncate=False)
    print('dim interests')
    print(buc_interests.count())

    

#########  ESCRITURA  #########

print('ESCRITURA TEMPORAL')

buc_customers.write.mode('overwrite').parquet('s3://parauco-analytics-datalake-corp-dev-transformation/enrichment/int/clientes/buc/dim-clf-buc-customers-temporal/')
buc_email.write.mode('overwrite').parquet('s3://parauco-analytics-datalake-corp-dev-transformation/enrichment/int/clientes/buc/dim-clf-buc-email-temporal/')
buc_phone.write.mode('overwrite').parquet('s3://parauco-analytics-datalake-corp-dev-transformation/enrichment/int/clientes/buc/dim-clf-buc-phone-temporal/')
buc_interests.write.mode('overwrite').parquet('s3://parauco-analytics-datalake-corp-dev-transformation/enrichment/int/clientes/buc/dim-clf-buc-interests-temporal/')

#########  LECTURA DE TEMPORAL  #########

print('LECTURA TEMPORAL')

#buc_customers = spark.read.parquet("s3://parauco-analytics-datalake-corp-dev-transformation/enrichment/int/clientes/buc/dim-clf-buc-customers-temporal/")
#buc_email = spark.read.parquet("s3://parauco-analytics-datalake-corp-dev-transformation/enrichment/int/clientes/buc/dim-clf-buc-email-temporal/")
#buc_phone = spark.read.parquet("s3://parauco-analytics-datalake-corp-dev-transformation/enrichment/int/clientes/buc/dim-clf-buc-phone-temporal/")
#buc_interests = spark.read.parquet("s3://parauco-analytics-datalake-corp-dev-transformation/enrichment/int/clientes/buc/dim-clf-buc-interests-temporal/")


print('ESCRITURA FINAL')

#buc_customers.write.mode('overwrite').parquet('s3://parauco-analytics-datalake-corp-dev-transformation/enrichment/int/clientes/buc/dim-clf-buc-customers/')
#buc_email.write.mode('overwrite').parquet('s3://parauco-analytics-datalake-corp-dev-transformation/enrichment/int/clientes/buc/dim-clf-buc-email/')
#buc_phone.write.mode('overwrite').parquet('s3://parauco-analytics-datalake-corp-dev-transformation/enrichment/int/clientes/buc/dim-clf-buc-phone/')
#buc_interests.write.mode('overwrite').parquet('s3://parauco-analytics-datalake-corp-dev-transformation/enrichment/int/clientes/buc/dim-clf-buc-interests/')

