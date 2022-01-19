import boto3
import pyspark
import pyspark.sql as sql
import sys
import argparse

from datetime import datetime
from boto3.dynamodb.conditions import Key
from pyspark.sql import functions as f
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, lit, col
from pyspark.sql.types import DateType



spark = SparkSession.builder.appName("procuidado") \
.config("hive.metastore.client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory") \
.enableHiveSupport().getOrCreate()

spark.conf.set("spark.sql.autoBroadcastJoinThreshold",250485760) 
spark.conf.set("spark.sql.inMemoryColumnarStorage.compressed",True)
spark.conf.set("spark.sql.shuffle.partitions",200)

dynamodb =  boto3.resource('dynamodb',region_name='us-east-1')

# Preparing Spark Context
conf = pyspark.SparkConf()

sc = pyspark.SparkContext.getOrCreate(conf=conf)
spark = SparkSession.builder.getOrCreate()
sqlContext = SQLContext(sc)

#System Manager
ssm = boto3.client('ssm', region_name='us-east-1')


### MODO CLUSTER ###

print("Corriendo en modo Cluster...")

parser = argparse.ArgumentParser(add_help=False)

parser.add_argument('-f', '--fecha')
parser.add_argument('-g', '--glue')
parser.add_argument("-h", "--host")
parser.add_argument("-p", "--puerto")
parser.add_argument("-b", "--bd")
parser.add_argument("-u", "--usuario")
parser.add_argument("-c", "--contrasena")

args = parser.parse_args()
args = vars(args)

if not args["glue"] or not args["host"] or not args["puerto"] or not args["bd"] or not args["usuario"] or not args["contrasena"]:
    raise Exception("Error: Parametros incompletos")


database = args["glue"]
dia = args["fecha"] or  datetime.strftime(datetime.now(), '%Y-%m-%d')
mes = (dia[0:4]+dia[5:7])
dia = datetime.strptime(dia, '%Y-%m-%d')
mes_filter = dia.month
anio_filter = dia.year
dia_filter = dia.day
redshift_host = args["host"]
redshift_port = args["puerto"]
redshift_db = args["bd"]
param_user = args["usuario"]
param_pass = args["contrasena"]


redshift_user = ssm.get_parameter(Name=param_user, WithDecryption=False)['Parameter']['Value']
redshift_pass = ssm.get_parameter(Name=param_pass, WithDecryption=True)['Parameter']['Value']

jdbc_conn_url = f'jdbc:redshift://{redshift_host}:{redshift_port}/{redshift_db}'



puh_df = sqlContext.read \
    .format("jdbc") \
    .option("driver", "com.amazon.redshift.jdbc42.Driver") \
    .option("url", jdbc_conn_url) \
    .option("user", redshift_user) \
    .option("password", redshift_pass) \
    .option("query", f"select * from ventas.producto_unificado_historico where date(fecha_timestamp) = '{dia}' ") \
    .load()
puh_df.createOrReplaceTempView('producto_unificado_historico')

#puh_df.count()

#CARGA DE TABLAS MANUALES DESDE REDSHIFT

df = sqlContext.read \
        .format("jdbc") \
        .option("driver", "com.amazon.redshift.jdbc42.Driver") \
        .option("url", jdbc_conn_url) \
        .option("user", redshift_user) \
        .option("password", redshift_pass) \
        .option("dbtable","public.procuidado_programas") \
        .load()
df.createOrReplaceTempView('lkp_procuidado_programas')


df = sqlContext.read \
        .format("jdbc") \
        .option("driver", "com.amazon.redshift.jdbc42.Driver") \
        .option("url", jdbc_conn_url) \
        .option("user", redshift_user) \
        .option("password", redshift_pass) \
        .option("dbtable","public.procuidado_reglas_cobro") \
        .load()
df.createOrReplaceTempView('procuidado_reglas_cobro')


df = sqlContext.read \
        .format("jdbc") \
        .option("driver", "com.amazon.redshift.jdbc42.Driver") \
        .option("url", jdbc_conn_url) \
        .option("user", redshift_user) \
        .option("password", redshift_pass) \
        .option("dbtable","public.procuidado_descuentos_validos") \
        .load()
df.createOrReplaceTempView('descuentos_procuidado_validos')


df = sqlContext.read \
    .format("jdbc") \
    .option("driver", "com.amazon.redshift.jdbc42.Driver") \
    .option("url", jdbc_conn_url) \
    .option("user", redshift_user) \
    .option("password", redshift_pass) \
    .option("dbtable","public.procuidado_reglas_porcentajes") \
    .load()
df.createOrReplaceTempView('lkp_reglas_porcentajes_procuidado')
    
    
df = sqlContext.read \
    .format("jdbc") \
    .option("driver", "com.amazon.redshift.jdbc42.Driver") \
    .option("url", jdbc_conn_url) \
    .option("user", redshift_user) \
    .option("password", redshift_pass) \
    .option("dbtable","public.procuidado_excluir") \
    .load()
df.createOrReplaceTempView('lkp_procuidado_excluir')
    
    
df = sqlContext.read \
    .format("jdbc") \
    .option("driver", "com.amazon.redshift.jdbc42.Driver") \
    .option("url", jdbc_conn_url) \
    .option("user", redshift_user) \
    .option("password", redshift_pass) \
    .option("dbtable","public.procuidado_reglas_monto") \
    .load()
df.createOrReplaceTempView('lkp_reglas_procuidado')
    
    
df = sqlContext.read \
    .format("jdbc") \
    .option("driver", "com.amazon.redshift.jdbc42.Driver") \
    .option("url", jdbc_conn_url) \
    .option("user", redshift_user) \
    .option("password", redshift_pass) \
    .option("dbtable","public.local") \
    .load()
df.createOrReplaceTempView('local')    


df = sqlContext.read \
    .format("jdbc") \
    .option("driver", "com.amazon.redshift.jdbc42.Driver") \
    .option("url", jdbc_conn_url) \
    .option("user", redshift_user) \
    .option("password", redshift_pass) \
    .option("query", f"select * from ventas.dimension_tipo_pago where date(fecha_venta_timestamp) = '{dia}' ") \
    .load()
df.createOrReplaceTempView('dimension_tipo_pago_dist') 


df = sqlContext.read \
    .format("jdbc") \
    .option("driver", "com.amazon.redshift.jdbc42.Driver") \
    .option("url", jdbc_conn_url) \
    .option("user", redshift_user) \
    .option("password", redshift_pass) \
    .option("query", f"select * from ventas.fact_producto where date(fecha_venta_timestamp) = '{dia}' ") \
    .load()
df.createOrReplaceTempView('fact_producto') 



hourly = True



#PREFILTRADO - TRANSACCIONES_CRONICO

if hourly:
    
    query = f"""

    SELECT *
    FROM {database}.transacciones_cronicos
    WHERE 
        FECHA_YEAR =  {anio_filter}
    AND FECHA_MONTH = {mes_filter}
    AND FECHA_DAY = {dia_filter}

    """

    temp = spark.sql(query)
    temp.createOrReplaceTempView('transacciones_cronicos') 
    
    
    query = f"""

    SELECT *
    FROM {database}.TRANSACCIONES_POS_VIG
    WHERE 
        FECHA_POS_YEAR =  {anio_filter}
    AND FECHA_POS_MONTH = {mes_filter}
    AND FECHA_POS_DAY = {dia_filter}

    """

    temp = spark.sql(query)
    temp.createOrReplaceTempView('transacciones_pos_vig')
    
    query = f"""

    SELECT *
    FROM {database}.TRANSACCIONES_PROMOCIONES
    WHERE 
        FECHA_YEAR =  {anio_filter}
    AND FECHA_MONTH = {mes_filter}
    AND FECHA_DAY = {dia_filter}

    """

    temp = spark.sql(query)
    temp.createOrReplaceTempView('transacciones_promociones')
    
    
else:
    
    query = f"""

    SELECT *
    FROM {database}.transacciones_cronicos

    """

    temp = spark.sql(query)
    temp.createOrReplaceTempView('transacciones_cronicos') 



temp.count()




### PROGRESIVO ###
query = f"""select /*+ BROADCAST(local,lp) */
    tpr.id_promocion                    PROMOCION_ID
    , tpr.it_producto                   PRODUCTO_ID
    , 'None'                              FABRICANTE_ID
    , tpr.fecha                           FECHA_HORA_VENTA
    , cast(tpr.fecha as date)             DIA_ID
    , date_format(tpr.fecha,'yyyyMM')     MES_ID
    , tp.local                          FILIAL_ID
    , tp.vendedor                       VEND_ID
    , cast(tp.nro_docto as char(15))    NRO_TICKET
    , tp.codigo_autorizacion            CODIGO_AUTORIZACION
    , tp.rut                            CLIENTE_RUT
    , 'None'                                CLIENTE_NOMBRE
    , tpr.TIPO_TRANSACCION              TIPO_TRANSACCION
    , tpr.TRANSACCION_INICIAL           TRANSACCION_INICIAL
    , 1                                 CANT_UNIDADES
    , tpr.precio_venta                  PRECIO
    , 0                               COSTO
    , tpr.monto_descuento               DESCUENTO
    , ROUND((tpr.monto_descuento / tpr.precio_venta)*100,0) PORCENTAJE_DESCUENTO
    , 'Progresivo'                      TIPO
    , 0                                REGLA_ID
    , 0                                COSTO_VENTA_NETO
    , 0                                BONIFICACION
    , 0                              PORCENTAJE_DESCUENTO_ESPERADO
    , 1                               CUP_ID
    , 0                              BONIFICACION_ESPERADA
    , 0                              CUF
from 
transacciones_promociones  tpr 
join transacciones_pos_vig tp on (tpr.tp_codigo_autorizacion = tp.codigo_autorizacion)
join lkp_procuidado_programas lp on (tpr.id_promocion = lp.id_promocion ) 
join local on (local.id_local = tp.local)
where
    -- cast(tpr.fecha as date) = date('{dia}')  
    tpr.tipo_transaccion ='N'
    and tpr.tp_codigo_autorizacion = tp.codigo_autorizacion
    and local.id_empresa=1
    and local.es_valido=1
    and activo = '2'
    """



DM_BONIFICACION_PROCUIDADO_P1 = spark.sql(query)
#DM_BONIFICACION_PROCUIDADO_P1 = DM_BONIFICACION_PROCUIDADO_P1.repartition(4)
DM_BONIFICACION_PROCUIDADO = DM_BONIFICACION_PROCUIDADO_P1
#DM_BONIFICACION_PROCUIDADO_P1.count()
#print(query)



### PROGRESIVO ACTIVO 6###
query = f"""
    select /*+ BROADCAST(local,lp) */
    tpr.id_promocion                    PROMOCION_ID
    , tpr.it_producto                   PRODUCTO_ID
    , 'None'                                FABRICANTE_ID
    , tpr.fecha                           FECHA_HORA_VENTA
    , cast(tpr.fecha as date)                       DIA_ID
    , date_format(tpr.fecha,'yyyyMM')   MES_ID
    , tp.local                          FILIAL_ID
    , tp.vendedor                       VEND_ID
    , cast(tp.nro_docto as char(15))    NRO_TICKET
    , tp.codigo_autorizacion            CODIGO_AUTORIZACION
    , tp.rut                            CLIENTE_RUT
    , 'None'                                CLIENTE_NOMBRE
    , tpr.TIPO_TRANSACCION              TIPO_TRANSACCION
    , tpr.TRANSACCION_INICIAL           TRANSACCION_INICIAL
    , 1                                 CANT_UNIDADES
    , tpr.precio_venta                  PRECIO
    , 0                                COSTO
    , tpr.monto_descuento               DESCUENTO
    , ROUND((tpr.monto_descuento / tpr.precio_venta)*100,0) PORCENTAJE_DESCUENTO
    , 'Progresivo'                      TIPO 
    , 0                                REGLA_ID
    , 0                                COSTO_VENTA_NETO
    , 0                                BONIFICACION
    , 0                              PORCENTAJE_DESCUENTO_ESPERADO
    , 1                               CUP_ID
    , 0                              BONIFICACION_ESPERADA
    , 0                              CUF
from 
transacciones_promociones  tpr 
join transacciones_pos_vig tp on (tpr.tp_codigo_autorizacion = tp.codigo_autorizacion)
join lkp_procuidado_programas lp on (tpr.id_promocion = lp.id_promocion  and tpr.it_producto = lp.id_producto) 
join local on (local.id_local = tp.local)
where
    tpr.id_promocion = lp.id_promocion 
    and tpr.it_producto = lp.id_producto
    -- and cast(tpr.fecha as date) = date('{dia}')
    and tpr.tipo_transaccion ='N'
    and local.id_empresa=1
    and local.es_valido=1
    and activo = '6'
    """


DM_BONIFICACION_PROCUIDADO_P2 = spark.sql(query)
DM_BONIFICACION_PROCUIDADO = DM_BONIFICACION_PROCUIDADO.unionByName(DM_BONIFICACION_PROCUIDADO_P2)
DM_BONIFICACION_PROCUIDADO_P2 = ''
#DM_BONIFICACION_PROCUIDADO_P2.count()
#print(query)



### FRECUENTE    ###  VALIDADO CON CAMILO: 269 PARA EL '2019-02-10'
query = f"""
    select /*+ BROADCAST(local,lpp,pc,tc) */
    lpp.id_promocion                        PROMOCION_ID 
    , lpp.id_producto                       PRODUCTO_ID 
    , 'None'                                FABRICANTE_ID
    , tc.fecha                           FECHA_HORA_VENTA
    , cast(tc.fecha as date)                            DIA_ID
    , date_format(tc.fecha,'yyyyMM')                    MES_ID
    , tp.local                          FILIAL_ID
    , TP.VENDEDOR                           VEND_ID
    , cast(tp.nro_docto as char(15))                NRO_TICKET
    , tp.codigo_autorizacion                    CODIGO_AUTORIZACION
    , tp.rut                            CLIENTE_RUT
    , 'None'                                CLIENTE_NOMBRE  
    , TC.tipo_transaccion                       TIPO_TRANSACCION
    , tc.transaccion_inicial                    TRANSACCION_INICIAL
    , 1     CANT_UNIDADES
    , tprom.precio_venta                        PRECIO
    , 0                        COSTO
    , tprom.monto_descuento  DESCUENTO

    
    
    , ((case when tprom.monto_descuento = 0 then 1 else tprom.monto_descuento end)*100)/(case when tprom.precio_venta   = 0 then 1 else tprom.precio_venta   end) PORCENTAJE_DESCUENTO
    
    
    , 'Frecuente'                           TIPO
    , 0                                REGLA_ID
    , 0                                COSTO_VENTA_NETO
    , 0                                BONIFICACION
    , 0                              PORCENTAJE_DESCUENTO_ESPERADO
    , 1                               CUP_ID
    , 0                              BONIFICACION_ESPERADA
    , 0                              CUF
from 
transacciones_cronicos tc 
join transacciones_pos_vig tp on (tc.tp_codigo_autorizacion = tp.codigo_autorizacion)
join {database}.pack_cronicos pc on (tc.it_producto = pc.codigo_pos)
join lkp_procuidado_programas lpp on (tc.it_producto = lpp.it_producto )
join transacciones_promociones tprom on (tc.tp_codigo_autorizacion = tprom.tp_codigo_autorizacion
                                         and lpp.it_producto = tprom.it_producto
                                         and tprom.id_promocion = lpp.id_promocion)
join local on (local.id_local = tp.local)
where 

    tc.tp_codigo_autorizacion is not null 
    and tc.tp_codigo_autorizacion = tp.codigo_autorizacion
    and local.id_empresa=1
    and local.es_valido=1
    and tc.it_producto = pc.codigo_pos
    and tc.tipo_transaccion <> 'A' and tc.TIPO_TRANSACCION <> 'Z'
    and activo = '1'
    and tc.TIPO_TRANSACCION = 'C'
    and tc.transaccion_inicial is null
    and tprom.tipo_transaccion <> 'A'
    """


DM_BONIFICACION_PROCUIDADO_P3 = spark.sql(query)
#Guardamos la tabla para poder hacer append luego
DM_BONIFICACION_PROCUIDADO = DM_BONIFICACION_PROCUIDADO.unionByName(DM_BONIFICACION_PROCUIDADO_P3)
DM_BONIFICACION_PROCUIDADO_P3 = ''
#DM_BONIFICACION_PROCUIDADO_P3.count()


#query = """ select * from lkp_procuidado_programas """
#x = spark.sql(query)
#x.count()


### FRECUENTE   ANULAR CANJE EN NORMAL###  VALIDADO CON CAMILO: 269 PARA EL '2019-02-10'
query = f"""
    select /*+ BROADCAST(local,lpp,pc,tc) */
    lpp.id_promocion                        PROMOCION_ID
    , lpp.id_producto                       PRODUCTO_ID
    , 'None'                                FABRICANTE_ID
    , tc.fecha                           FECHA_HORA_VENTA
    , cast(tc.fecha as date)                            DIA_ID
    , date_format(tc.fecha,'yyyyMM')                    MES_ID
    , tp.local                          FILIAL_ID
    , TP.VENDEDOR                           VEND_ID
    , cast(tp.nro_docto as string)              NRO_TICKET
    , tp.codigo_autorizacion                    CODIGO_AUTORIZACION
    , tp.rut                            CLIENTE_RUT
    , 'None'                                CLIENTE_NOMBRE  
    , 'N'                       TIPO_TRANSACCION
    , ''                    TRANSACCION_INICIAL
    , -1        CANT_UNIDADES
    , tprom.precio_venta                        PRECIO
    , 0                        COSTO
    , 0         DESCUENTO
    , 0 PORCENTAJE_DESCUENTO    
    , 'Frecuente'                           TIPO
    , 0                                REGLA_ID
    , 0                                COSTO_VENTA_NETO
    , 0                                BONIFICACION
    , 0                              PORCENTAJE_DESCUENTO_ESPERADO
    , 1                               CUP_ID
    , 0                              BONIFICACION_ESPERADA
    , 0                              CUF
from 
     transacciones_cronicos tc 
join transacciones_pos_vig tp on (tc.tp_codigo_autorizacion = tp.codigo_autorizacion)
join {database}.pack_cronicos pc on (tc.it_producto = pc.codigo_pos) 
join lkp_procuidado_programas lpp on (tc.it_producto = lpp.it_producto)
join transacciones_promociones tprom on (lpp.it_producto = tprom.it_producto 
                                          and tc.tp_codigo_autorizacion = tprom.tp_codigo_autorizacion)
join local on (local.id_local = tp.local)
where 
        lpp.id_PROMOCION = tprom.ID_PROMOCION 
    and tc.tp_codigo_autorizacion is not null 
    --and cast(tc.fecha as date) = date('{dia}')
    and tc.tipo_transaccion <> 'A' and tc.TIPO_TRANSACCION <> 'Z'
    and activo = '1'
    and tc.TIPO_TRANSACCION = 'C'
    and tc.transaccion_inicial is null
    and tprom.tipo_transaccion <> 'A'
    and local.id_empresa=1
    and local.es_valido=1
    """


DM_BONIFICACION_PROCUIDADO_P4 = spark.sql(query)
#Guardamos la tabla para poder hacer append luego
DM_BONIFICACION_PROCUIDADO= DM_BONIFICACION_PROCUIDADO.unionByName(DM_BONIFICACION_PROCUIDADO_P4)
DM_BONIFICACION_PROCUIDADO_P4 = ''
#DM_BONIFICACION_PROCUIDADO_P4.count()



### FRECUENTE   PARTE NORMAL ### VALIDADO CON CAMILO 960 AL '2019-02-10'
query = f"""
        select  /*+ BROADCAST(local,c,d,a) */
      c.id_promocion                        PROMOCION_ID
    , c.id_producto                         PRODUCTO_ID
    , 'None'                        FABRICANTE_ID -- preguntar por dimension producto y actualizar para eliminar la duplicidad
    , b.fecha_venta                           FECHA_HORA_VENTA
    , cast(b.fecha_venta as date)         DIA_ID --b.fecha_venta
    , date_format(d.fecha,'yyyyMM')                  MES_ID
    , b.id_local                            FILIAL_ID
    , b.id_vendedor                         VEND_ID
    , b.numero_documento                            NRO_TICKET
    , f.codigo_autorizacion                     CODIGO_AUTORIZACION
    , b.cliente_rut                         CLIENTE_RUT
    , 'None'                                CLIENTE_NOMBRE
    , d.tipo_transaccion                        TIPO_TRANSACCION
    , d.transaccion_inicial                     TRANSACCION_INICIAL
    , b.cantidad_unidades                       CANT_UNIDADES
    , b.precio_unitario                     PRECIO
    , b.costo_unitario                                 COSTO
    , 0                             DESCUENTO
    , 0                            PORCENTAJE_DESCUENTO
    , 'Frecuente'                           TIPO
    , 0                                REGLA_ID
    , 0                                COSTO_VENTA_NETO
    , 0                                BONIFICACION
    , 0                              PORCENTAJE_DESCUENTO_ESPERADO
    , 1                               CUP_ID
    , 0                              BONIFICACION_ESPERADA
    , 0                              CUF
From  
     dimension_tipo_pago_dist a    
join fact_producto b on (cast(a.fecha_venta_timestamp as date) = cast(b.fecha_venta as date)
                                    and a.correlativo_venta = b.correlativo_venta
                                    and a.id_local = b.id_local) 
join lkp_procuidado_programas c on (cast(b.id_producto as integer) = c.id_producto)
join transacciones_cronicos d on (d.it_producto = c.it_producto
                                             and cast(d.fecha as date) = cast(a.fecha_venta_timestamp as date)
                                             and cast(b.codigo_autorizacion_redmax as int) =  d.tp_codigo_autorizacion)
join transacciones_pos_vig f on (d.tp_codigo_autorizacion = f.codigo_autorizacion)
join local on (local.id_local = f.local)
where
    
    d.tipo_transaccion = 'N' 
    and cast(d.fecha as date) = cast(b.fecha_venta as date) 
    and d.tp_codigo_autorizacion is not null 
    and activo =  '1' 
    --and cast(d.fecha as date) = date('{dia}') 
    and d.transaccion_inicial is null
    and local.id_local = f.local
    and local.id_empresa=1
    and local.es_valido=1
    and b.es_venta=1

    """


DM_BONIFICACION_PROCUIDADO_P5 = spark.sql(query)
#Guardamos la tabla para poder hacer append luego
DM_BONIFICACION_PROCUIDADO= DM_BONIFICACION_PROCUIDADO.unionByName(DM_BONIFICACION_PROCUIDADO_P5)
DM_BONIFICACION_PROCUIDADO_P5 = ''
#DM_BONIFICACION_PROCUIDADO_P5.count()



### Favorito ###   VALIDADO CON CAMILO: 1208 PARA EL '2019-02-10'
query = f"""
    select /*+ BROADCAST(local,ldc) */
    tpr.id_promocion                            PROMOCION_ID
    , tpr.it_producto                           PRODUCTO_ID
    , 'None'                                    FABRICANTE_ID
    , tpr.fecha                                 FECHA_HORA_VENTA
    , cast(tpr.fecha as date)                   DIA_ID
    , date_format(tpr.fecha,'yyyyMM')                   MES_ID
    , tp.local                              FILIAL_ID
    , tp.vendedor                               VEND_ID
    , CAST(tp.nro_docto  AS CHAR(15))                   NRO_TICKET
    , tp.CODIGO_AUTORIZACION                        CODIGO_AUTORIZACION
    , tp.rut                                CLIENTE_RUT
    , 'None'                                    CLIENTE_NOMBRE
    , tpr.tipo_transaccion                          TIPO_TRANSACCION
    , tpr.transaccion_inicial                       TRANSACCION_INICIAL
    , count (tpr.tp_codigo_autorizacion)                    CANT_UNIDADES
    , sum(tpr.precio_venta)/count (tpr.tp_codigo_autorizacion)                      PRECIO  
    , 0                                    COSTO
    , sum(tpr.monto_descuento) /count (tpr.tp_codigo_autorizacion)                      DESCUENTO
    , cast((sum(tpr.monto_descuento)/sum(tpr.precio_venta))*100 as int) PORCENTAJE_DESCUENTO
    , 'Favorito'                                TIPO
    , 0                                REGLA_ID
    , 0                                COSTO_VENTA_NETO
    , 0                                BONIFICACION
    , 0                              PORCENTAJE_DESCUENTO_ESPERADO
    , 1                               CUP_ID
    , 0                              BONIFICACION_ESPERADA
    , 0                              CUF
from    
    transacciones_promociones tpr
join transacciones_pos_vig tp on (tpr.tp_codigo_autorizacion = tp.codigo_autorizacion)
join lkp_procuidado_programas ldc on (tpr.id_promocion = ldc.id_promocion 
                                      and tpr.it_producto = ldc.id_producto)
join local on (local.id_local = tp.local)
where
    tpr.id_promocion = ldc.id_promocion
    and tpr.it_producto = ldc.id_producto
    --and cast(tpr.fecha as date) = date('{dia}')
    and tpr.tipo_transaccion ='N'
    and activo = '3'
    and local.id_empresa=1
    and local.es_valido=1
group by
    tpr.id_promocion
    , tpr.it_producto
    , tpr.fecha
    , cast(tpr.fecha as date) 
    , date_format(tpr.fecha,'yyyyMM')
    , tp.local  
    , tp.vendedor 
    , tp.nro_docto
    , tp.codigo_autorizacion
    , tp.rut 
    , tpr.tipo_transaccion
    , tpr.transaccion_inicial
    """


DM_BONIFICACION_PROCUIDADO_P6 = spark.sql(query)
#Guardamos la tabla para poder hacer append luego
DM_BONIFICACION_PROCUIDADO= DM_BONIFICACION_PROCUIDADO.unionByName(DM_BONIFICACION_PROCUIDADO_P6)
DM_BONIFICACION_PROCUIDADO_P6 = ''
#DM_BONIFICACION_PROCUIDADO_P6.count()



### FORXIGA - CANJE ### VALIDADO CON CAMILO: 0 PARA EL '2019-02-10'
query = f"""
    select /*+ BROADCAST(local,lpp,pc,tc) */
      lpp.id_promocion       PROMOCION_ID 
    , lpp.id_producto      PRODUCTO_ID 
    , 'None'                 FABRICANTE_ID
    , tc.fecha               FECHA_HORA_VENTA
    , cast(tc.fecha as date)             DIA_ID
    , date_format(tc.fecha,'yyyyMM')     MES_ID
    , tp.local                          FILIAL_ID
    , TP.VENDEDOR                           VEND_ID
    , cast(tp.nro_docto as char(15))                NRO_TICKET
    , tp.codigo_autorizacion                    CODIGO_AUTORIZACION
    , tp.rut                            CLIENTE_RUT
    , 'None'                                CLIENTE_NOMBRE  
    , TC.tipo_transaccion                       TIPO_TRANSACCION
    , tc.transaccion_inicial                    TRANSACCION_INICIAL
    , 1     CANT_UNIDADES
    , tprom.precio_venta                        PRECIO
    , 0                        COSTO
    , cast(monto_descuento as int)  DESCUENTO
    -- , case when (TC.tipo_transaccion = 'C' and tprom.transaccion_inicial ='') then cast(monto_descuento as int) else 0 end DESCUENTO
    , ((case when cast(monto_descuento as int) = 0 then 1 else cast(monto_descuento as int) end)*100)/(case when tprom.precio_venta  = 0 then 1 else tprom.precio_venta  end) PORCENTAJE_DESCUENTO	
    , 'Frecuente'                           TIPO
    , 0                                REGLA_ID
    , 0                                COSTO_VENTA_NETO
    , 0                                BONIFICACION
    , 0                              PORCENTAJE_DESCUENTO_ESPERADO
    , 1                               CUP_ID
    , 0                              BONIFICACION_ESPERADA
    , 0                              CUF
    
    from 
     transacciones_cronicos tc 
join transacciones_pos_vig tp on (tc.tp_codigo_autorizacion = tp.codigo_autorizacion)
join {database}.pack_cronicos pc on (tc.it_producto = pc.codigo_pos)
join lkp_procuidado_programas lpp on (tc.it_producto = lpp.it_producto)
join transacciones_promociones tprom on (tc.tp_codigo_autorizacion = tprom.tp_codigo_autorizacion
                                         and lpp.id_producto = tprom.it_producto
                                         and lpp.ID_PROMOCION = tprom.ID_PROMOCION)
join ordenes_compra.local on (local.id_local = tp.local)
where 
    tc.tp_codigo_autorizacion is not null 
    --and cast(tc.fecha as date) = date('{dia}')
    and tc.tipo_transaccion <> 'A' and tc.TIPO_TRANSACCION <> 'Z'
    and activo = '5' 
    and tc.TIPO_TRANSACCION = 'C'
    and tc.transaccion_inicial is null
    and tprom.tipo_transaccion <> 'A'
    and local.id_empresa=1
    and local.es_valido=1


    """


DM_BONIFICACION_PROCUIDADO_P7 = spark.sql(query)
#Guardamos la tabla para poder hacer append luego
DM_BONIFICACION_PROCUIDADO= DM_BONIFICACION_PROCUIDADO.unionByName(DM_BONIFICACION_PROCUIDADO_P7)
DM_BONIFICACION_PROCUIDADO_P7 = ''
#DM_BONIFICACION_PROCUIDADO_P7.count()


### FORXIGA - NORMAL ### VALIDADO CON CAMILO: 0 PARA EL '2019-02-10'
query = f"""
    select /*+ BROADCAST(local,c,d) */
      c.id_promocion                        PROMOCION_ID
    , c.id_producto                         PRODUCTO_ID
    , 'None'                                FABRICANTE_ID
    , b.fecha_venta                              FECHA_HORA_VENTA
    , cast(b.fecha_venta as date)           DIA_ID
    , date_format(d.fecha,'yyyyMM')         MES_ID
    , b.id_local                            FILIAL_ID
    , b.id_vendedor                         VEND_ID
    , right (b.numero_documento,10)                         NRO_TICKET
    , f.codigo_autorizacion                     CODIGO_AUTORIZACION
    , b.cliente_rut                         CLIENTE_RUT
    , 'None'                                CLIENTE_NOMBRE
    , d.tipo_transaccion                        TIPO_TRANSACCION
    , d.transaccion_inicial                     TRANSACCION_INICIAL
    , b.numero_item                     CANT_UNIDADES
    , b.precio_unitario                     PRECIO
    , b.COSTO_UNITARIO                      COSTO
    , 0                             DESCUENTO
    , 100/(case when b.precio_unitario  = 0 then 1 else b.precio_unitario  end) PORCENTAJE_DESCUENTO	   
    , 'Frecuente'                           TIPO
    , 0                                REGLA_ID
    , 0                                COSTO_VENTA_NETO
    , 0                                BONIFICACION
    , 0                              PORCENTAJE_DESCUENTO_ESPERADO
    , 1                               CUP_ID
    , 0                              BONIFICACION_ESPERADA
    , 0                              CUF
From  
     dimension_tipo_pago_dist a 
join fact_producto b on (cast(a.fecha_venta_timestamp as date) = cast(b.fecha_venta as date) 
                           and a.CORRELATIVO_VENTA = b.CORRELATIVO_VENTA
                           and a.id_local = b.id_local)
join lkp_procuidado_programas c on (b.id_producto = c.id_producto)
join transacciones_cronicos d on (d.it_producto = c.it_producto
                                   --and a.AUTORIZACION_RDMAX =  d.tp_codigo_autorizacion
                                  and d.fecha = b.fecha_venta)
join transacciones_pos_vig f on (d.tp_codigo_autorizacion = f.codigo_autorizacion
                                             and b.numero_documento=f.nro_docto)
join local on (local.id_local = f.local)    --  en vez de b.local 
where 
    d.tipo_transaccion = 'N'
    and d.tp_codigo_autorizacion is not null 
    and activo = '4' 
    --and cast(d.fecha as date) = date('{dia}')
    and d.transaccion_inicial is null
    and local.id_empresa=1
    and local.es_valido=1

    """


DM_BONIFICACION_PROCUIDADO_P8 = spark.sql(query)
DM_BONIFICACION_PROCUIDADO= DM_BONIFICACION_PROCUIDADO.unionByName(DM_BONIFICACION_PROCUIDADO_P8)
DM_BONIFICACION_PROCUIDADO_P8=''
#DM_BONIFICACION_PROCUIDADO_P8.count()



### INSCRIPCIONES ### VALIDADO CON CAMILO: 0 PARA EL '2019-02-10'
query = f"""
    select /*+ BROADCAST(local,tc) */
    substring(cast(pf.codigo_pos as string),5,4)    PROMOCION_ID
    , ri.producto_inscrito                          PRODUCTO_ID
    , 'None'                                        FABRICANTE_ID
    , ri.fecha_registro                             FECHA_HORA_VENTA
    , cast(ri.fecha_registro as date)               DIA_ID
    , DATE_FORMAT(ri.fecha_registro,'yyyyMM')       MES_ID
    , tpv.local                                     FILIAL_ID
    , tpv.vendedor                                  VEND_ID
    , cast(tpv.nro_docto as char(15))               NRO_TICKET
    , tpv.codigo_autorizacion                       CODIGO_AUTORIZACION
    , tpv.rut                                       CLIENTE_RUT
    , 'None'                                            CLIENTE_NOMBRE
    , tc.TIPO_TRANSACCION                           TIPO_TRANSACCION
    , tc.TRANSACCION_INICIAL                        TRANSACCION_INICIAL
    , 1                                             CANT_UNIDADES
    , tc.precio_venta                               PRECIO
    , 0                                            COSTO
    , 0                                             DESCUENTO
    , 0                                             PORCENTAJE_DESCUENTO
    , 'PROGRESIVO'                                  TIPO 
    , 0                                REGLA_ID
    , 0                                COSTO_VENTA_NETO
    , 0                                BONIFICACION
    , 0                              PORCENTAJE_DESCUENTO_ESPERADO
    , 1                               CUP_ID
    , 0                              BONIFICACION_ESPERADA
    , 0                              CUF
From  
{database}.pack_frecuente pf
join transacciones_pos_vig tpv on (tpv.codigo_autorizacion = pf.tp_codigo_autorizacion
                                   and pf.pt_party_id = tpv.pt_party_id)
join {database}.registro_inscripcion ri on (tpv.nro_docto = cast((substring(cast(ri.cupon_inscripcion as string),3,12)) as int)
                                    and pf.codigo_pos = ri.codigo_pos
                                    and pf.pt_party_id = ri.party_id_cliente)
join {database}.transacciones_cronicos tc on (tc.tp_codigo_autorizacion = tpv.codigo_autorizacion
                                   and tc.pt_party_id = tpv.pt_party_id
                                   and TC.IT_PRODUCTO = ri.codigo_pos
                                   and tc.pt_party_id = ri.party_id_cliente
                                   and pf.codigo_pos = tc.it_producto)
join local on (local.id_local = tpv.local)   
where 
    pf.codigo_pos in (888847270000,888847280000,888847290000,888847300000,888847310000,
                      888847330000,888847340000,888847350000,888847360000,888847370000)
    and cast(ri.fecha_registro as date) = date('{dia}')
    and tc.precio_venta is not null
    and local.id_empresa=1
    and local.es_valido=1
    """


DM_BONIFICACION_PROCUIDADO_P9 = spark.sql(query)
DM_BONIFICACION_PROCUIDADO= DM_BONIFICACION_PROCUIDADO.unionByName(DM_BONIFICACION_PROCUIDADO_P9)
DM_BONIFICACION_PROCUIDADO_P9=''
#DM_BONIFICACION_PROCUIDADO_P9.count()





######################################## UPDATES ###############################################


DM_BONIFICACION_PROCUIDADO.createOrReplaceTempView('DM_BONIFICACION_PROCUIDADO')


### Actualizacion de productos del lab 256

DM_BONIFICACION_PROCUIDADO = DM_BONIFICACION_PROCUIDADO.withColumn("DESCUENTO",when(col("PRODUCTO_ID").isin([2561456,2561465,2561852,2561876]),0).otherwise(col("DESCUENTO")))

### WORKS ###

query =  f"""

SELECT /*+ BROADCAST(dp,lp,prc) */ distinct 
dp.producto_id as producto_id_del         
,lp.laboratorio as fabricante_id_del
,prc.id_regla as regla_id_del

from 
DM_BONIFICACION_PROCUIDADO dp  
join producto_unificado_historico lp on (dp.producto_id = cast(id_producto as int))
join procuidado_reglas_cobro prc on (prc.id_fabricante = lp.laboratorio)

where cast(dp.dia_id as date) = date('{dia}')


"""

# Se guarda el resultado del query en un dataframe de pandas para hacer el update

temp1 = spark.sql(query)
temp1.createOrReplaceTempView('temp1')
DM_BONIFICACION_PROCUIDADO = DM_BONIFICACION_PROCUIDADO.join(temp1, DM_BONIFICACION_PROCUIDADO.PRODUCTO_ID == temp1.producto_id_del, 'left')
DM_BONIFICACION_PROCUIDADO = DM_BONIFICACION_PROCUIDADO.withColumn("FABRICANTE_ID",col("fabricante_id_del"))
DM_BONIFICACION_PROCUIDADO = DM_BONIFICACION_PROCUIDADO.withColumn("REGLA_ID",col("regla_id_del"))
DM_BONIFICACION_PROCUIDADO = DM_BONIFICACION_PROCUIDADO.drop('producto_id_del')
DM_BONIFICACION_PROCUIDADO = DM_BONIFICACION_PROCUIDADO.drop('fabricante_id_del')
DM_BONIFICACION_PROCUIDADO = DM_BONIFICACION_PROCUIDADO.drop('regla_id_del')

##############################################################################################

### TEMPDOS ### 

query =  f"""

SELECT distinct -- LINEAS PROVISIONALES PARA DESARROLLO
    date_format('2020-07-08','yyyyMM') mes_id_del  -- date_format(dia_id,'yyyyMM') mes_id_del
    , CAST('2020-07-08' as date) as dia_id_del -- CAST(dia_id as date) as dia_id_del
    , cast(id_producto as int) as producto_id_del
    , costo_unitario COSTO_VENTA_NETO_del
from  
producto_unificado_historico
where 
    cast(fecha_timestamp as date) = date('{dia}') 
    and cast(id_producto as int) in (select 
                                      distinct cast(producto_id as int) 
                                      from DM_BONIFICACION_PROCUIDADO
                                      where cast(dia_id as date) = date('{dia}'))
    and codigo_empresa=1


"""

# Se guarda el resultado del query en un dataframe de spark para hacer el update

temp2 = spark.sql(query)                                                                

DM_BONIFICACION_PROCUIDADO = DM_BONIFICACION_PROCUIDADO.join(temp2, (DM_BONIFICACION_PROCUIDADO.PRODUCTO_ID == temp2.producto_id_del) , 'left')
#DM_BONIFICACION_PROCUIDADO = DM_BONIFICACION_PROCUIDADO.join(temp2, (DM_BONIFICACION_PROCUIDADO.PRODUCTO_ID == temp2.producto_id_del) & (col("DIA_ID").cast(DateType()) == temp2.dia_id_del), 'left')



#Se cambian los valores de las columnas necesarias y se borran las columnas adicionales creadas por el join
#DM_BONIFICACION_PROCUIDADO['COSTO_VENTA_NETO'] = DM_BONIFICACION_PROCUIDADO['COSTO_VENTA_NETO_updt']
#DM_BONIFICACION_PROCUIDADO.loc[DM_BONIFICACION_PROCUIDADO['COSTO_VENTA_NETO_updt'].notna(), 'COSTO_VENTA_NETO'] = DM_BONIFICACION_PROCUIDADO['COSTO_VENTA_NETO_updt']

DM_BONIFICACION_PROCUIDADO = DM_BONIFICACION_PROCUIDADO.withColumn("COSTO_VENTA_NETO",when(col("COSTO_VENTA_NETO_del").isNotNull(),col("COSTO_VENTA_NETO_del")).otherwise(col("COSTO_VENTA_NETO")))
DM_BONIFICACION_PROCUIDADO = DM_BONIFICACION_PROCUIDADO.drop("mes_id_del")
DM_BONIFICACION_PROCUIDADO = DM_BONIFICACION_PROCUIDADO.drop("dia_id_del") 
DM_BONIFICACION_PROCUIDADO = DM_BONIFICACION_PROCUIDADO.drop("producto_id_del") 
DM_BONIFICACION_PROCUIDADO = DM_BONIFICACION_PROCUIDADO.drop("COSTO_VENTA_NETO_del") 


##DIA ANTERIOR

temp2.createOrReplaceTempView('temp2')


query =  f"""

SELECT 
B.COSTO_VENTA_NETO_del, B.producto_id_del
FROM 
	DM_BONIFICACION_PROCUIDADO A, temp2 B
WHERE 
	cast(A.dia_id as date) = cast(date_add(B.dia_id_del,-1) as date)
	and A.producto_id = B.producto_id_del
	and cast(A.dia_id as date) = date('{dia}')
	and A.COSTO_VENTA_NETO is null

"""

# Se guarda el resultado del query en un dataframe de pandas para hacer el update

temp2_2 = spark.sql(query)

#Se hace join con la fact para poder actualizar las columnas
#DM_BONIFICACION_PROCUIDADO = DM_BONIFICACION_PROCUIDADO.merge(temp2_2,how='left', left_on=['PRODUCTO_ID'], right_on=['producto_id'])
  
DM_BONIFICACION_PROCUIDADO = DM_BONIFICACION_PROCUIDADO.join(temp2_2, DM_BONIFICACION_PROCUIDADO.PRODUCTO_ID == temp2.producto_id_del, 'left')
DM_BONIFICACION_PROCUIDADO = DM_BONIFICACION_PROCUIDADO.withColumn("COSTO_VENTA_NETO",when(col("COSTO_VENTA_NETO_del").isNotNull(),col("COSTO_VENTA_NETO_del")).otherwise(col("COSTO_VENTA_NETO")))
DM_BONIFICACION_PROCUIDADO = DM_BONIFICACION_PROCUIDADO.drop("producto_id_del") 
DM_BONIFICACION_PROCUIDADO = DM_BONIFICACION_PROCUIDADO.drop("COSTO_VENTA_NETO_del")  

##################################################################################################################

### Actualizacion de productos de Zabal

query =  f"""

SELECT 
A.PRODUCTO_ID as PRODUCTO_ID_del
FROM DM_BONIFICACION_PROCUIDADO A 
	, procuidado.registro_inscripcion B
WHERE 
	A.FABRICANTE_ID = 297
	AND cast(A.dia_id as date) = date('{dia}')
	AND A.CANT_UNIDADES = 3
	AND A.CLIENTE_RUT = cast(B.RUT_CLIENTE as int)
	AND cast(A.DIA_ID as date) = cast(B.FECHA_REGISTRO as date)
	AND A.PRODUCTO_ID = B.PRODUCTO_INSCRITO
"""


# Se guarda el resultado del query en un dataframe de pandas para hacer el update

temp = spark.sql(query)

#Se hace join con la fact para poder actualizar las columnas

DM_BONIFICACION_PROCUIDADO = DM_BONIFICACION_PROCUIDADO.join(temp, DM_BONIFICACION_PROCUIDADO.PRODUCTO_ID == temp.PRODUCTO_ID_del, 'left') 

DM_BONIFICACION_PROCUIDADO = DM_BONIFICACION_PROCUIDADO.withColumn("CANT_UNIDADES",when(col("PRODUCTO_ID_del").isNotNull(),1).otherwise(col("CANT_UNIDADES")))

DM_BONIFICACION_PROCUIDADO = DM_BONIFICACION_PROCUIDADO.drop("PRODUCTO_ID_del")

##################################################################################################################


### Actualizacion de productos de Janssen

query =  f"""

SELECT 
A.PRODUCTO_ID as PRODUCTO_ID_del
FROM DM_BONIFICACION_PROCUIDADO A 
WHERE 
	A.FABRICANTE_ID = 36
	and A.CANT_UNIDADES = 50
	AND cast(A.dia_id as date) = date('{dia}')
    
"""


# Se guarda el resultado del query en un dataframe de pandas para hacer el update

temp = spark.sql(query)

#Se hace join con la fact para poder actualizar las columnas

DM_BONIFICACION_PROCUIDADO = DM_BONIFICACION_PROCUIDADO.join(temp, DM_BONIFICACION_PROCUIDADO.PRODUCTO_ID == temp.PRODUCTO_ID_del, 'left') 

DM_BONIFICACION_PROCUIDADO = DM_BONIFICACION_PROCUIDADO.withColumn("CANT_UNIDADES",when(col("PRODUCTO_ID_del").isNotNull(),1).otherwise(col("CANT_UNIDADES")))
DM_BONIFICACION_PROCUIDADO = DM_BONIFICACION_PROCUIDADO.drop("PRODUCTO_ID_del")



### Actualizacion de los descuentos progresivos

query =  """
SELECT  
    case when max(porcentaje_descuento) is null then 0 else max(porcentaje_descuento) end as porcentaje_nuevo
    , max(porcentaje_descuento) as porcentaje_descuento
    , ID_PRODUCTO
FROM 
     descuentos_procuidado_validos
GROUP BY 
    --porcentaje_descuento
    ID_PRODUCTO
    """
temp_sub = spark.sql(query)
temp_sub.createOrReplaceTempView('temp_sub_1')
temp_sub=''


query =  f"""

SELECT   
     distinct
     DP.PRODUCTO_ID as PRODUCTO_ID_del
	, DP.PORCENTAJE_DESCUENTO as PORCENTAJE_DESCUENTO_del
	, b.porcentaje_nuevo 
from 
	DM_BONIFICACION_PROCUIDADO DP
join temp_sub_1 b on (cast(DP.PRODUCTO_ID as int)=b.ID_PRODUCTO and b.porcentaje_descuento<= DP.PORCENTAJE_DESCUENTO)
where 
	DP.tipo = 'Progresivo'
	and cast(dia_id as date) = date('{dia}')
	and cast(DP.PRODUCTO_ID as int) IN (SELECT DISTINCT ID_PRODUCTO FROM descuentos_procuidado_validos)

    
"""


# Se guarda el resultado del query en un dataframe de pandas para hacer el update

temp = spark.sql(query)

#Se hace join con la fact para poder actualizar las columnas


DM_BONIFICACION_PROCUIDADO = DM_BONIFICACION_PROCUIDADO.join(temp, (DM_BONIFICACION_PROCUIDADO.PRODUCTO_ID == temp.PRODUCTO_ID_del) & (DM_BONIFICACION_PROCUIDADO.PORCENTAJE_DESCUENTO == temp.PORCENTAJE_DESCUENTO_del), 'left') 

DM_BONIFICACION_PROCUIDADO = DM_BONIFICACION_PROCUIDADO.withColumn("PORCENTAJE_DESCUENTO_ESPERADO",when(col("PRODUCTO_ID_del").isNotNull(),col("porcentaje_nuevo")).otherwise(col("PORCENTAJE_DESCUENTO_ESPERADO")))
DM_BONIFICACION_PROCUIDADO = DM_BONIFICACION_PROCUIDADO.drop("PRODUCTO_ID_del")
DM_BONIFICACION_PROCUIDADO = DM_BONIFICACION_PROCUIDADO.drop("porcentaje_nuevo")
DM_BONIFICACION_PROCUIDADO = DM_BONIFICACION_PROCUIDADO.drop("PORCENTAJE_DESCUENTO_del")




DM_BONIFICACION_PROCUIDADO = DM_BONIFICACION_PROCUIDADO.withColumn("PORCENTAJE_DESCUENTO_ESPERADO",when(col("DIA_ID").cast(DateType())==dia,col("PORCENTAJE_DESCUENTO")).otherwise(col("PORCENTAJE_DESCUENTO_ESPERADO")))

DM_BONIFICACION_PROCUIDADO = DM_BONIFICACION_PROCUIDADO.withColumn("PORCENTAJE_DESCUENTO_ESPERADO",when((col("DIA_ID").cast(DateType())==dia) & (col("PORCENTAJE_DESCUENTO_ESPERADO")==1),0).otherwise(col("PORCENTAJE_DESCUENTO_ESPERADO")))
                            
DM_BONIFICACION_PROCUIDADO = DM_BONIFICACION_PROCUIDADO.withColumn("PORCENTAJE_DESCUENTO_ESPERADO",when((col("DIA_ID").cast(DateType())==dia) & (col("PORCENTAJE_DESCUENTO_ESPERADO")==99),100).otherwise(col("PORCENTAJE_DESCUENTO_ESPERADO")))




### Actualizacion de los descuentos progresivos


query =  f"""

SELECT  
	cast(A.ID_PRODUCTO as int)  PRODUCTO_ID_del
	, A.MONTO_CUF CUF_del
from producto_unificado_historico A
where 
    cast(A.ID_PRODUCTO  as int) in (   
        select
        producto_id
        from DM_BONIFICACION_PROCUIDADO
        where cast(DIA_ID as date) = date('{dia}') 
        group by producto_id)
	and A.codigo_empresa = 1
    and fecha_timestamp = cast(date_add(date('{dia}'),-1) as date) 

    
"""
# Se guarda el resultado del query en un dataframe de pandas para hacer el update

temp = spark.sql(query)

#Se hace join con la fact para poder actualizar las columnas


DM_BONIFICACION_PROCUIDADO = DM_BONIFICACION_PROCUIDADO.join(temp, DM_BONIFICACION_PROCUIDADO.PRODUCTO_ID == temp.PRODUCTO_ID_del, 'left') 

DM_BONIFICACION_PROCUIDADO = DM_BONIFICACION_PROCUIDADO.withColumn("CUF",when(col("PRODUCTO_ID_del").isNotNull(),col("CUF_del")).otherwise(col("CUF")))
DM_BONIFICACION_PROCUIDADO = DM_BONIFICACION_PROCUIDADO.drop("PRODUCTO_ID_del")
DM_BONIFICACION_PROCUIDADO = DM_BONIFICACION_PROCUIDADO.drop("CUF_del")


## WORKS ##
query =  f""" 
SELECT  
a.producto_id PRODUCTO_ID_del
, a.promocion_id promocion_id_del
, b.DESCUENTO DESCUENTO_del
from DM_BONIFICACION_PROCUIDADO a
, LKP_REGLAS_PORCENTAJES_PROCUIDADO b
where 
	a.promocion_id= b.id_promocion
	and a.producto_id = b.id_producto
	and date_format(a.dia_id, 'yyyyMM') = b.id_mes
	and cast(a.dia_id as date) = date('{dia}')
    
"""

# Se guarda el resultado del query en un dataframe de pandas para hacer el update

temp = spark.sql(query)

#Se hace join con la fact para poder actualizar las columnas
DM_BONIFICACION_PROCUIDADO = DM_BONIFICACION_PROCUIDADO.join(temp, (DM_BONIFICACION_PROCUIDADO.PRODUCTO_ID == temp.PRODUCTO_ID_del) & (DM_BONIFICACION_PROCUIDADO.PROMOCION_ID==temp.promocion_id_del), 'left') 

DM_BONIFICACION_PROCUIDADO = DM_BONIFICACION_PROCUIDADO.withColumn("PORCENTAJE_DESCUENTO_ESPERADO",when(col("PRODUCTO_ID_del").isNotNull(),col("DESCUENTO_del")).otherwise(col("PORCENTAJE_DESCUENTO_ESPERADO")))
DM_BONIFICACION_PROCUIDADO = DM_BONIFICACION_PROCUIDADO.drop("PRODUCTO_ID_del")
DM_BONIFICACION_PROCUIDADO = DM_BONIFICACION_PROCUIDADO.drop("DESCUENTO_del")
DM_BONIFICACION_PROCUIDADO = DM_BONIFICACION_PROCUIDADO.drop("promocion_id_del")




DM_BONIFICACION_PROCUIDADO = DM_BONIFICACION_PROCUIDADO.withColumn("PORCENTAJE_DESCUENTO_ESPERADO",when(col("DIA_ID").cast(DateType())==dia,col("PORCENTAJE_DESCUENTO")).otherwise(col("PORCENTAJE_DESCUENTO_ESPERADO")))

# DM_BONIFICACION_PROCUIDADO = DM_BONIFICACION_PROCUIDADO.withColumn("BONIFICACION_ESPERADA", when((col("DIA_ID").cast(DateType())==dia3) & (col("REGLA_ID")==1) & (col('TIPO_TRANSACCION') != 'A') & (col('TRANSACCION_INICIAL') != 'A'),None).otherwise(col("BONIFICACION_ESPERADA")))

DM_BONIFICACION_PROCUIDADO = DM_BONIFICACION_PROCUIDADO.withColumn("BONIFICACION_ESPERADA",when((col("DIA_ID").cast(DateType())==dia) & (col("REGLA_ID")==1) ,(col("CUF") * col("PORCENTAJE_DESCUENTO_ESPERADO") * 0.01* col('CANT_UNIDADES'))).otherwise(col("BONIFICACION_ESPERADA")))

DM_BONIFICACION_PROCUIDADO = DM_BONIFICACION_PROCUIDADO.withColumn("BONIFICACION_ESPERADA",when((col("DIA_ID").cast(DateType())==dia) & (col("REGLA_ID")==2)  & (((1-(col("CUF")*col("CANT_UNIDADES"))/((col('PRECIO')*col("CANT_UNIDADES"))/1.19))*100) <= 25) , (col("CANT_UNIDADES")*(col("PRECIO")*col("PORCENTAJE_DESCUENTO_ESPERADO")*0.01)/1.19 ) ).otherwise(col("BONIFICACION_ESPERADA")))
     



# DM_BONIFICACION_PROCUIDADO - Regla 2
query = """
SELECT  /*+ BROADCAST(A) */
       A.PRODUCTO_ID as PRODUCTO_ID_del
     , A.CANT_UNIDADES*((A.PRECIO*(A.PORCENTAJE_DESCUENTO_ESPERADO*0.01))/1.19) as BONIFICACION_ESPERADA_del
FROM   DM_BONIFICACION_PROCUIDADO A
WHERE  A.REGLA_ID = 2
-- AND    (TIPO_TRANSACCION <> 'A' AND TRANSACCION_INICIAL <> 'A')
AND    ((1-(COSTO*CANT_UNIDADES)/((PRECIO*CANT_UNIDADES)/1.19))*100) <= 25
"""

# Se guarda resultado del query en DataFrame de Pandas para hacer el update
temp = spark.sql(query)


DM_BONIFICACION_PROCUIDADO = DM_BONIFICACION_PROCUIDADO.withColumn("BONIFICACION_ESPERADA", when((col("DIA_ID").cast(DateType())==dia) & (col("REGLA_ID")==3) ,(col('COSTO')*(col("PORCENTAJE_DESCUENTO_ESPERADO") *0.01))*col('CANT_UNIDADES')).otherwise(col("BONIFICACION_ESPERADA")))

DM_BONIFICACION_PROCUIDADO = DM_BONIFICACION_PROCUIDADO.withColumn("BONIFICACION_ESPERADA",when((col("DIA_ID").cast(DateType())==dia) & (col("REGLA_ID")==3)  & ((((col("DESCUENTO")*col("CANT_UNIDADES"))/(col('PRECIO')*col("CANT_UNIDADES")))*100) > 25) , (col("CANT_UNIDADES")*(col("CUF")*col("PORCENTAJE_DESCUENTO_ESPERADO")*0.01))).otherwise(col("BONIFICACION_ESPERADA")))



# Actualizar las bonificaciones esperadas para la regla 4
query = f"""
SELECT   
         A.PRODUCTO_ID as PRODUCTO_ID_del
     ,   A.CANT_UNIDADES*B.DESCUENTO as BONIFICACION_ESPERADA_del
FROM     DM_BONIFICACION_PROCUIDADO A
    , lkp_reglas_procuidado B
WHERE    B.ID_REGLA = 4
AND      cast(A.dia_id as date) = date('{dia}')
AND      A.promocion_id = B.id_promocion
AND      B.id_mes = (SELECT MAX(ID_MES) FROM lkp_reglas_procuidado WHERE ID_MES <= DATE_FORMAT (date('{dia}'),'yyyyMM'))
AND      A.producto_id = B.id_producto
AND      A.fabricante_id = B.id_fabricante
--AND      (TIPO_TRANSACCION <> 'A' AND TRANSACCION_INICIAL <> 'A')
"""

# Se guarda resultado del query en DataFrame de Pandas para hacer el update
temp = spark.sql(query)

#Se hace join con la fact para poder actualizar las columnas
DM_BONIFICACION_PROCUIDADO = DM_BONIFICACION_PROCUIDADO.join(temp, DM_BONIFICACION_PROCUIDADO.PRODUCTO_ID == temp.PRODUCTO_ID_del, 'left') 

DM_BONIFICACION_PROCUIDADO = DM_BONIFICACION_PROCUIDADO.withColumn("BONIFICACION_ESPERADA",when(col("PRODUCTO_ID_del").isNotNull(),col('BONIFICACION_ESPERADA_del')).otherwise(col("BONIFICACION_ESPERADA")))
DM_BONIFICACION_PROCUIDADO = DM_BONIFICACION_PROCUIDADO.drop("PRODUCTO_ID_del")
DM_BONIFICACION_PROCUIDADO = DM_BONIFICACION_PROCUIDADO.drop("BONIFICACION_ESPERADA_del")



# Actualizar a 0 la bonificacion esperada
query = f"""
SELECT  /*+ BROADCAST(A,B) */
        A.PRODUCTO_ID as PRODUCTO_ID_del
FROM    DM_BONIFICACION_PROCUIDADO A
   ,    lkp_procuidado_excluir B
WHERE   A.PRODUCTO_ID = B.ID_PRODUCTO
AND     A.PROMOCION_ID = B.ID_PROMOCION
AND     A.TIPO = B.TIPO
AND     A.PORCENTAJE_DESCUENTO = B.PORCENTAJE_DESCUENTO
AND     cast(dia_id as date) = date('{dia}')
"""

# Se guarda resultado del query en DataFrame de Pandas para hacer el update
temp = spark.sql(query)

#Se hace join con la fact para poder actualizar las columnas
DM_BONIFICACION_PROCUIDADO = DM_BONIFICACION_PROCUIDADO.join(temp, DM_BONIFICACION_PROCUIDADO.PRODUCTO_ID == temp.PRODUCTO_ID_del, 'left') 

DM_BONIFICACION_PROCUIDADO = DM_BONIFICACION_PROCUIDADO.withColumn("BONIFICACION_ESPERADA",when(col("PRODUCTO_ID_del").isNotNull(),0).otherwise(col("BONIFICACION_ESPERADA")))
DM_BONIFICACION_PROCUIDADO = DM_BONIFICACION_PROCUIDADO.drop("PRODUCTO_ID_del")




query = f"""
SELECT 
    promocion_id, 
    producto_id, 
    codigo_autorizacion, 
    tipo_transaccion, 
    sum(abs(cant_unidades)) unidades
from
    DM_BONIFICACION_PROCUIDADO  
where 
    mes_id = date('{dia}') -- '$mes_id' 
    and tipo_transaccion = 'C'
    and tipo = 'Frecuente'
group by
    promocion_id, 
    producto_id, 
    codigo_autorizacion, 
    tipo_transaccion
"""

# Se guarda resultado del query en DataFrame de para hacer el update
temp = spark.sql(query)

temp.createOrReplaceTempView('CANJES_ELIMINADOS')


query = f"""
SELECT 
    promocion_id, 
    producto_id, 
    --nro_ticket, 
    codigo_autorizacion, 
    tipo_transaccion, 
    sum(abs(cant_unidades)) unidades
from
    DM_BONIFICACION_PROCUIDADO  
where 
    mes_id = {mes} -- '$mes_id' 
    and tipo_transaccion = 'N'
    and tipo = 'Frecuente'
group by
    promocion_id, 
    producto_id, 
    codigo_autorizacion, 
    tipo_transaccion
    """

# Se guarda resultado del query en DataFrame de para hacer el update
temp = spark.sql(query)
temp.createOrReplaceTempView('NORMALES_ELIMINADOS')

query = """
SELECT 
    a.promocion_id as promocion_id_del
    ,a.producto_id as producto_id_del
    ,a.codigo_autorizacion as codigo_autorizacion_del
    ,a.tipo_transaccion as tipo_transaccion_del
    ,a.unidades as unidades_del
from    
    NORMALES_ELIMINADOS a, CANJES_ELIMINADOS b
where 
    a.promocion_id = b.promocion_id and
    a.producto_id = b.producto_id and
    a.codigo_autorizacion = b.codigo_autorizacion and
    a.unidades = b.unidades
    """

# Se guarda resultado del query en DataFrame de Pandas para hacer el update
temp = spark.sql(query)

DM_BONIFICACION_PROCUIDADO = DM_BONIFICACION_PROCUIDADO.join(temp, (DM_BONIFICACION_PROCUIDADO.PRODUCTO_ID == temp.producto_id_del) & (DM_BONIFICACION_PROCUIDADO.PROMOCION_ID==temp.promocion_id_del) & (DM_BONIFICACION_PROCUIDADO.CODIGO_AUTORIZACION==temp.codigo_autorizacion_del) & (DM_BONIFICACION_PROCUIDADO.TIPO_TRANSACCION=='N') & (DM_BONIFICACION_PROCUIDADO.CANT_UNIDADES<0), 'left') 
DM_BONIFICACION_PROCUIDADO = DM_BONIFICACION_PROCUIDADO.withColumn("TIPO_TRANSACCION",when(col("unidades_del").isNotNull(),'Z').otherwise(col("TIPO_TRANSACCION")))

DM_BONIFICACION_PROCUIDADO = DM_BONIFICACION_PROCUIDADO.drop("promocion_id_del")
DM_BONIFICACION_PROCUIDADO = DM_BONIFICACION_PROCUIDADO.drop("producto_id_del")
DM_BONIFICACION_PROCUIDADO = DM_BONIFICACION_PROCUIDADO.drop("codigo_autorizacion_del")
DM_BONIFICACION_PROCUIDADO = DM_BONIFICACION_PROCUIDADO.drop("tipo_transaccion_del")
DM_BONIFICACION_PROCUIDADO = DM_BONIFICACION_PROCUIDADO.drop("unidades_del")

DM_BONIFICACION_PROCUIDADO = DM_BONIFICACION_PROCUIDADO.filter(DM_BONIFICACION_PROCUIDADO.TIPO_TRANSACCION != 'Z')




###################################### CAMBIOS DE NOMBRE ##########################################


DM_BONIFICACION_PROCUIDADO.createOrReplaceTempView('DM_BONIFICACION_PROCUIDADO')

query = f"""
SELECT 
    PROMOCION_ID as ID_PROMOCION,
    PRODUCTO_ID as ID_PRODUCTO,
    FABRICANTE_ID as ID_FABRICANTE,
    FECHA_HORA_VENTA,
    DIA_ID as FECHA_VENTA,
    MES_ID as ID_MES,
    FILIAL_ID as ID_LOCAL,
    VEND_ID as ID_VENDEDOR,
    NRO_TICKET as NUMERO_DOCUMENTO,
    CODIGO_AUTORIZACION,
    CLIENTE_RUT as RUT_CLIENTE,
    CLIENTE_NOMBRE as NOMBRE_CLIENTE,
    TIPO_TRANSACCION,
    TRANSACCION_INICIAL,
    CANT_UNIDADES as CANTIDAD_UNIDADES,
    PRECIO as PRECIO_UNITARIO,
    COSTO_VENTA_NETO as COSTO_UNITARIO,
    DESCUENTO as DESCUENTO_NETO,
    PORCENTAJE_DESCUENTO as DESCUENTO_PORCENTAJE,
    TIPO,
    REGLA_ID as ID_REGLA,
    BONIFICACION,
    PORCENTAJE_DESCUENTO_ESPERADO as DESCUENTO_PORCENTAJE_ESPERADO,
    BONIFICACION_ESPERADA,
    CUF
    
FROM DM_BONIFICACION_PROCUIDADO
"""

# Se guarda resultado del query en DataFrame de para hacer el update
DM_BONIFICACION_PROCUIDADO = spark.sql(query)


###################################### ESCRITURA A PARQUET ##########################################



DM_BONIFICACION_PROCUIDADO = DM_BONIFICACION_PROCUIDADO.withColumn("ID_PROMOCION", DM_BONIFICACION_PROCUIDADO["ID_PROMOCION"].cast("int"))
DM_BONIFICACION_PROCUIDADO = DM_BONIFICACION_PROCUIDADO.withColumn("ID_PRODUCTO", DM_BONIFICACION_PROCUIDADO["ID_PRODUCTO"].cast("int"))
DM_BONIFICACION_PROCUIDADO = DM_BONIFICACION_PROCUIDADO.withColumn("ID_FABRICANTE", DM_BONIFICACION_PROCUIDADO["ID_FABRICANTE"].cast("int"))
DM_BONIFICACION_PROCUIDADO = DM_BONIFICACION_PROCUIDADO.withColumn("ID_MES", DM_BONIFICACION_PROCUIDADO["ID_MES"].cast("string"))
DM_BONIFICACION_PROCUIDADO = DM_BONIFICACION_PROCUIDADO.withColumn("ID_LOCAL", DM_BONIFICACION_PROCUIDADO["ID_LOCAL"].cast("int"))
DM_BONIFICACION_PROCUIDADO = DM_BONIFICACION_PROCUIDADO.withColumn("ID_VENDEDOR", DM_BONIFICACION_PROCUIDADO["ID_VENDEDOR"].cast("int"))
DM_BONIFICACION_PROCUIDADO = DM_BONIFICACION_PROCUIDADO.withColumn("CANTIDAD_UNIDADES", DM_BONIFICACION_PROCUIDADO["CANTIDAD_UNIDADES"].cast("int"))
DM_BONIFICACION_PROCUIDADO = DM_BONIFICACION_PROCUIDADO.withColumn("NUMERO_DOCUMENTO", DM_BONIFICACION_PROCUIDADO["NUMERO_DOCUMENTO"].cast("int"))
DM_BONIFICACION_PROCUIDADO = DM_BONIFICACION_PROCUIDADO.withColumn("CODIGO_AUTORIZACION", DM_BONIFICACION_PROCUIDADO["CODIGO_AUTORIZACION"].cast("bigint"))
DM_BONIFICACION_PROCUIDADO = DM_BONIFICACION_PROCUIDADO.withColumn("RUT_CLIENTE", DM_BONIFICACION_PROCUIDADO["RUT_CLIENTE"].cast("string"))
DM_BONIFICACION_PROCUIDADO = DM_BONIFICACION_PROCUIDADO.withColumn("NOMBRE_CLIENTE", DM_BONIFICACION_PROCUIDADO["NOMBRE_CLIENTE"].cast("string"))
DM_BONIFICACION_PROCUIDADO = DM_BONIFICACION_PROCUIDADO.withColumn("NOMBRE_CLIENTE", DM_BONIFICACION_PROCUIDADO["NOMBRE_CLIENTE"].cast("string"))
DM_BONIFICACION_PROCUIDADO = DM_BONIFICACION_PROCUIDADO.withColumn("COSTO_UNITARIO", DM_BONIFICACION_PROCUIDADO["COSTO_UNITARIO"].cast("double"))
DM_BONIFICACION_PROCUIDADO = DM_BONIFICACION_PROCUIDADO.withColumn("ID_REGLA", DM_BONIFICACION_PROCUIDADO["ID_REGLA"].cast("int"))
DM_BONIFICACION_PROCUIDADO = DM_BONIFICACION_PROCUIDADO.withColumn("BONIFICACION", DM_BONIFICACION_PROCUIDADO["BONIFICACION"].cast("double"))
DM_BONIFICACION_PROCUIDADO = DM_BONIFICACION_PROCUIDADO.withColumn("DESCUENTO_PORCENTAJE_ESPERADO", DM_BONIFICACION_PROCUIDADO["DESCUENTO_PORCENTAJE_ESPERADO"].cast("int"))
DM_BONIFICACION_PROCUIDADO = DM_BONIFICACION_PROCUIDADO.withColumn("DESCUENTO_PORCENTAJE", DM_BONIFICACION_PROCUIDADO["DESCUENTO_PORCENTAJE"].cast("int"))
DM_BONIFICACION_PROCUIDADO = DM_BONIFICACION_PROCUIDADO.withColumn("BONIFICACION_ESPERADA", DM_BONIFICACION_PROCUIDADO["BONIFICACION_ESPERADA"].cast("double"))





DM_BONIFICACION_PROCUIDADO = DM_BONIFICACION_PROCUIDADO.repartition(1)
#DM_BONIFICACION_PROCUIDADO.write.mode('overwrite').parquet('s3://test-compose-emr529/procuidado/salidas/fact_procuidado/')
DM_BONIFICACION_PROCUIDADO.write.mode('overwrite').parquet('s3://esb-bi.datalake.prod/processed/models/fact_procuidado/')


