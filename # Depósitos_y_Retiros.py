# Databricks notebook source
# MAGIC %md
# MAGIC # 2.Depósitos_y_Retiros_MDN-bplay_PySpark.ipynb
# MAGIC <h> by PySpark
# MAGIC #### Estado: OK **FTD_JUJUY**
# MAGIC
# MAGIC <p> Source: Se realizará la lectura de archivos en formato parquet para obtener los dataframes necesarios para generar la tabla 'Depositos y Retiros', y cargarla en formato Delta Table para su posterior consulta.
# MAGIC <p> Se tomará la información desde el STG1 de Datalake para generar un consumible de STG2 en formato Delta Table.
# MAGIC <p> Inputs:
# MAGIC   <ol>
# MAGIC     <li>Transaction</li>
# MAGIC     <li>Movement</li>
# MAGIC     <li>Operation</li>
# MAGIC     <li>Player</li>
# MAGIC     <li>Transaction S Name</li>
# MAGIC     <li>Transaction T Name</li>
# MAGIC     <li>Payment Platform</li>
# MAGIC     <li>Currency</li>
# MAGIC   </ol>
# MAGIC <p> Output: Delta Table actualizado de "depositos_y_premios" y su carga en el Warehouse de Databricks.

# COMMAND ----------

# MAGIC %md
# MAGIC <h5> Versionados
# MAGIC <p> Se agrego la columna de Diccionario "relacion_partner_transaction_p_t_s_c_p" - Fecha:23/04/2024 - Autor:Ezequiel Vivas
# MAGIC <p> Se agrego la columna "currency_id" y "moneda" - Fecha:23/04/2024 - Autor:Ezequiel Vivas
# MAGIC <p> Se integro la GT "Parana" y "Jujuy" (BPLAYBET_PARANA y BPLAYBET_ARG) - Fecha:06/06/2024 - Autor:Ezequiel Vivas

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Start Up
# MAGIC <p> Etapa de preparación para su ejecución

# COMMAND ----------

# MAGIC %md
# MAGIC ### Declaración de librerias utilizadas
# MAGIC <p> pyspark.sql.functions = Importa funciones de PySpark para manipular y trabajar con columnas y datos en los DataFrames.
# MAGIC <p> pyspark.sql.types = Importa tipos de datos, para definir la estructura de los DataFrames.
# MAGIC <p> import pyspark.sql.functions as F = Importa las funcions de pyspark y abrevia la libreria como "F"

# COMMAND ----------

from pyspark.sql.types import IntegerType, DecimalType, StringType, TimestampType
from pyspark.sql.functions import col, concat, datediff, expr, when, datediff, lit, format_string, current_timestamp, to_timestamp, min, date_sub
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %md
# MAGIC ### Nomenclatura de Fuentes y Auxiliares
# MAGIC <ul>
# MAGIC   <li>df = dataframe</li>
# MAGIC   <li>t = Transaction</li>
# MAGIC   <li>p = Player</li>
# MAGIC   <li>ts = Transaction S Name</li>
# MAGIC   <li>tt = Transaction T Name</li>
# MAGIC   <li>pp = Payment Platform</li>
# MAGIC   <li>c = Currency</li>
# MAGIC   <li>0 = DWBPLAY_PY</li>
# MAGIC   <li>1 = DWBPLAY_SF</li>
# MAGIC   <li>2 = DWBPLAY_CABA</li>
# MAGIC   <li>3 = DWBPLAY_CORDOBA</li>
# MAGIC </ul>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Conexion de Source y Obtención de DataFrames
# MAGIC <p> Descripción: 
# MAGIC <ol>
# MAGIC   <li>Establece la conexión al STG1 del Data Lake.</li>
# MAGIC   <li>Se definen los paths de los archivos parquet "Transaction", "Player", "Transaction S Name", "Transaction T Name", "Payment Platform" y "Currency" para cada Base de Datos DWBPLAY_PY, DWBPLAY_SF, DWBPLAY_CABA, DWBPLAY_CORDOBA y GT Jujuy.</li>
# MAGIC   <li> Luego se leen los archivos parquet para crear los dataframes en formato PySpark.</li>
# MAGIC </ol>

# COMMAND ----------

### Switch Carga Historica/Actualzacion
bl_Carga_Historica = True # Siempre en True por el calculo de FTDs

if bl_Carga_Historica:
    print("se realizara la carga historica de Movement y Operation")
else:
    print("Se actualizara el año corriente")

# COMMAND ----------

  #Get Transaction
df_t0 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_py/OGP/transaction')
df_t1 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_sf/OGP/transaction')
df_t2 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_caba/OGP/transaction')
df_t3 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_cordoba/OGP/transaction')
  #Get Transaction S Name
df_ts0 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_py/OGP/transaction_s_name')
df_ts1 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_sf/OGP/transaction_s_name')
df_ts2 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_caba/OGP/transaction_s_name')
df_ts3 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_cordoba/OGP/transaction_s_name')
  #Get Transaction T Name
df_tt0 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_py/OGP/transaction_t_name')
df_tt1 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_sf/OGP/transaction_t_name')
df_tt2 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_caba/OGP/transaction_t_name')
df_tt3 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_cordoba/OGP/transaction_t_name')
  #Get Payment Platform
df_pp0 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_py/OGP/payment_platform')
df_pp1 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_sf/OGP/payment_platform')
df_pp2 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_caba/OGP/payment_platform')
df_pp3 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_cordoba/OGP/payment_platform')
  #Get Currency
df_c0 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_py/OGP/currency')
df_c1 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_sf/OGP/currency')
df_c2 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_caba/OGP/currency')
df_c3 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_cordoba/OGP/currency')
  #Get Player
df_p0 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_py/OGP/player')
df_p1 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_sf/OGP/player')
df_p2 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_caba/OGP/player')
df_p3 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_cordoba/OGP/player')

if bl_Carga_Historica:
    print("se realizara la carga historica de Movement y Operation")
      # Get DF Movement
    df_m0 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_py/ogp/movement/')
    df_m1 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_sf/ogp/movement/')
    df_m2 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_caba/ogp/movement/')
    df_m3 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_cordoba/OGP/movement/')
      # Get DF Operation
    df_o0 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_py/ogp/operation/')
    df_o1 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_sf/ogp/operation/')
    df_o2 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_caba/ogp/operation/')
    df_o3 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_cordoba/OGP/operation/')
else:
    print("Se actualizara el año corriente")
      # Get DF Movement
    df_m0 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_py/ogp/movement/Year=2025/*')
    df_m1 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_sf/ogp/movement/Year=2025/*')
    df_m2 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_caba/ogp/movement/Year=2025/*')
    df_m3 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_cordoba/OGP/movement/Year=2025/*')
      # Get DF Operation
    df_o0 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_py/ogp/operation/Year=2025/*')
    df_o1 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_sf/ogp/operation/Year=2025/*')
    df_o2 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_caba/ogp/operation/Year=2025/*')
    df_o3 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_cordoba/OGP/operation/Year=2025/*')

# COMMAND ----------

try:
    ### PRN y JUJ ###
    #Get Users GT
  df_usr46 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/bplaybet_arg/dbo/users')
    #Get Deposits GT
  df_dpt46 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/bplaybet_arg/dbo/deposits')
    #Get Withdrawals GT
  df_wdr46 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/bplaybet_arg/dbo/withdrawals')

    ### Switch Carga GT Parana & Jujuy
  bl_Carga_GT = True

except Exception as e:
  print(f"No se pudieron leer las tablas de Apuestas Deportivas GT: {e}")
  bl_Carga_GT = False  


# COMMAND ----------

# Definir los datos
data = [(46, 460), (13, 130), (88, 256)]

# Definir los nombres de las columnas
columns = ["id", "partner"]

# Crear el DataFrame
df_partner_Migra = spark.createDataFrame(data, columns)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Switch PRD/DEV

# COMMAND ----------

#   Variable PROD/DEV
PROD = dbutils.widgets.get("run_job")
print(f"El parametro de PROD es: {PROD}")

PROD = PROD == "True"
print(type(PROD))

# COMMAND ----------

#   Path PRD del Delta Table Depositos y Retiros
delta_table_DyR_prd = "entprod_mdn.bplay.depositos_y_retiros"

#   Path DEV del Delta Table Depositos y Retiros
delta_table_DyR_dev = "entprod_mdn.default.depositos_y_retiros"

#   Variable PROD/DEV
# PROD = False

#   Switch PROD/DEV
if PROD:
    delta_table_DyR = delta_table_DyR_prd
else:
    delta_table_DyR = delta_table_DyR_dev

# Manejar el caso en que la tabla no exista
try:
    # Intentar leer la tabla Delta
    df_origen = spark.read.table(delta_table_DyR)
    count = df_origen.select("partner_transaction_id").count()

    # Validar la tabla AyP
    if count > 0:
        exist = True
    else:
        exist = False
except Exception as e:
    # print(f"La tabla {delta_table_Balance} no existe o no se puede leer: {e}")
    exist = False

# Mostrar el resultado de la validación
#print(f"¿Existe la tabla y tiene datos? {exist}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Create DF Depositos & Retiros ALIRA

# COMMAND ----------

# MAGIC %md
# MAGIC ### Definición de la Función Create_Depositos_Aprobados ALIRA
# MAGIC <p> A partir de las tablas de "transaction", "player", "Transaction S Name", "Transaction T Name", "Payment Platform" y "Currency" obtenidas de DWBPLAY_PY, DWBPLAY_SF, DWBPLAY_CABA y DWBPLAY_CORDOBA. Se creará el dataframe de Depositos Aprobados 

# COMMAND ----------

def depositos_y_retiros(o, m, p, t, ts, tt, pp, c):
    m = m.filter(col('balance_type') == 1)
    o = o.filter(col('type').isin(1, 2))
    dyr = m.join(o, (m['operation'] == o['id']) & (m['player'] == o['player'])) \
            .join(p.filter((col('status') != 1) & (col('type') == 3)), p['id'] == o['player']) \
            .join(t.filter((col('type').isin(1, 2)) & (col('status') == 2)), o['transaction'] == t['id'], 'left') \
            .join(tt.filter(col("language") == 2), t["type"] == tt["type"]) \
            .join(ts.filter(col("language") == 2), t["status"] == ts["status"]) \
            .join(pp, t["platform"] == pp["id"]) \
            .join(c, t["currency"] == c["id"]) \
            .select(
            concat(p['partner'], m['id'], o['id']).alias('id'),
            p['partner'].cast(DecimalType(5,0)).alias('partner_id'),
            o['created_date'].alias('fecha'),
            concat(p['partner'], p['id'].cast("integer")).cast(DecimalType(15,0)).alias('partner_player_id'),
            concat(p['partner'], t['id']).cast(DecimalType(20, 0)).alias('partner_transaction_id'),
            t["currency"].alias("currency_id"),
            c["name"].alias("moneda"),
            when(m["amount"] >= 0, m["amount"]).otherwise(m["amount"]*-1).cast(DecimalType(15, 2)).alias("transaction_amount"),
            t["type"].alias("transaction_type_id"),
            t["status"].alias("transaction_status_id"),
            t["created_date"].alias("transaction_created_date"),
            t["approval_date"].alias("transaction_approval_date"),
            t["sent_date"].alias("transaction_sent_date"),
            t["platform"].alias("platform_id"),
            p["created_date"].alias("fecha_registro"),
            concat(p["partner"],
                    tt["type"],
                    format_string("%02d", ts["status"]),
                    format_string("%02d", t["currency"]),
                    format_string("%08d", t["platform"])
            ).cast(DecimalType(18,0)).alias("relacion_partner_transaction_p_t_s_c_p")
        ).distinct()
    return dyr

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Create DF de Depositos & Retiros GT
# MAGIC <p> A partir de las tablas de "transaction", "player" y "currency" obtenidas de DWBPLAY_PY, DWBPLAY_SF, DWBPLAY_CABA y DWBPLAY_CORDOBA.. Se creará el dataframe de Retiros.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Get Depositos GT

# COMMAND ----------

# Depositos GT
def Create_DF_Depositos_GT(dep, usr, p):
    # Set hour -3 & Filter Depositos Aprobados = 2
    dep_aprobados = dep.filter(col("deposit_status_id") == '2')\
        .withColumn("settled_at", to_timestamp(expr("settled_at - INTERVAL 3 HOURS")))\
            .withColumn("created_at", to_timestamp(expr("created_at - INTERVAL 3 HOURS")))

    # Users
    usr = usr.filter((col("local_register") == '0') & (col("is_backoffice") == '0') & (col("is_services") == '0'))

    # Depositos
    depositos_GT = dep_aprobados.join(usr.alias("usr"), dep_aprobados["user_id"] == usr["id"], "inner")\
                                .join(p.alias("p"), col("usr.bookmaker_id") == col("p.id"), 'left')\
                                .select(
                                    concat(col("p.partner"), lit('01'), dep_aprobados["id"]).alias("id"), # partner_id, transaction_type_id, transaction_id 
                                    col("p.partner").cast(DecimalType(5,0)).alias("partner_id"),
                                    dep_aprobados["settled_at"].cast("date").alias("fecha"),
                                    concat(col("p.partner"), 
                                        when(col("usr.alira_id").isNotNull(), col("usr.alira_id"))
                                        .when(col("usr.bookmaker_id").isin(88), concat(col("usr.bookmaker_id"), format_string("%08d", col("usr.id"))))
                                        .otherwise(col("usr.id"))).alias("partner_player_id"),
                                    concat(col("p.partner"), dep_aprobados["id"]).alias("partner_transaction_id"),
                                    when((col("usr.bookmaker_id").isin(88)) & (dep_aprobados["currency_id"] == 41), lit(7))
                                        .otherwise(dep_aprobados["currency_id"]).alias('currency_id'),
                                    when(dep_aprobados["currency_id"] == '8', 'BRL').when(dep_aprobados["currency_id"] == '41', 'ARS').otherwise("Revisar").alias("moneda"),
                                    dep_aprobados["amount"].cast(DecimalType(15,2)).alias("transaction_amount"),
                                    lit('1').alias("transaction_type_id"),
                                    dep_aprobados["deposit_status_id"].alias("transaction_status_id"),
                                    dep_aprobados["created_at"].alias("transaction_created_date"),
                                    dep_aprobados["settled_at"].alias("transaction_approval_date"),
                                    lit(None).alias("transaction_sent_date"),
                                    dep_aprobados["payment_gateway_id"].alias("platform_id"),
                                    col("usr.created_at").alias("fecha_registro"),
                                    concat(col("p.partner"), 
                                            lit('01'), 
                                            format_string("%02d", dep_aprobados["deposit_status_id"]),
                                            format_string("%02d", when(dep_aprobados["currency_id"] == 8, 9)\
                                                .when(dep_aprobados["currency_id"] == 41, 7))
                                            ).cast(DecimalType(18,0)).alias("relacion_partner_transaction_p_t_s_c_p")
                                )
    return depositos_GT

# COMMAND ----------

# MAGIC %md
# MAGIC ### Get Retiros GT

# COMMAND ----------

# Retiros GT
def Create_DF_Retiros_GT(wdr, usr, p):
        # Filter Depositos Aprobados = 2 & Set hour -3
    wdr_filter = df_wdr46.filter(col("withdrawal_status_id") == '2')\
            .withColumn("created_at", to_timestamp(expr("created_at - INTERVAL 3 HOURS")))\
            .withColumn("approved_at", to_timestamp(expr("approved_at - INTERVAL 3 HOURS")))\
            .withColumn("settled_at", to_timestamp(expr("settled_at - INTERVAL 3 HOURS")))

    usr = usr.filter((col("local_register") == '0') & (col("is_backoffice") == '0') & (col("is_services") == '0'))

        # Calculo de Retiros
    retiros = wdr_filter.join(usr.alias('usr'), usr["id"] == wdr_filter["user_id"], "inner")\
                    .join(p.alias("p"), col("usr.bookmaker_id") == col("p.id"), 'left')\
                        .select(
                            concat(col("p.partner"), lit('02'), wdr_filter["id"]).alias("id"),
                            col("p.partner").cast("decimal(5,0)").alias("partner_id"),
                            wdr_filter["settled_at"].cast("date").alias("fecha"), ##### Revisar cambiar por "approved_at"
                            concat(col("p.partner"), 
                                        when(col("usr.alira_id").isNotNull(), col("usr.alira_id"))
                                        .when(col("usr.bookmaker_id").isin(88), concat(col("usr.bookmaker_id"), format_string("%08d", col("usr.id"))))
                                        .otherwise(col("usr.id"))).alias("partner_player_id"),
                            concat(col("p.partner"), wdr_filter["id"]).alias("partner_transaction_id"),
                            when((col("usr.bookmaker_id").isin(88)) & (wdr_filter["currency_id"] == 41), lit(7))
                            .otherwise(wdr_filter["currency_id"]).alias('currency_id'),
                            when(wdr_filter["currency_id"] == '8', 'BRL').when(wdr_filter["currency_id"] == '41', 'ARS').otherwise("Revisar").alias("moneda"),
                            wdr_filter["amount"].cast("decimal(15,2)").alias("transaction_amount"),
                            lit('2').alias("transaction_type_id"),
                            wdr_filter["withdrawal_status_id"].alias("transaction_status_id"),
                            wdr_filter["created_at"].alias("transaction_created_date"),
                            wdr_filter["approved_at"].alias("transaction_approval_date"),
                            wdr_filter["settled_at"].alias("transaction_sent_date"),
                            wdr_filter["payment_gateway_id"].alias("platform_id"),
                            col("usr.created_at").alias("fecha_registro"),
                            concat(col("p.partner"), lit(0), lit('02'),
                                    format_string("%02d", wdr_filter["withdrawal_status_id"]),
                                    format_string("%02d", when(wdr_filter["currency_id"] == 8, 9)\
                                        .when(wdr_filter["currency_id"] == 41, 7))
                                    ).cast(DecimalType(18,0)).alias("relacion_partner_transaction_p_t_s_c_p")
                        )
    return retiros

# COMMAND ----------

# MAGIC %md
# MAGIC ### Union GT Depositos y Retiros 
# MAGIC <p> Uniendo los dataframe de Depositos y de Retiros, se formara el resultado requerido de Depositos y Retiros.

# COMMAND ----------

def Create_Depositos_Y_Retiros(Dep, Ret):
    # Renombrar columnas en df_Dep para que coincidan con df_Ret 
    Dep_S = Dep.select(
        Dep['id'],
        Dep["partner_id"].cast(DecimalType(5,0)).alias("partner_id"),
        Dep["fecha"].alias("fecha"),
        Dep["partner_player_id"].cast(DecimalType(15,0)).alias("partner_player_id"),
        Dep["partner_transaction_id"].cast(DecimalType(20,0)).alias("partner_transaction_id"),
        Dep["currency_id"],
        Dep["moneda"].alias("moneda"),
        Dep["transaction_amount"].alias("transaction_amount"),
        Dep["transaction_type_id"].cast(DecimalType(2,0)).alias("transaction_type_id"),
        Dep["transaction_status_id"].cast(DecimalType(2,0)).alias("transaction_status_id"),
        Dep["transaction_created_date"].alias("transaction_created_date"),
        Dep["transaction_approval_date"].alias("transaction_approval_date"),
        Dep["transaction_sent_date"].alias("transaction_sent_date"),
        Dep["platform_id"].cast(DecimalType(5,0)).alias("platform_id"),
        Dep["fecha_registro"].alias("fecha_registro"),
        Dep["relacion_partner_transaction_p_t_s_c_p"]
    ).distinct()

    # Renombrar columnas en df_Ret para que coincidan con df_Dep y añadir columnas faltantes con valores null
    Ret_S = Ret.select(
        Ret['id'],
        Ret["partner_id"].cast(DecimalType(5,0)).alias("partner_id"),
        Ret["fecha"].alias("fecha"),
        Ret["partner_player_id"].cast(DecimalType(15,0)).alias("partner_player_id"),
        Ret["partner_transaction_id"].cast(DecimalType(20,0)).alias("partner_transaction_id"),
        Ret["currency_id"].alias("currency_id"),
        Ret["moneda"],
        Ret["transaction_amount"].alias("transaction_amount"),
        Ret["transaction_type_id"].cast(DecimalType(2,0)).alias("transaction_type_id"),
        Ret["transaction_status_id"].cast(DecimalType(2,0)).alias("transaction_status_id"),
        Ret["transaction_created_date"].alias("transaction_created_date"),
        Ret["transaction_approval_date"].alias("transaction_approval_date"),
        Ret["transaction_sent_date"].alias("transaction_sent_date"),
        Ret["platform_id"].cast(DecimalType(5,0)).alias("platform_id"),
        Ret["fecha_registro"].alias("fecha_registro"),
        Ret["relacion_partner_transaction_p_t_s_c_p"]
    ).distinct()

    # Unir los DataFrames usando unionByName
    DyR = Dep_S.unionByName(Ret_S).orderBy('transaction_created_date')

    return DyR

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Union de Dataframe Depositos y Retiros

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ejecución del DataFrame de Depositos y Retiros por Base de Datos: DWBPLAY_PY, DWBPLAY_SF, DWBPLAY_CABA y DWBPLAY_CORDOBA.
# MAGIC <p> Se ejecuta la función Create_DF_Retiros para obtener un dataframe con la información de los retiros de las bases de datos DWBPLAY_PY, DWBPLAY_SF, DWBPLAY_CABA y DWBPLAY_CORDOBA con Opcion de GT (Jujuy, Parana y Migración de Mendoza)

# COMMAND ----------

# DWBPLAY_PY
df_DyR0 = depositos_y_retiros(df_o0, df_m0, df_p0, df_t0, df_ts0, df_tt0, df_pp0, df_c0)
# DWBPLAY_SF
df_DyR1 = depositos_y_retiros(df_o1, df_m1, df_p1, df_t1, df_ts1, df_tt1, df_pp1, df_c1)
# DWBPLAY_CABA
df_DyR2 = depositos_y_retiros(df_o2, df_m2, df_p2, df_t2, df_ts2, df_tt2, df_pp2, df_c2)
# DWBPLAY_CORDOBA
df_DyR3 = depositos_y_retiros(df_o3, df_m3, df_p3, df_t3, df_ts3, df_tt3, df_pp3, df_c3)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ejecución del DataFrame de Depositos y Retiros GT

# COMMAND ----------

if bl_Carga_GT:
    # PRN y JUJ y MZA
    df_Dep_GT = Create_DF_Depositos_GT(df_dpt46, df_usr46, df_partner_Migra)
    df_Ret_GT = Create_DF_Retiros_GT(df_wdr46, df_usr46, df_partner_Migra)
    # GT Jujuy & Parana & Mendoza
    df_DyR_GT = Create_Depositos_Y_Retiros(df_Dep_GT, df_Ret_GT)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Union de DF Depositos y Retiros
# MAGIC <p> Se realiza la unificacion de las ejecuciones de las bases de datos DWBPLAY_PY, DWBPLAY_SF, DWBPLAY_CABA y DWBPLAY_CORDOBA.

# COMMAND ----------

if bl_Carga_GT:
    # GT Jujuy & Parana
    ### Switch Carga PRD/DEV
    # Union PRD
    df_DyR_union = df_DyR0.unionByName(df_DyR1).unionByName(df_DyR2).unionByName(df_DyR3).unionByName(df_DyR_GT)
else:
    ### Switch Carga PRD/DEV
    # Union PRD
    df_DyR_union = df_DyR0.unionByName(df_DyR1).unionByName(df_DyR2).unionByName(df_DyR3)

if PROD:
    print(f"Se actualizará en PRD {delta_table_DyR}")   
else:
    print(f"Se actualizará en DEV {delta_table_DyR}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Auxiliar para IDs Duplicados

# COMMAND ----------

# from pyspark.sql.functions import col, count

# ## Se contabilizan los IDs duplicados
# df_duplicados = df_DyR2.groupBy(col('id')).agg(count('*').alias('count')).filter(col('count') > 1)
# display(df_duplicados)

# COMMAND ----------

# from pyspark.sql.functions import col, count

# ## Se contabilizan los IDs duplicados
# df_duplicados = df_DyR_union.groupBy(col('id')).agg(count('*').alias('count')).filter(col('count') > 1)
# display(df_duplicados)
# # display(df_DyR_union.filter((col('id') == '250127496657129925973')))

# ## Borrar Auxiliar por duplicado en base
# df_DyR_union = df_DyR_union.filter((col('id') != '250127496657129925973') & (col('fecha') != '2025-04-06'))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Get FTDs
# MAGIC <p> Se genera un Dataframe de FTDs

# COMMAND ----------

# MAGIC %md
# MAGIC ### Definición de la Función Create_DF_FTD
# MAGIC <p> Esta función genera un dataframe con la información de las transacciones FTD de los players.

# COMMAND ----------

# MAGIC %md
# MAGIC ### FTDs Alira 

# COMMAND ----------

# Function Create_DF_FTD
def Create_DF_FTD(t, p):
    t = t.filter((col("type") == 1) & (col("status") == 2))

    # transaction aux
    aux = t.groupBy("player").agg(min("approval_date").alias("approval_date"))
    aux = aux.select(aux["player"].cast(DecimalType(15,0)), aux["approval_date"])

    # player
    p = p.filter(p["status"] != 1)

    # transaction
    t = t.select(t["player"].cast(DecimalType(15, 0)), t["id"].cast(DecimalType(15, 0)), t["created_date"], t["approval_date"])

    t_ftd = t.join(aux, t["player"] == aux["player"]).select(aux["player"], t["id"], aux["approval_date"])

    t_ftd = t_ftd.groupBy("player").agg(min("id").alias("FTD_transaction_id"), min("approval_date").alias("FTD_approval_date"))

    # Create DF FTD
    ftd = t_ftd.join(p, t_ftd["player"] == p["id"]) \
    .select(
        p["partner"].cast(DecimalType(15, 0)).alias("partner_id"),
        t_ftd["player"].cast(DecimalType(15, 0)).alias("player_id"),
        concat(p["partner"], t_ftd["player"]).cast(DecimalType(15, 0)).alias("partner_player_id"),
        t_ftd["FTD_transaction_id"],
        concat(p["partner"], t_ftd["FTD_transaction_id"]).cast(DecimalType(20, 0)).alias("FTD_partner_transaction_id"),
        t_ftd["FTD_approval_date"]
    ).distinct()

    ftd = ftd.groupBy("partner_player_id", "FTD_partner_transaction_id") \
    .agg(min("FTD_approval_date").alias("FTD_approval_date"))

    return ftd


# COMMAND ----------

# MAGIC %md
# MAGIC ### FTDs GT

# COMMAND ----------

def getFTDs_GT(df):
    depositos_aprobados = df.filter(
        (col("transaction_type_id") == 1) & 
        (col("transaction_status_id") == 2)
    )

    # transaction aux
    ftd = depositos_aprobados.groupBy("partner_player_id").agg(
        min("transaction_approval_date").alias("FTD_approval_date")
    )

    result_ftd = ftd.alias('ftd').join(
        depositos_aprobados.alias('da'),
        (col('ftd.partner_player_id') == col('da.partner_player_id')) & 
        (col('ftd.FTD_approval_date') == col('da.transaction_approval_date'))
    ).groupBy(
        'da.partner_id',
        'ftd.partner_player_id',
        'ftd.FTD_approval_date'
    ).agg(
        min("da.partner_transaction_id").alias("FTD_partner_transaction_id")
    )

    return result_ftd

# COMMAND ----------

# MAGIC %md
# MAGIC ### Union FTDs

# COMMAND ----------

def Calcular_FTD(df):
    # Paso 1: Obtener la primera fecha de FTD por jugador
    primer_ftd_fecha = df.groupBy("partner_player_id") \
        .agg(min("FTD_approval_date").alias("primera_FTD_fecha"))

    # Paso 2: Hacer join con el dataframe original para recuperar el resto de los campos
    primer_ftd_completo = df.join(
        primer_ftd_fecha,
        (df.partner_player_id == primer_ftd_fecha.partner_player_id) &
        (df.FTD_approval_date == primer_ftd_fecha.primera_FTD_fecha),
        "inner"
    ).select(df["*"])

    return primer_ftd_completo

# COMMAND ----------

# DWBPLAY_PY
df_FTD0 = Create_DF_FTD(df_t0, df_p0)
# DWBPLAY_SF
df_FTD1 = Create_DF_FTD(df_t1, df_p1)
# DWBPLAY_CABA
df_FTD2 = Create_DF_FTD(df_t2, df_p2)
# DWBPLAY_CORDOBA
df_FTD3 = Create_DF_FTD(df_t3, df_p3)
# FTDs GT
df_FTD_GT = getFTDs_GT(df_DyR_GT).select('partner_player_id', 'FTD_partner_transaction_id', 'FTD_approval_date')

# FTDs Union
df_FTDs_union = df_FTD0.unionByName(df_FTD1).unionByName(df_FTD2).unionByName(df_FTD3).unionByName(df_FTD_GT)

# FTDs Final
df_FTDs = Calcular_FTD(df_FTDs_union)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Create DF Depositos y Retiros con FTDs
# MAGIC <p> Teniendo los dataframe FTD y Depositos Aprobados, se creará el dataframe de Depositos FTD.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Get DyR w/FTDs
# MAGIC <p> Esta función genera un dataframe con la información de las transacciones FTD de los players.

# COMMAND ----------

def Create_DF_DyR_FTD(df, ftd):
    dep = df.filter(col('transaction_type_id') == 1)
    ret = df.filter(col('transaction_type_id') == 2)
    #dep_ftd = dep.join(ftd, ((ftd["partner_player_id"] == dep["partner_player_id"]) & (ftd["FTD_approval_date"] == dep["transaction_approval_date"])), "left") \
    dep_ftd = dep.join(ftd, ((ftd["partner_player_id"] == dep["partner_player_id"])), "left") \
        .select(
            dep['id'],
            dep["partner_id"],
            dep["fecha"],
            dep["partner_player_id"],
            dep["partner_transaction_id"],
            dep["currency_id"],
            dep["moneda"],
            dep["transaction_amount"].cast(DecimalType(15,2)),
            dep["transaction_type_id"].cast(DecimalType(15,0)),
            dep["transaction_status_id"].cast(DecimalType(15,0)),
            dep["transaction_created_date"],
            dep["transaction_approval_date"],
            dep["transaction_sent_date"],
            dep["platform_id"].cast(DecimalType(5,0)),
            dep["fecha_registro"],
            when(dep["transaction_status_id"] == 2, ftd["FTD_approval_date"]).otherwise(lit(None)).alias('FTD_approval_date'),
            when(dep["transaction_status_id"] == 2, ftd["FTD_partner_transaction_id"]).otherwise(lit(None)).alias('FTD_partner_transaction_id'),
            dep["relacion_partner_transaction_p_t_s_c_p"]
        ).distinct()

    DyR = dep_ftd.unionByName(ret.withColumn('FTD_approval_date', lit(None).cast(TimestampType()))\
                          .withColumn('FTD_partner_transaction_id', lit(None).cast(DecimalType(20,0)))
                            )

    # Add Column "dias_registro"
    DyR = DyR.withColumn("dias_registro", when((col("fecha_registro").isNull()) | (col("transaction_approval_date").isNull()), None)\
            .otherwise(datediff(col("transaction_approval_date"), col("fecha_registro"))).cast(DecimalType(10, 0)))
    
    # Add Column "Categoria_FTD"
    DyR_cantFTD = DyR.withColumn("Categoria_FTD", when(DyR["dias_registro"].isNull(), lit(None)).when(datediff(col("FTD_approval_date"), col("fecha_registro")) <= 7, "FTD Primera Semana").otherwise("FTD Resto"))

    result = DyR_cantFTD.withColumn("Categoria_Deposito",
        when(col("partner_transaction_id") == col("FTD_partner_transaction_id"), "FTD")
        .otherwise(
            when(datediff(col("transaction_approval_date"), col("FTD_approval_date")) <= 30, "TD 30 días")
            .otherwise(
                when(datediff(col("transaction_approval_date"), col("FTD_approval_date")) <= 60, "TD 60 días")
                .otherwise(
                    when(datediff(col("transaction_approval_date"), col("FTD_approval_date")) <= 90, "TD 90 días")
                    .otherwise("TD Resto")
                ))))
    
    return result.select("id", "partner_id", "fecha", "partner_player_id", "partner_transaction_id", "currency_id", "moneda", "transaction_amount", "transaction_type_id", "transaction_status_id", "transaction_created_date", "transaction_approval_date", "transaction_sent_date", "platform_id", "fecha_registro", "dias_registro", "FTD_approval_date", "FTD_partner_transaction_id", "Categoria_FTD", "Categoria_Deposito","relacion_partner_transaction_p_t_s_c_p")

# COMMAND ----------

df_DyR_wFTDs = Create_DF_DyR_FTD(df_DyR_union, df_FTDs)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Calculo de Cotizacion USD "amount_USD"

# COMMAND ----------

# Get Tipo de Cambio
df_Tipo_de_Cambio = spark.read.table("entprod_mdn.bplay.tipos_de_cambio").select("BASE", col("Fecha").alias("Fecha_Cot"), "ARS_CCL", "ARS", "BRL", "PYG")

# COMMAND ----------

def Calcular_amountUSD(DyR, tc):
        # Se le agrega la columna "Fecha_DyR", para poder realizar el JOIN
    DyR_F = DyR.withColumn("Fecha_DyR", col("fecha").cast("date"))
        # Calculo de amoun_USD
    DyR_tc = DyR_F.join(tc,  tc["Fecha_Cot"] == DyR_F["Fecha_DyR"] , "left").select(
                                                    "id",
                                                    "partner_id", 
                                                    col("fecha").cast("date"), 
                                                    "partner_player_id", 
                                                    "partner_transaction_id", 
                                                    "currency_id", 
                                                    "moneda", 
                                                    "transaction_amount", 
                                                    "transaction_type_id",
                                                    "transaction_status_id",
                                                    "transaction_created_date",
                                                    "transaction_approval_date",
                                                    "transaction_sent_date",
                                                    "platform_id",
                                                    "fecha_registro",
                                                    "dias_registro",
                                                    "FTD_approval_date",
                                                    col("FTD_partner_transaction_id").cast(StringType()),
                                                    "Categoria_FTD",
                                                    "Categoria_Deposito",
                                                    "relacion_partner_transaction_p_t_s_c_p",
                                                    when((col("currency_id") == "7") | (col("currency_id") == "41"), (col("transaction_amount")/col("ARS_CCL")))\
                                                        .when(col("currency_id") == "15", (col("transaction_amount")/col("PYG")))\
                                                            .when((col("currency_id") == "9") | (col("currency_id") == "8"), (col("transaction_amount")/col("BRL")))\
                                                                .otherwise(col("transaction_amount")).cast(DecimalType(25, 2)).alias("amount_USD")
                                                    )
    return DyR_tc

# COMMAND ----------

df_DyR_wFTDs_wTC = Calcular_amountUSD(df_DyR_wFTDs, df_Tipo_de_Cambio)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Preparación de la Tabla Depositos y Retiros para la carga

# COMMAND ----------

def get_dif(nuevo):
    # Obtengo el df de la tabla Player ya creada
    origen = spark.read.table(delta_table_DyR)

    # Selecciono los campos por comparar
    origen = origen.select("id", "partner_id", "fecha", "partner_player_id", "partner_transaction_id", "currency_id", "moneda", "transaction_amount", "transaction_type_id", "transaction_status_id", "transaction_created_date", "transaction_approval_date", "transaction_sent_date", "platform_id", "fecha_registro", "dias_registro", "FTD_approval_date", "FTD_partner_transaction_id", "Categoria_FTD", "Categoria_Deposito", "amount_USD", "relacion_partner_transaction_p_t_s_c_p")

    # Obtengo las filas nuevas y actualizadas 
    dif = nuevo.exceptAll(origen)

    # Agrega la columna de actualización
    res = dif.withColumn("fecha_actualizacion", current_timestamp()).withColumn("fecha_creacion", col("transaction_created_date"))
    
    return res

# COMMAND ----------

def filter_last_15_days(df): # Filtra los ultimos 15 días
    last_15_days = concat(date_sub(current_timestamp(), 90).cast("string"),lit(" 23:59:59")).cast("timestamp")
    result = df.filter(col("fecha") >= last_15_days)
    return result

# COMMAND ----------

def filter_last_day(df): # Filtra hasta las  23:59:59 del dia anterior a la ejecucion
    yesterday_end = concat(date_sub(current_timestamp(), 1).cast("string"),lit(" 23:59:59")).cast("timestamp")
    result = df.filter(col("fecha") <= yesterday_end)
    return result

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Upsert de Tabla Depositos y Retiros
# MAGIC <p> Si ya existe la Delta Table de Depositos y Retiros creada se obtendra la tabla y se actualizará con el DataFrame generado en esta ejecución.
# MAGIC <p> Si no existiera, se definirá la limitación las particiones en 8, y se generará un nuevo Delta Table de "Depositos y Retiros".

# COMMAND ----------

print(delta_table_DyR)
print(exist)
print('Se ejecutará en PRD' if PROD else 'Se ejecutará en DEV')
print('Se realizará la Carga Historica' if bl_Carga_Historica else 'Se cargara el año corriente')

# COMMAND ----------

# Verificar si la ruta contiene una tabla Delta
if exist:
    # Se Obtiene las filas por actualizar
    df_DyR_dif = get_dif(df_DyR_wFTDs_wTC)
    # Se obtiene las actualizaciones de los ultimos 15 días
    df_DyR_dif = filter_last_15_days(df_DyR_dif)
    # Se elimina hasta el ultimo día de ejecucion
    df_DyR_dif = filter_last_day(df_DyR_dif)

    # Si la ruta contiene una tabla Delta, creamos el objeto DeltaTable
    dt_origen = DeltaTable.forName(spark, delta_table_DyR)

    # Upsert del DataFrame de Depositos y Retiros
    dt_origen.alias("dest").merge(source=df_DyR_dif.alias("update"), condition="dest.id = update.id")\
            .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
else:
    # Agregado de las columnas identificatorias para el upgrade de la base
    df_DyR = df_DyR_wFTDs_wTC \
        .withColumn("fecha_actualizacion", current_timestamp())\
            .withColumn("fecha_creacion", col("transaction_created_date"))

    # Limitar a 8 particiones el DF "df_DyR"
    df_DyR = df_DyR.coalesce(8)
    # Guardar el DataFrame en formato Delta con overwrite habilitado
    df_DyR.write \
        .mode("overwrite") \
                .saveAsTable(delta_table_DyR)

# COMMAND ----------

# delta_table_DyR = 'entprod_mdn.default.depositos_y_retiros'

# # Agregado de las columnas identificatorias para el upgrade de la base
# df_DyR = df_DyR_wFTDs_wTC \
#     .withColumn("fecha_actualizacion", current_timestamp())\
#         .withColumn("fecha_creacion", col("transaction_created_date"))

# # Limitar a 8 particiones el DF "df_DyR"
# df_DyR = df_DyR.coalesce(8)
# # Guardar el DataFrame en formato Delta con overwrite habilitado
# df_DyR.write \
#     .mode("overwrite") \
#             .saveAsTable(delta_table_DyR)