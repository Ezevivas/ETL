# Databricks notebook source
# MAGIC %md
# MAGIC # Otras_Operaciones_MDN-bplay_PySpark.ipynb
# MAGIC <h> by PySpark
# MAGIC #### Estado: **OK**
# MAGIC <p> Source: Se realizará la lectura de archivos en formato parquet para obtener los dataframes necesarios para generar la tabla 'Otras Operaciones', y cargarla en formato Delta Table para su posterior consulta.
# MAGIC <p> Se tomará la información desde el STG1 de Datalake para generar un consumible de STG2 en formato Delta Table.
# MAGIC <p> Inputs:
# MAGIC   <ol>
# MAGIC     <li>Operation</li>
# MAGIC     <li>Movement</li>
# MAGIC     <li>Player</li>
# MAGIC   </ol>
# MAGIC <p> Output: Delta Table actualizado de "otras_operaciones" y su carga en el Warehouse de Databricks.

# COMMAND ----------

# MAGIC %md
# MAGIC <h5> Versionados
# MAGIC <p> Se agrego la columna "partner_operation_id" realizar con exito el Upsert - Fecha:21/03/2024 - Autor:Ezequiel Vivas

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Start Up
# MAGIC <p> Etapa de preparación para su ejecución

# COMMAND ----------

# MAGIC %md
# MAGIC ### Declaración de librerias utilizadas
# MAGIC <p> pyspark.sql.functions = Importa funciones de PySpark para manipular y trabajar con columnas y datos en los DataFrames.
# MAGIC <p> pyspark.sql.types = Importa tipos de datos, para definir la estructura de los DataFrames.

# COMMAND ----------

from pyspark.sql.functions import col, concat, datediff, expr, lit, when, month, year, format_string, current_date, to_date, to_timestamp, current_timestamp, date_sub, date_format, from_utc_timestamp, trunc
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, DateType, TimestampType, LongType, DecimalType
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %md
# MAGIC ### Nomenclatura de Fuentes y Auxiliares
# MAGIC <ul>
# MAGIC   <li>df = dataframe</li>
# MAGIC   <li>o = operation</li>
# MAGIC   <li>m = movement</li>
# MAGIC   <li>p = player</li>
# MAGIC   <li>0 = DWBPLAY_PY</li>
# MAGIC   <li>1 = DWBPLAY_SF</li>
# MAGIC   <li>2 = DWBPLAY_CABA</li>
# MAGIC   <li>3 = DWBPLAY_CORDOBA</li>
# MAGIC   <li>fm = Fact Movement
# MAGIC </ul>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Conexion de Source y Obtención de DataFrames
# MAGIC <p> Descripción: 
# MAGIC <ol>
# MAGIC   <li>Establece la conexión al STG1 del Data Lake.</li>
# MAGIC   <li>Se definen los paths de los archivos parquet "Operation", "Movement" y "Player" para cada Base de Datos DWBPLAY_PY, DWBPLAY_SF, DWBPLAY_CABA y DWBPLAY_CORDOBA.</li>
# MAGIC   <li> Luego se leen los archivos parquet para crear los dataframes en formato PySpark.</li>
# MAGIC </ol>

# COMMAND ----------

### Switch Carga Historica/Actualzacion
bl_Carga_Historica = False 

if bl_Carga_Historica:
    print("se realizara la carga historica de Movement y Operation")
else:
    print("Se actualizara el año corriente")

# COMMAND ----------

# Asignación variable para el año corriente
  # Creo filtro de 90 días para atrás
  # last_90_days = concat(date_sub(current_timestamp(), 91).cast("string"),lit(" 23:59:59")).cast("timestamp")  
last_90_days = trunc(date_sub(current_timestamp(), 91), "month")  

## Query Fact Movement
  #Get DF Player
df_p0 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_py/OGP/player')
df_p1 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_sf/OGP/player')
df_p2 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_caba/OGP/player').filter(col("partner") != 255)
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
    df_m0 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_py/ogp/movement/') \
                              .filter(col("created_date") >=  last_90_days)
    df_m1 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_sf/ogp/movement/') \
                              .filter(col("created_date") >=  last_90_days)
    df_m2 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_caba/ogp/movement/') \
                              .filter(col("created_date") >=  last_90_days)
    df_m3 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_cordoba/OGP/movement/') \
                              .filter(col("created_date") >=  last_90_days)
                              
      # Get DF Operation
    df_o0 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_py/ogp/operation/') \
                              .filter(col("created_date") >=  last_90_days)
    df_o1 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_sf/ogp/operation/') \
                              .filter(col("created_date") >=  last_90_days)
    df_o2 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_caba/ogp/operation/') \
                              .filter(col("created_date") >=  last_90_days)
    df_o3 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_cordoba/OGP/operation/') \
                              .filter(col("created_date") >=  last_90_days)


try:
    if bl_Carga_Historica:
        print("se realizara la carga historica de Movement y Operation")

                  #Get bonus_users
        df_bu = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/bplaybet_arg/dbo/bonus_users')
          #Get users
        df_u = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/bplaybet_arg/dbo/users').select('id', 'bookmaker_id', 'alira_id', 'currency_id')
          #Get transactions
        # salva el error de definición del parquet en el unity_catalog pero elimina operaciones 
        # df_t = spark.read.option("mergeSchema", "true") \
        #                   .parquet("/Volumes/boldtproddatalake/boldtdatalake/bplaybet_arg/dbo/transactions/") \
        #                   .filter(col("transaction_type_id") == 22) \
        #                     .filter(col("transaction_status_id") == 1) # revisar cambio definición reunión desarrollo
        #                               # v20250606
          #Get transactions
        # salva el error de definición del parquet en el unity_catalog
        df_t = spark.read.parquet("/Volumes/boldtproddatalake/boldtdatalake/bplaybet_arg/dbo/transactions/") \
                          .filter(col("transaction_type_id") == 22) \
                            .filter(col("transaction_status_id") == 1) # revisar cambio definición reunión desarrollo
                                      # v20250606

          #Get wallets
        df_w = spark.read.parquet("/Volumes/boldtproddatalake/boldtdatalake/bplaybet_arg/dbo/wallets/")
        
          ### Switch Carga GT Parana, Jujuy y Mza
        bl_Carga_GT = True
    
    else:
        print("Se actualizara el año corriente")

          #Get bonus_users
        df_bu = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/bplaybet_arg/dbo/bonus_users') \
                                  .filter(col("created_at") >=  last_90_days)
          #Get users
        df_u = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/bplaybet_arg/dbo/users') \
                                  .filter(col("created_at") >=  last_90_days) \
                                    .select('id', 'bookmaker_id', 'alira_id', 'currency_id')
          #Get transactions
        # salva el error de definición del parquet en el unity_catalog
        # df_t = spark.read.option("mergeSchema", "true") \
        #                   .parquet("/Volumes/boldtproddatalake/boldtdatalake/bplaybet_arg/dbo/transactions/") \
        #                     .filter(col("created_at") >=  last_90_days) \
        #                       .filter(col("transaction_type_id") == 22) \
        #                         .filter(col("transaction_status_id") == 1) # revisar cambio definición reunión desarrollo
        #                               # v20250606
          #Get transactions
        # salva el error de definición del parquet en el unity_catalog
        df_t = spark.read.parquet("/Volumes/boldtproddatalake/boldtdatalake/bplaybet_arg/dbo/transactions/") \
                          .filter(col("created_at") >=  last_90_days) \
                            .filter(col("transaction_type_id") == 22) \
                              .filter(col("transaction_status_id") == 1) # revisar cambio definición reunión desarrollo
                                      # v20250606

          #Get wallets
        df_w = spark.read.parquet("/Volumes/boldtproddatalake/boldtdatalake/bplaybet_arg/dbo/wallets/")

          ### Switch Carga GT Parana, Jujuy y Mza
        bl_Carga_GT = True

except Exception as e:
    print(f"No se pudieron leer las tablas de GT-bplaybet: {e}")
    bl_Carga_GT = False

# COMMAND ----------

# MAGIC %md
# MAGIC ### Switch PROD/DEV

# COMMAND ----------

#   Variable PROD/DEV
PROD = dbutils.widgets.get("run_job")
print(f"El parametro de PROD es: {PROD}")

PROD = PROD == "True"
print(type(PROD))

# COMMAND ----------

#Path DEV del Delta Table Otras Operaciones
delta_table_OO_prd = 'entprod_mdn.bplay.otras_operaciones'

#Path PRD del Delta Table Otras Operaciones
delta_table_OO_dev = 'entprod_mdn.default.otras_operaciones'

# #   Variable PROD/DEV
# PROD = True

#   Switch PROD/DEV
if PROD:
    delta_table_OtrasOperaciones = delta_table_OO_prd
else:
    delta_table_OtrasOperaciones = delta_table_OO_dev

# COMMAND ----------

# Manejar el caso en que la tabla no exista
try:
    # Get Delta Table Otras Operaciones
    df_origen = spark.read.table(delta_table_OtrasOperaciones)
    count = df_origen.select("partner_operation_id").count()

    # Validar la tabla Fact Coupon Row
    if count > 0:
        exist = True
    else:
        exist = False
except Exception as e:
    # print(f"La tabla {delta_table_Balance} no existe o no se puede leer: {e}")
    exist = False

print(delta_table_OtrasOperaciones)
print(exist)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tabla Auxiliar Migración

# COMMAND ----------

# Definir los datos
# data = [(46, "460"), (13, "130"), (88, "25688")]
data = [(46, "460"), (13, "130"), (88, "256")]

# Definir los nombres de las columnas
columns = ["id", "partner"]

# Crear el DataFrame
df_partner_Migra = spark.createDataFrame(data, columns)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. DF Otras Operaciones
# MAGIC <p> Esta función genera un dataframe con la información de las verticales de las salas.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Definición de la Función Create_DF_Otras_Operaciones
# MAGIC <p> Esta función genera un dataframe con la información de las verticales de las salas.

# COMMAND ----------

def Create_DF_Otras_Operaciones(o, m, p):
    # Filter
    o = o.filter(~col("type").isin([1, 2, 3, 4]))
    m = m.filter(F.col("balance_type") != 15)
    p = p.filter(F.col("status") != 1)

    # Join 1
    fm1 = m.join(o, (o["id"] == m["operation"]), "inner")\
        .select(
            o["created_date"],
            o["id"].alias("operation_id"),
            m["id"].alias("movement_id"),
            o["player"],
            o["transaction"],
            F.to_date(o["created_date"]).alias("fecha_operacion"),
            o["type"].alias("operation_type_id"),
            m["balance_type"].alias("balance_type_id"),
            o["room"],
            o["event_reference"],
            o["administrator"],
            m["amount"].cast(DecimalType(25, 2)).alias("movement_amount"),
            m["promotion"],
            m["currency"].alias("currency_id")
        ).distinct()
    
    # Join 2
    fm2 = fm1.join(p, fm1["player"] == p["id"])\
        .select(
            fm1["created_date"],
            p["partner"].cast(DecimalType(15,0)).alias("partner_id"),
            fm1["fecha_operacion"],
            F.concat(p["partner"], fm1["operation_id"]).cast(DecimalType(20,0)).alias("partner_operation_id"),
            F.concat(p["partner"], p["id"]).cast(DecimalType(20,0)).alias("partner_player_id"),
            p["id"].cast(DecimalType(15,0)).alias("player_id"),
            fm1["operation_type_id"].cast(DecimalType(15,0)),
            fm1["operation_id"].cast(DecimalType(15,0)),
            F.concat(p["partner"], fm1["movement_id"]).cast(DecimalType(20,0)).alias("partner_movement_id"),
            fm1["movement_id"].cast(DecimalType(15,0)),
            F.when(fm1["transaction"].isNull(), None).otherwise(F.concat(p["partner"], fm1["transaction"])).cast(DecimalType(20,0)).alias("partner_transaction_id"),
            fm1["transaction"].cast(DecimalType(15,0)).alias("transaction_id"),
            fm1["balance_type_id"],
            fm1["event_reference"],
            fm1["movement_amount"],
            fm1["currency_id"].cast(DecimalType(15,0)),
            F.when(fm1["room"].isNull(), None).otherwise(F.concat(p["partner"], fm1["room"])).alias("partner_room_id"),
            F.when(fm1["room"].isNull(), None).otherwise(fm1["room"]).alias("room_id"),
            F.when(fm1["administrator"].isNull(), None).otherwise(F.concat(p["partner"], fm1["administrator"])).alias("partner_administrator_id"),
            F.when(fm1["administrator"].isNull(), None).otherwise(fm1["administrator"]).alias("administrator_id"),
            F.when(fm1["administrator"] <= 0, None).otherwise(F.concat(p["partner"], fm1["promotion"])).alias("partner_promotion_id"),
            F.when(fm1["administrator"] <= 0, None).otherwise(fm1["promotion"]).alias("promotion_id")
        ).distinct()

    # Add Column "partner_event_reference"
    Otras_Operaciones = fm2.withColumn("partner_event_reference", F.when((F.col("room_id").isNull()) | (F.col("event_reference") == 0), None).otherwise(F.concat(F.col("partner_id"), F.col("event_reference"))))

    return Otras_Operaciones

# COMMAND ----------

def Create_DF_Otras_Operaciones_GT(u, bu, t, p, w):
    # Filter
    # u = u.filter(col("local_register") == 0).filter(col("is_backoffice") == 0).filter(col("is_services") == 0)
    bu = bu.filter(col("released_at").isNotNull()).filter(col("approved") == 1).filter(col("balance") > 0)

    # Obtener Conversión a Efectivo
    Conv_Efvo_GT = bu.join(u, (u["id"] == bu["user_id"]), "inner")\
                        .join(p, (p["id"] == u["bookmaker_id"]), "inner")\
                    .select(
                        date_format(from_utc_timestamp(bu["created_at"], 'America/Buenos_Aires'), 'yyyy-MM-dd HH:mm:ss').alias('created_date'), #cambiar fecha utc a la zona horaria y escribirla en formato timestamp
                        p["partner"].cast(IntegerType()).alias("partner_id"),
                        date_format(from_utc_timestamp(bu["released_at"], 'America/Buenos_Aires'), 'yyyy-MM-dd HH:mm:ss').cast(TimestampType()).alias('fecha_operacion'), #cambiar fecha utc a la zona horaria y escribirla en formato timestamp
                        when(u["bookmaker_id"].isin(88), concat(p['partner'], format_string("%012d", bu["bonus_id"])))
                            .otherwise(concat(p['partner'], bu["bonus_id"])).cast(LongType()).alias("partner_operation_id"),
                        concat(p['partner'],
                                when(u["alira_id"].isNotNull(), u["alira_id"])
                                .when(u["bookmaker_id"].isin(88), concat(u["bookmaker_id"], format_string("%08d", u["id"])))
                                .otherwise(u["id"])).alias("partner_player_id"),
                        when(u['alira_id'].isNotNull(), u['alira_id'])
                            .when(u["bookmaker_id"].isin(88), concat(u["bookmaker_id"], format_string("%08d", u["id"])))
                                .otherwise(u["id"]).alias("player_id"),                        
                        lit(5).cast(IntegerType()).alias("operation_type_id"),
                        bu["bonus_id"].cast(LongType()).alias("operation_id"),
                        when(u["bookmaker_id"].isin(88), concat(p['partner'], format_string("%012d", bu["id"])))
                            .otherwise(concat(p['partner'], bu["id"])).cast(LongType()).alias("partner_movement_id"),
                        bu["id"].cast(LongType()).alias("movement_id"),
                        lit(None).alias("partner_transaction_id"),
                        lit(None).alias("transaction_id"),
                        lit(1).cast(IntegerType()).alias("balance_type_id"),
                        lit(None).alias("event_reference"),
                        bu["balance"].cast(DecimalType(15, 2)).alias("movement_amount"),
                        when(u["currency_id"] == 41, lit(7))
                            .when(u["currency_id"] == 8, lit(9))
                            .when(u["currency_id"] == 2, lit(4))
                            .otherwise(0).cast(DecimalType(15, 0)).alias("currency_id"),
                        lit(None).alias("partner_room_id"),
                        lit(None).alias("room_id"),
                        lit(None).alias("partner_administrator_id"),
                        lit(None).alias("administrator_id"),
                        when(u["bookmaker_id"].isin(88), concat(p['partner'], format_string("%06d", bu["bonus_id"])))
                            .otherwise(concat(p["partner"], bu["bonus_id"])).cast(LongType()).alias("partner_promotion_id"),
                        bu["bonus_id"].cast(LongType()).alias("promotion_id"),
                        lit(None).alias("partner_event_reference"),
                    ).distinct()

    # Ajuste Manual
        # La tabla transaction viene filtrada desde la carga con transaction_type_id = 22 
        # revisar cambio definición tras reunión con desarrollo y no se filtra más transaction_status_id = 1
    # además se hace el cruce con wallets y no por user_id dado que el user_id no es el del jugador si no del que procesa
    Ajuste_GT = t.join(w, (w["id"] == t["wallet_id"]), "inner") \
                    .join(u, (u["id"] == w["user_id"]), "inner")\
                        .join(p, (p["id"] == u["bookmaker_id"]), "inner")\
                    .select(
                        date_format(from_utc_timestamp(t["created_at"], 'America/Buenos_Aires'), 'yyyy-MM-dd HH:mm:ss').alias('created_date'), #cambiar fecha utc a la zona horaria y escribirla en formato timestamp
                        p["partner"].cast(IntegerType()).alias("partner_id"),
                        date_format(from_utc_timestamp(t["created_at"], 'America/Buenos_Aires'), 'yyyy-MM-dd HH:mm:ss').cast(TimestampType()).alias('fecha_operacion'), #cambiar fecha utc a la zona horaria y escribirla en formato timestamp
                        when(u["bookmaker_id"].isin(88), concat(p['partner'], format_string("%012d", t["id"])))
                            .otherwise(concat(p['partner'], t["id"])).cast(LongType()).alias("partner_operation_id"),
                        concat(p['partner'],
                                when(u["alira_id"].isNotNull(), u["alira_id"])
                                .when(u["bookmaker_id"].isin(88), concat(u["bookmaker_id"], format_string("%08d", u["id"])))
                                .otherwise(u["id"])).alias("partner_player_id"),
                        when(u['alira_id'].isNotNull(), u['alira_id'])
                            .when(u["bookmaker_id"].isin(88), concat(u["bookmaker_id"], format_string("%08d", u["id"])))
                                .otherwise(u["id"]).alias("player_id"),                        
                        lit(6).cast(IntegerType()).alias("operation_type_id"),
                        t["id"].cast(LongType()).alias("operation_id"),
                        when(u["bookmaker_id"].isin(88), concat(p['partner'], format_string("%012d", t["wallet_id"])))
                            .otherwise(concat(p['partner'], t["wallet_id"])).cast(LongType()).alias("partner_movement_id"),
                        t["wallet_id"].cast(LongType()).alias("movement_id"),
                        when(u["bookmaker_id"].isin(88), concat(p['partner'], format_string("%012d", t["id"])))
                            .otherwise(concat(p['partner'], t["id"])).cast(LongType()).alias("partner_transaction_id"),
                        t["id"].alias("transaction_id"),
                        lit(1).cast(IntegerType()).alias("balance_type_id"),
                        lit(None).alias("event_reference"),
                        when(((t["credit"] > 0) & (t["debit"] == 0)), t["credit"])
                            .when(((t["credit"] == 0) & (t["debit"] > 0)), - t["debit"])
                            .when(((t["credit"] > 0) & (t["debit"] > 0)), (t["credit"] - t["debit"]))
                            .otherwise(lit(0))
                            .cast(DecimalType(15, 2)).alias("movement_amount"),
                        when(u["currency_id"] == 41, lit(7))
                            .when(u["currency_id"] == 8, lit(9))
                            .when(u["currency_id"] == 2, lit(4))
                            .otherwise(0).cast(DecimalType(15, 0)).alias("currency_id"),
                        when(t["casino_game_id"].isNull(), concat(p["partner"], lit(9009999)))
                            .otherwise(concat(p["partner"], t["casino_game_id"]))
                            .alias("partner_room_id"),
                        when(t["casino_game_id"].isNull(), lit(9009999))
                            .otherwise(t["casino_game_id"]).alias("room_id"),
                        lit(None).alias("partner_administrator_id"), # falta columna en parquet
                        # concat(p["partner"], t["adjustment_user_id"]).alias("partner_administrator_id"),
                        lit(None).alias("administrator_id"), # falta columna en parquet
                        # t["adjustment_user_id"].alias("administrator_id"),
                        lit(None).alias("partner_promotion_id"),
                        lit(None).cast(LongType()).alias("promotion_id"),
                        lit(None).alias("partner_event_reference"),
                    ).distinct()

    Otras_operaciones_GT = Conv_Efvo_GT.unionByName(Ajuste_GT)

    return Otras_operaciones_GT

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ejecución del DataFrame Otras Operaciones por Base de Datos: DWBPLAY_PY, DWBPLAY_SF, DWBPLAY_CABA y DWBPLAY_CORDOBA.
# MAGIC <p> Se ejecuta la función Create_Otras_Operaciones para obtener el dataframe con la información de las verticales de la sala en las bases de datos DWBPLAY_PY, DWBPLAY_SF, DWBPLAY_CABA y DWBPLAY_CORDOBA.

# COMMAND ----------

# DWBPLAY_PY
df_Otras_Operaciones0 = Create_DF_Otras_Operaciones(df_o0, df_m0, df_p0)
# DWBPLAY_SF
df_Otras_Operaciones1 = Create_DF_Otras_Operaciones(df_o1, df_m1, df_p1)
# DWBPLAY_CABA
df_Otras_Operaciones2 = Create_DF_Otras_Operaciones(df_o2, df_m2, df_p2)
# DWBPLAY_CORDOBA
df_Otras_Operaciones3 = Create_DF_Otras_Operaciones(df_o3, df_m3, df_p3)

# BPLAYBET - GT - Paraná, Jujuy & Mendoza
if bl_Carga_GT:
    df_Otras_Operaciones_GT = Create_DF_Otras_Operaciones_GT(df_u, df_bu, df_t, df_partner_Migra, df_w)

# COMMAND ----------

display(df_Otras_Operaciones_GT.filter(col("partner_id") == 256))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Union de DF Otras Operaciones
# MAGIC <p> Se realiza la unificacion de las ejecuciones de las bases de datos DWBPLAY_PY, DWBPLAY_SF, DWBPLAY_CABA y DWBPLAY_CORDOBA.

# COMMAND ----------

if bl_Carga_GT:
  # con BPLAYBET - GT - Paraná, Jujuy & Mendoza
  df_Otras_Operaciones_union = df_Otras_Operaciones0.unionByName(df_Otras_Operaciones1).unionByName(df_Otras_Operaciones2).unionByName(df_Otras_Operaciones3)\
    .unionByName(df_Otras_Operaciones_GT)
else:
  df_Otras_Operaciones_union = df_Otras_Operaciones0.unionByName(df_Otras_Operaciones1).unionByName(df_Otras_Operaciones2).unionByName(df_Otras_Operaciones3)



# COMMAND ----------

def Calcular_amountUSD(Otras_Operaciones):
        #Get Tipo_de_Cambio
    tc = spark.read.table("entprod_mdn.bplay.tipos_de_cambio")
        # Calculo de amoun_USD
    Otras_Operaciones_tc = Otras_Operaciones.join(tc,  tc["Fecha"] == Otras_Operaciones["fecha_operacion"] , "left")\
        .select(
            "created_date",
            "partner_id",
            "fecha_operacion",
            "partner_operation_id",
            "partner_player_id",
            "player_id",
            "operation_type_id",
            "operation_id",
            "partner_movement_id",
            "movement_id",
            "partner_transaction_id",
            "transaction_id",
            "balance_type_id",
            "event_reference",
            "movement_amount",
            "currency_id",
            "partner_room_id",
            "room_id",
            "partner_administrator_id",
            "administrator_id",
            "partner_promotion_id",
            "promotion_id",
            "partner_event_reference",
            when((col("currency_id") == "7"), (col("movement_amount")/col("ARS_CCL")))\
                .when((col("currency_id") == "15"), (col("movement_amount")/col("PYG")))\
                    .when((col("currency_id") == "9"), (col("movement_amount")/col("BRL")))\
                        .otherwise(col("movement_amount")).cast(DecimalType(25, 2)).alias("amount_USD")
        )
    return Otras_Operaciones_tc

# COMMAND ----------

df_Otras_Operaciones = Calcular_amountUSD(df_Otras_Operaciones_union)

# COMMAND ----------

# import pyspark.sql.functions as F
# display(df_Otras_Operaciones.groupBy('partner_operation_id', 'movement_id').agg(F.count('*').alias('count')).filter(col('count') > 1))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Upsert de Tabla Delta "Otras_Operaciones"
# MAGIC <p> Obtenemos la tabla original para actualizar la Delta Table 'Otras_Operaciones' con el dataframe df_Otras_Operaciones recién creado.

# COMMAND ----------

def get_dif(nuevo):
    # Obtengo el df de la tabla Otras Operaciones ya creada
    origen = spark.read.table(delta_table_OtrasOperaciones)
    # Selecciono los campos por comparar
    origen = origen.select("created_date", "partner_id", "fecha_operacion", "partner_operation_id", "partner_player_id", "player_id", "operation_type_id", "operation_id", "partner_movement_id", "movement_id", "partner_transaction_id", "transaction_id", "balance_type_id", "event_reference", "movement_amount", "currency_id", "partner_room_id", "room_id", "partner_administrator_id", "administrator_id", "partner_promotion_id", "promotion_id", "amount_USD", "partner_event_reference")
    # Obtengo las filas nuevas y actualizadas 
    dif = nuevo.exceptAll(origen)
    # Agrega la columna de actualización
    res = dif.withColumn("fecha_actualizacion", current_timestamp()).withColumn("fecha_creacion", col("created_date"))
    return res

# COMMAND ----------

# Filtra los ultimos 20 días
def filter_last_15_days(df): 
    last_15_days = concat(date_sub(current_timestamp(), 15).cast("string"),lit(" 23:59:59")).cast("timestamp")
    result = df.filter(col("fecha_operacion") >= last_15_days)
    return result

# COMMAND ----------

# Filtra hasta las  23:59:59 del dia anterior a la ejecucion
def filter_last_day(df): 
    yesterday_end = concat(date_sub(current_timestamp(), 1).cast("string"),lit(" 23:59:59")).cast("timestamp")
    result = df.filter(col("fecha_operacion") <= yesterday_end)
    return result

# COMMAND ----------

# MAGIC %md
# MAGIC ### Obtención de la Última Tabla Generada para Actualización
# MAGIC <p> Si ya existe la Delta Table de Otras Operaciones creada se obtendra la tabla y se actualizará con el DataFrame generado en esta ejecución.
# MAGIC <p> Si no existiera, se definirá la limitación las particiones en 8, y se generará un nuevo Delta Table de "Otras Operaciones".

# COMMAND ----------

print(delta_table_OtrasOperaciones)
print(exist)
print('Se ejecutará en PRD' if PROD else 'Se ejecutará en DEV')
print('Se realizará la Carga Historica' if bl_Carga_Historica else 'Se cargara el año corriente')

# COMMAND ----------

# Verificar si la ruta contiene una tabla Delta
if exist:
    # Se Obtiene las filas por actualizar
    df_Otras_Operaciones_dif = get_dif(df_Otras_Operaciones)
    # Se obtiene las actualizaciones de los ultimos 15 días
    df_Otras_Operaciones_dif = filter_last_15_days(df_Otras_Operaciones_dif)
    # Se elimina hasta el ultimo día de ejecucion
    df_Otras_Operaciones_act = filter_last_day(df_Otras_Operaciones_dif)
    
    # Si la ruta contiene una tabla Delta, creamos el objeto DeltaTable
    dt_origen = DeltaTable.forName(spark, delta_table_OtrasOperaciones)
    # Upsert del DataFrame de Otras Operaciones
    dt_origen.alias("dest")\
            .merge(source=df_Otras_Operaciones_act.alias("update"), condition="dest.partner_operation_id = update.partner_operation_id AND dest.movement_id = update.movement_id")\
                .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    #print("Se actualizó la base de Otras Operaciones")
else:
    # Agregado de las columnas identificatorias para el upgrade de la base
    df_Otras_Operaciones = df_Otras_Operaciones\
        .withColumn("fecha_actualizacion", current_timestamp())\
            .withColumn("fecha_creacion", col("created_date")).coalesce(8)
    # Guardar el DataFrame en formato Delta con overwrite habilitado
    df_Otras_Operaciones.write\
        .mode("overwrite")\
            .saveAsTable(delta_table_OtrasOperaciones)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Carga del DataFrame Actualizado de Otras Operaciones
# MAGIC <p> Se carga la tabla actualizada en el Warehouse de Databricks para su posterior utilización o consumo.

# COMMAND ----------

# # Se Obtiene las filas por actualizar
# df_Otras_Operaciones_dif = get_dif(df_Otras_Operaciones)
# # Se obtiene las actualizaciones de los ultimos 15 días
# df_Otras_Operaciones_dif_l15 = filter_last_15_days(df_Otras_Operaciones_dif)
# # Se elimina hasta el ultimo día de ejecucion
# df_Otras_Operaciones_act = filter_last_day(df_Otras_Operaciones_dif_l15)
# 
# df_Otras_Operaciones = df_Otras_Operaciones\
#         .withColumn("fecha_actualizacion", current_timestamp())\
#             .withColumn("fecha_creacion", col("created_date")).coalesce(8)
# df_Otras_Operaciones.write \
#     .mode("overwrite")\
#         .option("overwriteSchema", "true")\
#             .saveAsTable('entprod_mdn.bplay.otras_operaciones')