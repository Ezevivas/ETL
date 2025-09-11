# Databricks notebook source
# MAGIC %md
# MAGIC # Apuestas_y_Premios_MDN-bplay_PySpark.ipynb
# MAGIC <h> by PySpark
# MAGIC #### Estado: **OK**
# MAGIC <p> Source: Se realizará la lectura de archivos en formato parquet para obtener los dataframes necesarios para generar la tabla 'Apuestas y Premios', y cargarla en formato Delta Table para su posterior consulta.
# MAGIC <p> Se tomará la información desde el STG1 de Datalake para generar un consumible de STG2 en formato Delta Table.
# MAGIC <p> Inputs:
# MAGIC   <ol>
# MAGIC     <li>Room</li>
# MAGIC     <li>Service</li>
# MAGIC     <li>Room Partner</li>
# MAGIC     <li>Operation</li>
# MAGIC     <li>Movement</li>
# MAGIC     <li>Player</li>
# MAGIC   </ol>
# MAGIC <p> Output: Delta Table actualizado de "apuestas_y_premios" y su carga en el Warehouse de Databricks.

# COMMAND ----------

# MAGIC %md
# MAGIC <h5> Versionados
# MAGIC <p> Se agrego la columna de Diccionario nombrada "relacion_operation_t_t_c" - Fecha:21/03/2024 - Autor:Ezequiel Vivas
# MAGIC <p> Se agrego la columna "id" para realizar el upsert de manera eficiente - Fecha:16/04/2024 - Autor:Ezequiel Vivas
# MAGIC <p> Se agregaron las columnas de "fecha_creacion" y "fecha_actualizacion", para el control de la carga incremental. - Fecha:01/05/2024 - Autor:Ezequiel Vivas

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Start Up
# MAGIC <p> Etapa de preparación para su ejecución

# COMMAND ----------

# MAGIC %md
# MAGIC ### Declaración de librerias utilizadas
# MAGIC <p> pyspark.sql.functions = Importa funciones de PySpark para manipular y trabajar con columnas y datos en los DataFrames.
# MAGIC <p> pyspark.sql.types = Importa tipos de datos, para definir la estructura de los DataFrames.
# MAGIC <p> delta.tables = Importa funciones para el manejo de Tablas Delta
# MAGIC <p> spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY") = Configura las propiedades de las librerias de Spark 3.0

# COMMAND ----------

from pyspark.sql.functions import concat, col, lit, sum, max, when, format_string, to_date, expr, countDistinct, date_format, current_timestamp, to_timestamp, date_sub, year
from pyspark.sql.types import IntegerType, DecimalType, LongType, StringType
from delta.tables import DeltaTable

# Set the configuration property to restore the behavior before Spark 3.0
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Nomenclatura de Fuentes y Auxiliares
# MAGIC <ul>
# MAGIC   <li>df = dataframe</li>
# MAGIC   <li>r = room</li>
# MAGIC   <li>s = service</li>
# MAGIC   <li>rp = room partner</li>
# MAGIC   <li>o = operation</li>
# MAGIC   <li>m = movement</li>
# MAGIC   <li>p = player</li>
# MAGIC   <li>0 = DWBPLAY_PY</li>
# MAGIC   <li>1 = DWBPLAY_SF</li>
# MAGIC   <li>2 = DWBPLAY_CABA</li>
# MAGIC   <li>3 = DWBPLAY_CORDOBA</li>
# MAGIC   <li>fm = Fact Movement</li>
# MAGIC   <li>dr = Dim Room</li>
# MAGIC </ul>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Conexion de Source y Obtención de DataFrames
# MAGIC <p> Descripción: 
# MAGIC <ol>
# MAGIC   <li>Establece la conexión al STG1 del Data Lake.</li>
# MAGIC   <li>Se definen los paths de los archivos parquet "Room", "Service", "Room Partner", "Operation", "Movement" y "Player" para cada Base de Datos DWBPLAY_PY, DWBPLAY_SF, DWBPLAY_CABA y DWBPLAY_CORDOBA.</li>
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

 # Get DF Room
# df_r0 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_py/OGP/room')
# df_r1 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_sf/OGP/room')
# df_r2 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_caba/OGP/room')
# df_r3 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_cordoba/OGP/room')
  # Get DF Service
# df_s0 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_py/OGP/service')
# df_s1 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_sf/OGP/service')
# df_s2 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_caba/OGP/service')
# df_s3 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_cordoba/OGP/service')
  # Get DF Room Partner
# df_rp0 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_py/OGP/room_partner')
# df_rp1 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_sf/OGP/room_partner')
# df_rp2 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_caba/OGP/room_partner')
# df_rp3 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_cordoba/OGP/room_partner')

# Se toman los datos de las salas de juegos Alira desde la tabla DB Salas

  # Get DF Player
df_p0 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_py/OGP/player') \
                    .select(col('id').cast(LongType()).alias('id'), col('partner').cast(IntegerType()).alias('partner'), 
                            col('type').cast(IntegerType()).alias('type'), col('status').cast(IntegerType()).alias('status'))
df_p1 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_sf/OGP/player') \
                    .select(col('id').cast(LongType()).alias('id'), col('partner').cast(IntegerType()).alias('partner'), 
                            col('type').cast(IntegerType()).alias('type'), col('status').cast(IntegerType()).alias('status'))
df_p2 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_caba/OGP/player') \
                    .select(col('id').cast(LongType()).alias('id'), col('partner').cast(IntegerType()).alias('partner'), 
                            col('type').cast(IntegerType()).alias('type'), col('status').cast(IntegerType()).alias('status'))
df_p3 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_cordoba/OGP/player') \
                    .select(col('id').cast(LongType()).alias('id'), col('partner').cast(IntegerType()).alias('partner'), 
                            col('type').cast(IntegerType()).alias('type'), col('status').cast(IntegerType()).alias('status'))

  # variable para filtrar el año en curso
current_year = date_format(current_timestamp(), 'yyyy').cast(IntegerType())

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
              .filter(col('Year') == current_year)
    df_m1 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_sf/ogp/movement/') \
              .filter(col('Year') == current_year)
    df_m2 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_caba/ogp/movement/') \
              .filter(col('Year') == current_year)
    df_m3 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_cordoba/OGP/movement/') \
              .filter(col('Year') == current_year)
      # Get DF Operation
    df_o0 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_py/ogp/operation/') \
              .filter(col('Year') == current_year)
    df_o1 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_sf/ogp/operation/') \
              .filter(col('Year') == current_year)
    df_o2 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_caba/ogp/operation/') \
              .filter(col('Year') == current_year)
    df_o3 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_cordoba/OGP/operation/') \
              .filter(col('Year') == current_year)

# COMMAND ----------

try:
  # Apuestas y Premios GT
    #Get Bets GT
  df_b_GT = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/bplaybet_arg/dbo/bets')
    #Get Tickets GT
  df_tick_GT = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/bplaybet_arg/dbo/tickets')
    #Get Users GT
  df_usr_GT = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/bplaybet_arg/dbo/users') \
                            .select('id', 'alira_id', col('bookmaker_id').cast(IntegerType()).alias('bookmaker_id'), 
                                    'local_register', 'is_services', 'is_backoffice', 'created_at')   

  #### Get Salas ####
    #Get Casino Games
  # df_cg_GT = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/bplaybet_arg/dbo/casino_games')
    #Get Bookmaker Casino Games
  # df_bcg_GT = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/bplaybet_arg/dbo/bookmaker_casino_game')
    #Get Casino Providers
  # df_cp_GT = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/bplaybet_arg/dbo/casino_providers')
    #Get Translation Entries
  # df_te_GT = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/bplaybet_arg/dbo/translation_entries')
    #Get Casino Category Casino Game
  # df_cgc_GT = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/bplaybet_arg/dbo/casino_category_casino_game')
    #Get Casino Category Casino Game
  # df_cc_GT = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/bplaybet_arg/dbo/casino_categories')

  # Se toman los datos de las salas de juegos Alira desde la tabla DB Salas

# from pyspark.sql.functions import count
# display(df_tick_GT.groupBy('bookmaker_id').agg(count('*').alias('count')))

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

#   Path PRD del Delta Table AyP
delta_table_AyP_prd = "entprod_mdn.bplay.apuestas_y_premios"

#   Path DEV del Delta Table AyP
delta_table_AyP_dev = "entprod_mdn.default.apuestas_y_premios"

#   Switch PROD/DEV
if PROD:
    delta_table_AyP = delta_table_AyP_prd
else:
    delta_table_AyP = delta_table_AyP_dev
print(delta_table_AyP)

# COMMAND ----------

# Manejar el caso en que la tabla no exista
try:
    # Intentar leer la tabla Delta
    df_origen = spark.read.table(delta_table_AyP)
    count = df_origen.select("id").count()

    # Validar la tabla AyP
    if count > 0:
        exist = True
    else:
        exist = False
except Exception as e:
    # print(f"La tabla {delta_table_Balance} no existe o no se puede leer: {e}")
    exist = False

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Get Salas

# COMMAND ----------

# MAGIC %md
# MAGIC ### Obtencion de las Salas
# MAGIC <p> Esta función obtiene un dataframe con la información de las verticales de las salas.

# COMMAND ----------

df_salas = spark.read.table('entprod_mdn.bplay.salas')

#df_salas = spark.read.table('entprod_mdn.default.salas_mig')

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. DF Auxiliar Fact Movement
# MAGIC <p> Se genera un DataFrame auxiliar de Fact Movement con las columnas necesarias.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Definición de la Función Create_DF_Fact_Movement
# MAGIC <p> Esta función genera un dataframe con la información de las transacciones de los player por jurisdiccion y tipo de juego, organizado por fecha.

# COMMAND ----------

def Create_DF_Fact_Movement(o, m, p):
    # Filter
    m = m.filter(col("balance_type") != 15)
    p = p.filter(col("status") != 1)
    o = o.filter(col("type").isin(3, 4)) # Type = 3 Premios / 4 Apuesta

    # Join Movement Operation
    fm1 = m.join(o, (o["id"] == m["operation"]), "inner")\
        .select(
            o["created_date"],
            o["id"].alias("operation_id"),
            m["id"].alias("movement_id"),
            o["player"],
            o["transaction"],
            to_date(o["created_date"]).alias("fecha_operacion"),
            o["type"].alias("operation_type_id"),
            m["balance_type"].alias("balance_type_id"),
            o["room"],
            o["event_reference"],
            o["administrator"],
            m["amount"].cast(DecimalType(14, 2)).alias("movement_amount"),
            m["promotion"],
            m["currency"].alias("currency_id")
        ).distinct()
    
    # Join Player, Operation, Movement
    ### REVISAR "LEFT JOIN" PARA QUE FILTRE JUGADORES STATUS 1, # VA A CAMBIAR LOS VALORES $$$? ###
    fm = fm1.join(p, (fm1["player"] == p["id"])) \
        .select(
            p["partner"].cast(IntegerType()).alias("partner_id"), # DecimalType(15,0)
            fm1["fecha_operacion"],
            concat(p["partner"], p["id"]).cast(LongType()).alias("partner_player_id"), # DecimalType(10,0))
            fm1["operation_type_id"].cast(IntegerType()),    # DecimalType(2,0)
            fm1["balance_type_id"].cast(IntegerType()),      # DecimalType(2,0)
            fm1["event_reference"].cast(LongType()).alias("event_reference"),
            when(fm1["room"].isNull(), None).otherwise(concat(p["partner"], fm1["room"]))\
                .cast(LongType()).alias("partner_room_id"),
            fm1["currency_id"].cast(IntegerType()),          # DecimalType(2,0)
            concat(p["partner"], fm1["operation_id"]).cast(LongType()).alias("partner_operation_id"),
            fm1["movement_amount"].cast(DecimalType(15,2)).alias("movement_amount"),
            when((fm1["room"].isNull()) | (fm1["event_reference"] == 0), None) \
                .otherwise(concat(p["partner"], fm1["event_reference"])).cast(LongType()).alias("partner_event_reference")
        ).distinct()

    return fm

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ejecución del DataFrame Auxiliar Dim Room por Base de Datos: DWBPLAY_PY, DWBPLAY_SF, DWBPLAY_CABA y DWBPLAY_CORDOBA
# MAGIC <p> Se ejecuta la función Create_DF_Aux_Fact_Movement para obtener el dataframe de Fact Movement de cada base de datos: DWBPLAY_PY, DWBPLAY_SF, DWBPLAY_CABA y DWBPLAY_CORDOBA

# COMMAND ----------

# DWBPLAY_PY
df_fm0 = Create_DF_Fact_Movement(df_o0, df_m0, df_p0)
# DWBPLAY_SF
df_fm1 = Create_DF_Fact_Movement(df_o1, df_m1, df_p1)
# DWBPLAY_CABA
df_fm2 = Create_DF_Fact_Movement(df_o2, df_m2, df_p2)
# DWBPLAY_CORDOBA
df_fm3 = Create_DF_Fact_Movement(df_o3, df_m3, df_p3)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Apuestas y Premios
# MAGIC <p> Se genera el DataFrame de Apuestas y Premios a partir de los DataFrames auxiliares creados de Fact Movement y Dim Room.

# COMMAND ----------

# MAGIC %md
# MAGIC #### GT - Get AyP Bono Casino

# COMMAND ----------

from pyspark.sql.functions import count
### Apuestas y Premios Bono Casino
def AyP_Bono_Casino(tick, bet, usr, salas, p):
    usr_filter = usr.filter((col("local_register") == '0') & (col("is_backoffice") == '0') & (col("is_services") == '0'))

    # Apuestas Bono Casino
    b_Ap = bet.filter(col("result_status_id") == 9).filter(col("amount").isNotNull())\
                .filter(col("bonus_user_amount").isNotNull()).filter(col("bonus_user_amount") > 0)

    aux = b_Ap.alias("b_Ap").join(tick.alias("tick"), tick["id"] == b_Ap["ticket_id"])\
        .join(usr_filter,  tick["user_id"] == usr_filter["id"])\
            .join(p, tick["bookmaker_id"] == p['id'])\
                .select(
                        p['partner'].cast(IntegerType()).alias("partner_id"),
                        concat( p['partner'], 
                                when(usr_filter['alira_id'].isNotNull(), usr_filter['alira_id'])
                                .when(usr_filter["bookmaker_id"].isin(88), concat(usr_filter["bookmaker_id"], \
                                    format_string("%08d", usr_filter["id"]))) \
                                .otherwise(usr_filter["id"])
                                ).cast(LongType()).alias("partner_player_id"),
                        b_Ap["ticket_id"].alias("bet_ticket_id"),
                        to_date(expr("b_Ap.created_at - INTERVAL 3 HOURS")).alias("fecha_operacion"),
                        concat(p['partner'], b_Ap["casino_game_id"]).cast(LongType()).alias("partner_room_id"),
                        # when((tick["bookmaker_id"].isin(88)) & (tick["currency_id"] == 41), lit(7))
                        #                                 .otherwise(tick["currency_id"]).alias('currency_id'),
                        when(tick["currency_id"] == 41, lit(7))
                            .when(tick["currency_id"] == 8, lit(9))
                            .when(tick["currency_id"] == 2, lit(4))
                            .otherwise(0).cast(IntegerType()).alias("currency_id"),
                        b_Ap["bonus_user_amount"].cast(DecimalType(15,2)).alias("importe_efectivo")
                            )

    # Apuestas_Bono_Casino 
    Apuestas_Bono_Casino = aux.join(salas, (aux["partner_room_id"] == salas["partner_room_id"]) & (aux["partner_id"] == salas["partner_id"]), 'left')\
        .select(
                aux["partner_id"],
                aux["fecha_operacion"],
                aux["partner_player_id"],
                lit('4').alias("operation_type_id"),
                lit('8').alias("balance_type_id"),
                salas["partner_room_provider_id"],
                when((aux["partner_id"] == 460) & (aux["partner_room_id"].isNull()), lit("TGM")) 
                    .otherwise(salas["service_legal_type_id"]).alias("Categoria_juego"),
                when((aux["partner_id"] == 460) & (aux["partner_room_id"].isNull()), lit("Slots")) \
                    .otherwise(salas["service_legal_type"]).alias("service_legal_type"),
                aux["partner_room_id"],
                aux["currency_id"].cast(IntegerType()),
                aux["bet_ticket_id"],
                aux["importe_efectivo"],
                concat(lit('04'), format_string("%02d", lit(8)), format_string("%02d", aux["currency_id"])).alias("relacion_operation_t_t_c")
                    ).groupBy(
                "partner_id", "fecha_operacion", "partner_player_id", "operation_type_id", "balance_type_id", "partner_room_provider_id", "Categoria_juego",
                "service_legal_type", "partner_room_id", "currency_id", "relacion_operation_t_t_c"
                        ).agg(
                            (count(col("bet_ticket_id"))).alias("cant_juegos"), 
                            sum(col("importe_efectivo") * -1).cast(DecimalType(15,2)).alias("movement_amount")
                )
               
    # Premio Bono Casino            
    b_Pr = bet.filter(col("result_status_id").isin(8, 10))\
                .filter(col("bonus_user_amount").isNotNull()).filter(col("bonus_user_amount") > 0)

    aux = b_Pr.alias("b_Pr").join(tick.alias("tick"), tick["id"] == b_Pr["ticket_id"])\
        .join(usr_filter,  tick["user_id"] == usr_filter["id"])\
            .join(p, tick["bookmaker_id"] == p['id'])\
                .select(
                        p['partner'].cast(IntegerType()).alias("partner_id"),
                        concat(p['partner'], 
                                when(usr_filter['alira_id'].isNotNull(), usr_filter['alira_id'])
                                .when(usr_filter["bookmaker_id"].isin(88), concat(usr_filter["bookmaker_id"], format_string("%08d", usr_filter["id"])))
                                .otherwise(usr_filter["id"])
                        ).cast(LongType()).alias("partner_player_id"),
                        b_Pr["ticket_id"].alias("bet_ticket_id"),
                        #to_date(expr("b_Pr.settled_at - INTERVAL 3 HOURS")).alias("fecha_operacion"), ################################
                        to_date(expr("b_Pr.created_at - INTERVAL 3 HOURS")).alias("fecha_operacion"),
                        concat(p['partner'], b_Pr["casino_game_id"]).cast(LongType()).alias("partner_room_id"),
                        # tick["currency_id"].alias("currency_id"),
                        when(tick["currency_id"] == 41, lit(7))
                            .when(tick["currency_id"] == 8, lit(9))
                            .when(tick["currency_id"] == 2, lit(4))
                            .otherwise(0).cast(IntegerType()).alias("currency_id"),
                        b_Pr["bonus_user_amount"].cast(DecimalType(15,2)).alias("importe_efectivo")
                    )
                
    Premio_Bono_Casino = aux.join(salas, (aux["partner_room_id"] == salas["partner_room_id"]) & (aux["partner_id"] == salas["partner_id"]), 'left').select(
                                    aux["partner_id"],
                                    aux["fecha_operacion"],
                                    aux["partner_player_id"],
                                    lit('3').alias("operation_type_id"),
                                    lit('8').alias("balance_type_id"),
                                    salas["partner_room_provider_id"],
                                    when((aux["partner_id"] == 460) & (aux["partner_room_id"].isNull()), lit("TGM")) \
                                        .otherwise(salas["service_legal_type_id"]).alias("Categoria_juego"),
                                    when((aux["partner_id"] == 460) & (aux["partner_room_id"].isNull()), lit("Slots")) \
                                        .otherwise(salas["service_legal_type"]).alias("service_legal_type"),
                                    aux["partner_room_id"],
                                    aux["currency_id"].cast(IntegerType()),
                                    aux["bet_ticket_id"],
                                    aux["importe_efectivo"],
                                    concat(lit('03'), format_string("%02d", lit(1)), format_string("%02d", aux["currency_id"])).alias("relacion_operation_t_t_c")
                                ).groupBy(
                                    "partner_id", "fecha_operacion", "partner_player_id", "operation_type_id", "balance_type_id", "partner_room_provider_id", "Categoria_juego",
                                    "service_legal_type", "partner_room_id", "currency_id", "relacion_operation_t_t_c"
                                    ).agg( 
                                        (count(col("bet_ticket_id"))).alias("cant_juegos"), 
                                        sum("importe_efectivo").cast(DecimalType(15,2)).alias("movement_amount")
                                )
        
    return (Apuestas_Bono_Casino.unionByName(Premio_Bono_Casino)).withColumn("id", concat(col("partner_player_id"), 
                                            date_format(col("fecha_operacion"), "yyyyMMdd"), 
                                            format_string("%02d", col("operation_type_id").cast(IntegerType())), 
                                            format_string("%02d", col("balance_type_id").cast(IntegerType())),
                                            format_string("%02d", col("currency_id").cast(IntegerType())),
                                            col("partner_room_id")
                                        ))

# COMMAND ----------

# MAGIC %md
# MAGIC #### GT - Get AyP Efectivo Casino

# COMMAND ----------

from pyspark.sql.functions import count
### Apuestas y Premios Efectivo Casino
def AyP_Efectivo_Casino(tick, bet, usr, salas, p):
    usr_filter = usr.filter((col("local_register") == '0') & (col("is_backoffice") == '0') & (col("is_services") == '0'))

    # Apuestas Efectivo Casino
    b_Ap = bet.filter(col("result_status_id") == 9).filter(col("amount").isNotNull())

    aux = b_Ap.alias("b_Ap").join(tick.alias("tick"), tick["id"] == b_Ap["ticket_id"])\
        .join(usr_filter,  tick["user_id"] == usr_filter["id"])\
            .join(p, tick["bookmaker_id"] == p['id'])\
                .select(
                        p['partner'].cast(IntegerType()).alias("partner_id"),
                        concat( p['partner'], 
                                when(usr_filter['alira_id'].isNotNull(), usr_filter['alira_id'])
                                .when(usr_filter["bookmaker_id"].isin(88), concat(usr_filter["bookmaker_id"], format_string("%08d", usr_filter["id"])))
                                .otherwise(usr_filter["id"])
                        ).cast(LongType()).alias("partner_player_id"),
                        b_Ap["ticket_id"].alias("bet_ticket_id"),
                        to_date(expr("b_Ap.created_at - INTERVAL 3 HOURS")).alias("fecha_operacion"),
                        concat(p['partner'], b_Ap["casino_game_id"]).cast(LongType()).alias("partner_room_id"),
                        # when((tick["bookmaker_id"].isin(88)) & (tick["currency_id"] == 41), lit(7))
                        #                                 .otherwise(tick["currency_id"]).alias('currency_id'),
                        when(tick["currency_id"] == 41, lit(7))
                            .when(tick["currency_id"] == 8, lit(9))
                            .when(tick["currency_id"] == 2, lit(4))
                            .otherwise(0).cast(IntegerType()).alias("currency_id"),
                        when(b_Ap["bonus_user_amount"].isNull(), b_Ap["amount"])
                            .otherwise(b_Ap["amount"] - b_Ap["bonus_user_amount"])
                            .cast(DecimalType(15,2)).alias("importe_efectivo")
                    )

    # Apuestas_Efectivo_Casino 
    Apuestas_Efectivo_Casino = aux.join(salas, (aux["partner_room_id"] == salas["partner_room_id"]) & (aux["partner_id"] == salas["partner_id"]), 'left')\
        .select(
                aux["partner_id"],
                aux["fecha_operacion"],
                aux["partner_player_id"],
                lit('4').alias("operation_type_id"),
                lit('1').alias("balance_type_id"),
                salas["partner_room_provider_id"],
                when((aux["partner_id"] == 460) & (aux["partner_room_id"].isNull()), lit("TGM")) \
                    .otherwise(salas["service_legal_type_id"]).alias("Categoria_juego"),
                when((aux["partner_id"] == 460) & (aux["partner_room_id"].isNull()), lit("Slots")) \
                    .otherwise(salas["service_legal_type"]).alias("service_legal_type"),
                aux["partner_room_id"],
                aux["currency_id"].cast(IntegerType()),
                aux["bet_ticket_id"],
                aux["importe_efectivo"],
                concat(lit('04'), format_string("%02d", lit(1)), format_string("%02d", aux["currency_id"])).alias("relacion_operation_t_t_c")
                    ).groupBy(
                "partner_id", "fecha_operacion", "partner_player_id", "operation_type_id", "balance_type_id", "partner_room_provider_id", "Categoria_juego",
                "service_legal_type", "partner_room_id", "currency_id", "relacion_operation_t_t_c"
                        ).agg(
                            (count(col("bet_ticket_id"))).alias("cant_juegos"), 
                            sum(col("importe_efectivo") * -1).cast(DecimalType(15,2)).alias("movement_amount")
                )
               
    # Premio Efectivo Casino            
    b_Pr = bet.filter(col("result_status_id").isin(8, 10))

    aux = b_Pr.alias("b_Pr").join(tick.alias("tick"), tick["id"] == b_Pr["ticket_id"])\
        .join(usr_filter,  tick["user_id"] == usr_filter["id"])\
            .join(p, tick["bookmaker_id"] == p['id'])\
                .select(
                        p['partner'].cast(IntegerType()).alias("partner_id"),
                        concat(p['partner'], 
                                when(usr_filter['alira_id'].isNotNull(), usr_filter['alira_id'])
                                .when(usr_filter["bookmaker_id"].isin(88), concat(usr_filter["bookmaker_id"], format_string("%08d", usr_filter["id"])))
                                .otherwise(usr_filter["id"])
                        ).cast(LongType()).alias("partner_player_id"),
                        b_Pr["ticket_id"].alias("bet_ticket_id"),
                        #to_date(expr("b_Pr.settled_at - INTERVAL 3 HOURS")).alias("fecha_operacion"), ################################
                        to_date(expr("b_Pr.created_at - INTERVAL 3 HOURS")).alias("fecha_operacion"),
                        concat(p['partner'], b_Pr["casino_game_id"]).cast(LongType()).alias("partner_room_id"),
                        # tick["currency_id"].alias("currency_id"),
                        # when((tick["bookmaker_id"].isin(88)) & (tick["currency_id"] == 41), lit(7))
                        #      .otherwise( tick["currency_id"]).alias('currency_id'),
                        when(tick["currency_id"] == 41, lit(7))
                            .when(tick["currency_id"] == 8, lit(9))
                            .when(tick["currency_id"] == 2, lit(4))
                            .otherwise(0).cast(IntegerType()).alias("currency_id"),
                        when(b_Pr["bonus_user_amount"].isNull(), b_Pr["amount"])
                            .otherwise(b_Pr["amount"] - b_Pr["bonus_user_amount"])
                            .cast(DecimalType(15,2)).alias("importe_efectivo")
                        )
                
    Premio_Efectivo_Casino = aux.join(salas, (aux["partner_room_id"] == salas["partner_room_id"]) & (aux["partner_id"] == salas["partner_id"]), 'left').select(
        aux["partner_id"],
        aux["fecha_operacion"],
        aux["partner_player_id"],
        lit('3').alias("operation_type_id"),
        lit('1').alias("balance_type_id"),
        salas["partner_room_provider_id"],
        when((aux["partner_id"] == 460) & (aux["partner_room_id"].isNull()), lit("TGM")) \
            .otherwise(salas["service_legal_type_id"]).alias("Categoria_juego"),
        when((aux["partner_id"] == 460) & (aux["partner_room_id"].isNull()), lit("Slots")) \
            .otherwise(salas["service_legal_type"]).alias("service_legal_type"),
        aux["partner_room_id"],
        aux["currency_id"].cast(IntegerType()),
        aux["bet_ticket_id"],
        aux["importe_efectivo"],
        concat(lit('03'), format_string("%02d", lit(1)), format_string("%02d", aux["currency_id"])).alias("relacion_operation_t_t_c")
    ).groupBy(
        "partner_id", "fecha_operacion", "partner_player_id", "operation_type_id", "balance_type_id", "partner_room_provider_id", "Categoria_juego",
        "service_legal_type", "partner_room_id", "currency_id", "relacion_operation_t_t_c"
        ).agg( 
              (count(col("bet_ticket_id"))).alias("cant_juegos"), 
              sum("importe_efectivo").cast(DecimalType(15,2)).alias("movement_amount")
              )
        
    return (Apuestas_Efectivo_Casino.unionByName(Premio_Efectivo_Casino)).withColumn("id", concat(col("partner_player_id"), 
                                            date_format(col("fecha_operacion"), "yyyyMMdd"), 
                                            format_string("%02d", col("operation_type_id").cast(IntegerType())), 
                                            format_string("%02d", col("balance_type_id").cast(IntegerType())),
                                            format_string("%02d", col("currency_id").cast(IntegerType())),
                                            col("partner_room_id")
                                        ))

# COMMAND ----------

# from pyspark.sql.functions import count
#     # Apuestas y Premios Efectivo Casino
# df_ef = AyP_Efectivo_Casino(df_tick_GT, df_b_GT, df_usr_GT, df_salas, df_partner_Migra)

# display(df_ef.filter((col('partner_id').isin(130, 460, 256)) & (col('fecha_operacion') >= '2025-05-01')).groupBy('partner_id', 'fecha_operacion', 'operation_type_id').agg(sum("movement_amount").alias('amount')).orderBy('partner_id', 'fecha_operacion', 'operation_type_id'))

# COMMAND ----------

# MAGIC %md
# MAGIC #### GT - Get AyP Bono Apuestas Deportivas

# COMMAND ----------

### Apuesta y Premios Bonos AADD
def AyP_Bono_AADD(tick, bet, usr, p):
    usr_filter = usr.filter((col("local_register") == '0') & (col("is_backoffice") == '0') & (col("is_services") == '0')).select("bookmaker_id", "id", 'alira_id')
    
    # Apuesta Bonos AADD
    b_Ap = bet.filter(~col("result_status_id").isin(8, 9, 10, 11)).filter(col("bonus_user_amount").isNotNull())

    Apuesta_Bono_AADD = b_Ap.alias("b_Ap")\
        .join(tick.alias("tick"), tick["id"] == b_Ap["ticket_id"])\
        .join(usr_filter, tick["user_id"] == usr_filter["id"])\
            .join(p, tick["bookmaker_id"] == p['id'])\
            .select(
                    p['partner'].cast(IntegerType()).alias('partner_id'),
                    concat(p['partner'], when(usr_filter['alira_id'].isNotNull(), usr_filter['alira_id'])
                        .when(usr_filter["bookmaker_id"].isin(88), concat(usr_filter["bookmaker_id"], format_string("%08d", usr_filter["id"])))
                        .otherwise(usr_filter["id"])).cast(LongType()).alias("partner_player_id"),
                    tick["user_id"].alias("ticket_user_id"),
                    b_Ap["ticket_id"].alias("bet_ticket_id"),
                    to_date(expr("b_Ap.created_at - INTERVAL 3 HOURS")).alias("fecha_operacion"),
                    lit('4').alias("operation_type_id"),
                    lit('44').alias("balance_type_id"),
                    concat(p['partner'], lit('9009999')).alias("partner_room_provider_id"),
                    lit('ADP').alias("Categoria_juego"),
                    lit('Apuestas Deportivas').alias("service_legal_type"),
                    concat(p['partner'], lit('9009')).alias("partner_room_id"),
                    #tick["currency_id"].cast(DecimalType(2,0)).alias("currency_id"),
                    # when((tick["bookmaker_id"].isin(88)) & (tick["currency_id"] == 41), lit(7))
                    #      .otherwise( tick["currency_id"]).alias('currency_id'),
                    when(tick["currency_id"] == 41, lit(7))
                        .when(tick["currency_id"] == 8, lit(9))
                        .when(tick["currency_id"] == 2, lit(4))
                        .otherwise(0).cast(IntegerType()).alias("currency_id"),
                    b_Ap["bonus_user_amount"].cast(DecimalType(15,2)).alias("importe_bono"),
                    concat(lit('04'), format_string("%02d", lit(8)), format_string("%02d", col("currency_id"))).alias("relacion_operation_t_t_c")
    ).groupBy(
        "partner_id", "fecha_operacion", "partner_player_id", "operation_type_id", "balance_type_id", "partner_room_provider_id", "Categoria_juego",
        "service_legal_type", "partner_room_id", "currency_id", "relacion_operation_t_t_c"
        ).agg(
            (count(col("bet_ticket_id"))).alias("cant_juegos"), 
            sum(col("importe_bono") * -1).cast(DecimalType(15,2)).alias("movement_amount")
            )
    
    # Premios Bonos AADD
    b_Pr = bet.filter(col("result_status_id").isin(1, 2, 3, 7)).filter(col("bonus_user_amount").isNotNull()).filter(col("amount") != 0)

    Premio_Bono_AADD = b_Pr.alias("b_Pr").join(tick.alias("tick"), tick["id"] == b_Pr["ticket_id"])\
        .join(usr_filter, tick["user_id"] == usr_filter["id"])\
            .join(p, tick["bookmaker_id"] == p['id'])\
                .select(
                        p['partner'].cast(IntegerType()).alias('partner_id'),
                        concat(p['partner'],  when(usr_filter['alira_id'].isNotNull(), usr_filter['alira_id'])
                            .when(usr_filter["bookmaker_id"].isin(88), concat(usr_filter["bookmaker_id"], format_string("%08d", usr_filter["id"])))
                            .otherwise(usr_filter["id"])).cast(LongType()).alias("partner_player_id"),
                        tick["user_id"].alias("ticket_user_id"),
                        b_Pr["ticket_id"].alias("bet_ticket_id"),
                        lit('3').alias("operation_type_id"),
                        lit('44').alias("balance_type_id"),
                        concat(p['partner'], lit('9009999')).alias("partner_room_provider_id"),
                        lit('ADP').alias("Categoria_juego"),
                        lit('Apuestas Deportivas').alias("service_legal_type"),
                        concat(p['partner'], lit('9009')).alias("partner_room_id"),
                        to_date(expr("b_Pr.settled_at - INTERVAL 3 HOURS")).alias("fecha_operacion"),
                        # to_date(expr("b_Pr.created_at - INTERVAL 3 HOURS")).alias("fecha_operacion"),
                        #tick["currency_id"].cast(DecimalType(2,0)).alias("currency_id"),
                        # when((tick["bookmaker_id"].isin(88)) & (tick["currency_id"] == 41), lit(7))
                        #      .otherwise( tick["currency_id"]).alias('currency_id'),
                        when(tick["currency_id"] == 41, lit(7))
                            .when(tick["currency_id"] == 8, lit(9))
                            .when(tick["currency_id"] == 2, lit(4))
                            .otherwise(0).cast(IntegerType()).alias("currency_id"),
                        (when(b_Pr["bonus_user_amount"].isNull(), 0))
                        .otherwise(b_Pr["potential_payment"] * ((b_Pr["bonus_user_amount"] / b_Pr["amount"])) ).cast(DecimalType(15,2)).alias("importe_bono"),
                        concat(lit('03'), format_string("%02d", lit(44)), format_string("%02d", col("currency_id"))).alias("relacion_operation_t_t_c")
        ).groupBy(
        "partner_id", "fecha_operacion", "partner_player_id", "operation_type_id", "balance_type_id", "partner_room_provider_id", "Categoria_juego",
        "service_legal_type", "partner_room_id", "currency_id", "relacion_operation_t_t_c"
        ).agg(
            (count(col("bet_ticket_id"))).alias("cant_juegos"), 
            sum("importe_bono").cast(DecimalType(15,2)).alias("movement_amount")
            )
    return (Apuesta_Bono_AADD.unionByName(Premio_Bono_AADD)).withColumn("id", concat(col("partner_player_id"), 
                                            date_format(col("fecha_operacion"), "yyyyMMdd"), 
                                            format_string("%02d", col("operation_type_id").cast(IntegerType())), 
                                            format_string("%02d", col("balance_type_id").cast(IntegerType())),
                                            format_string("%02d", col("currency_id").cast(IntegerType())),
                                            col("partner_room_id")
                                        ))

# COMMAND ----------

# MAGIC %md
# MAGIC #### GT - Get AyP Efectivo Apuestas Deportivas

# COMMAND ----------

### Apuestas y Premios Efectivo AADD (operation_type_id = 4 | balance_type_id = 1 | partner_room_provider_id = 0)
def AyP_Efectivo_AADD(tick, bet, usr, p):
    usr_filter = usr.filter((col("local_register") == '0') & (col("is_backoffice") == '0') & (col("is_services") == '0')).select("bookmaker_id", "id", 'alira_id')

   # u = u.filter((col("local_register") == '0') & (col("is_backoffice") == '0') & (col("is_services") == '0'))
    # Apuestas Efectivo AADD
    b_Ap = bet.filter(~col("result_status_id").isin(8, 9, 10, 11)).filter(col("amount").isNotNull())
    Apuestas_Efectivo_AADD = b_Ap.alias("b_Ap").join(tick.alias("tick"), tick["id"] == b_Ap["ticket_id"])\
        .join(usr_filter, tick["user_id"] == usr_filter["id"])\
            .join(p, tick["bookmaker_id"] == p['id'])\
                .select(
                        p['partner'].cast(IntegerType()).alias('partner_id'),
                        concat(p['partner'], 
                            when(usr_filter['alira_id'].isNotNull(), usr_filter['alira_id'])
                            .when(usr_filter["bookmaker_id"].isin(88), concat(usr_filter["bookmaker_id"], format_string("%08d", usr_filter["id"])))
                            .otherwise(usr_filter["id"])
                        ).cast(LongType()).alias("partner_player_id"),
                        tick["user_id"].alias("ticket_user_id"),
                        b_Ap["ticket_id"].alias("bet_ticket_id"),
                        to_date(expr("b_Ap.created_at - INTERVAL 3 HOURS")).alias("fecha_operacion"),
                        #tick["currency_id"].cast(DecimalType(2,0)).alias("currency_id"),
                        # when((tick["bookmaker_id"].isin(88)) & (tick["currency_id"] == 41), lit(7))
                        #      .otherwise( tick["currency_id"]).alias('currency_id'),
                        when(tick["currency_id"] == 41, lit(7))
                            .when(tick["currency_id"] == 8, lit(9))
                            .when(tick["currency_id"] == 2, lit(4))
                            .otherwise(0).cast(IntegerType()).alias("currency_id"),
                        lit('4').alias("operation_type_id"), # Apuesta 4
                        lit('1').alias("balance_type_id"), ### Efectivo = 1
                        concat(p['partner'], lit('9009999')).alias("partner_room_provider_id"),
                        lit('ADP').alias("Categoria_juego"),
                        lit('Apuestas Deportivas').alias("service_legal_type"),
                        concat(p['partner'], lit('9009')).alias("partner_room_id"),
                        (when(b_Ap["bonus_user_amount"].isNull(), b_Ap["amount"])
                        .otherwise(b_Ap["amount"] - b_Ap["bonus_user_amount"]).cast(DecimalType(15,2))).alias("importe_efectivo"),
                        concat(lit('04'), format_string("%02d", lit(1)), format_string("%02d", col("currency_id"))).alias("relacion_operation_t_t_c")
    ).groupBy(
        "partner_id", "fecha_operacion", "partner_player_id", "operation_type_id", "balance_type_id", "partner_room_provider_id", "Categoria_juego",
        "service_legal_type", "partner_room_id", "currency_id", "relacion_operation_t_t_c"
        ).agg(
            (count(col("bet_ticket_id"))).alias("cant_juegos"), 
            sum(col("importe_efectivo") * -1).cast(DecimalType(15,2)).alias("movement_amount")
            )

    # Premios Efectivos AADD (operation_type_id = 3 | balance_type_id = 1 | partner_room_provider_id = 0)
    Premios_Efectivo_AADD = bet.filter((bet.result_status_id.isin(1, 2, 3, 7)) & ((bet.amount.isNotNull()) | (bet.amount != 0))).alias("b_Pr")\
        .join(tick, tick.id == bet.ticket_id)\
            .join(usr_filter, tick["user_id"] == usr_filter["id"])\
                .join(p, tick["bookmaker_id"] == p['id'])\
                    .select(
                            p['partner'].cast(IntegerType()).alias('partner_id'),
                            concat(p['partner'], 
                                    when(usr_filter['alira_id'].isNotNull(), usr_filter['alira_id'])
                                    .when(usr_filter["bookmaker_id"].isin(88), concat(usr_filter["bookmaker_id"], format_string("%08d", usr_filter["id"])))
                                    .otherwise(usr_filter["id"])
                            ).cast(LongType()).alias("partner_player_id"),
                            tick["user_id"].alias("ticket_user_id"),
                            bet["ticket_id"].alias("bet_ticket_id"),
                            lit('3').alias("operation_type_id"), # Premio 3
                            lit('1').alias("balance_type_id"),
                            concat(p['partner'], lit('9009999')).alias("partner_room_provider_id"),
                            lit('ADP').alias("Categoria_juego"),
                            lit('Apuestas Deportivas').alias("service_legal_type"),
                            concat(p['partner'], lit('9009')).alias("partner_room_id"),
                            tick["user_id"].alias("ticket_user_id"),
                            to_date(expr("b_Pr.settled_at - INTERVAL 3 HOURS")).alias("fecha_operacion"),
                            # to_date(expr("b_Pr.created_at - INTERVAL 3 HOURS")).alias("fecha_operacion"),
                            #tick["currency_id"].cast(DecimalType(2,0)).alias("currency_id"),
                            # when((tick["bookmaker_id"].isin(88)) & (tick["currency_id"] == 41), lit(7))
                            #      .otherwise( tick["currency_id"]).alias('currency_id'),
                            when(tick["currency_id"] == 41, lit(7))
                                .when(tick["currency_id"] == 8, lit(9))
                                .when(tick["currency_id"] == 2, lit(4))
                                .otherwise(0).cast(IntegerType()).alias("currency_id"),
                            when(bet["bonus_user_amount"].isNull(), bet["potential_payment"])
                                .otherwise(bet["potential_payment"] * (1 - (bet["bonus_user_amount"] / bet["amount"]))).cast(DecimalType(15,2)).alias("importe_efectivo"),
                            concat(lit('03'), format_string("%02d", lit(1)), format_string("%02d", col("currency_id"))).alias("relacion_operation_t_t_c") 
    ).groupBy(
        "partner_id", "fecha_operacion", "partner_player_id", "operation_type_id", "balance_type_id", "partner_room_provider_id", "Categoria_juego",
        "service_legal_type", "partner_room_id", "currency_id", "relacion_operation_t_t_c"
        ).agg(
            (count(col("bet_ticket_id"))).alias("cant_juegos"), 
            sum("importe_efectivo").cast(DecimalType(15,2)).alias("movement_amount")
            )
    
    return (Apuestas_Efectivo_AADD.unionByName(Premios_Efectivo_AADD)).withColumn("id", concat(col("partner_player_id"), 
                                            date_format(col("fecha_operacion"), "yyyyMMdd"), 
                                            format_string("%02d", col("operation_type_id").cast(IntegerType())), 
                                            format_string("%02d", col("balance_type_id").cast(IntegerType())),
                                            format_string("%02d", col("currency_id").cast(IntegerType())),
                                            col("partner_room_id")
                                        ))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Union de DF Auxiliar de Fact Movement y Dim Room
# MAGIC <p> A partir de los dataframe auxiliares Fact Movement y Dim Room se genera el dataframe de Apuestas y Premios

# COMMAND ----------

def Create_Apuestas_Premios(salas, fm):
    ap = fm.join(salas, "partner_room_id", "left") \
            .select(
                fm["partner_id"],
                fm["fecha_operacion"],
                fm["partner_player_id"], 
                fm["operation_type_id"].cast(IntegerType()), # DecimalType(2,0)
                fm["balance_type_id"].cast(IntegerType()),   # DecimalType(2,0)
                when(salas["partner_room_provider_id"].isNull(), "Revisar Configuración de Salas")\
                    .otherwise(salas["partner_room_provider_id"]).cast(LongType()).alias('partner_room_provider_id'),
                when(salas["service_legal_type_id"].isNull(), "Revisar Configuración de Salas").otherwise(salas["service_legal_type_id"]).alias('Categoria_juego'),
                when(salas["service_legal_type"].isNull(), "Revisar Configuración de Salas").otherwise(salas["service_legal_type"]).alias('service_legal_type'),
                fm["partner_room_id"],
                fm["currency_id"],
                fm["partner_operation_id"],
                fm["movement_amount"],
                fm["partner_event_reference"]
            ).distinct()

    ap = ap.withColumn("relacion_operation_t_t_c",
                                concat(
                                    format_string("%02d", col("operation_type_id").cast(IntegerType())), 
                                    format_string("%02d", col("balance_type_id").cast(IntegerType())),
                                    format_string("%02d", col("currency_id").cast(IntegerType()))
                                ))
    return ap

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ejecución del DataFrame Apuestas Premios por Base de Datos: DWBPLAY_PY, DWBPLAY_SF, DWBPLAY_CABA y DWBPLAY_CORDOBA
# MAGIC <p> Se ejecuta la función Create_Apuestas_Premios para obtener el dataframe df_AyP de cada base de datos: DWBPLAY_PY, DWBPLAY_SF, DWBPLAY_CABA  y DWBPLAY_CORDOBA.

# COMMAND ----------

# DWBPLAY_PY
df_AyP0 = Create_Apuestas_Premios(df_salas, df_fm0)
# DWBPLAY_SF
df_AyP1 = Create_Apuestas_Premios(df_salas, df_fm1)
# DWBPLAY_CABA
df_AyP2 = Create_Apuestas_Premios(df_salas, df_fm2)
# DWBPLAY_CORDOBA
df_AyP3 = Create_Apuestas_Premios(df_salas, df_fm3)

if bl_Carga_GT:
    # GT JUJUY & PARANA & MENDOZA
        # Apuestas y Premios Bono Apuesta Deportiva
    df_AyP_Bono_AADD_GT = AyP_Bono_AADD(df_tick_GT, df_b_GT, df_usr_GT, df_partner_Migra)
        # Apuestas y Premios Efectivo Apuestas Deportivas
    df_AyP_Efectivo_AADD_GT = AyP_Efectivo_AADD(df_tick_GT, df_b_GT, df_usr_GT, df_partner_Migra)
        # Apuestas y Premios Efectivo Casino
    df_AyP_Efectivo_Casino_GT = AyP_Efectivo_Casino(df_tick_GT, df_b_GT, df_usr_GT, df_salas, df_partner_Migra)
        # Apuestas y Premios Bono Casino
    df_AyP_Bono_Casino_GT = AyP_Bono_Casino(df_tick_GT, df_b_GT, df_usr_GT, df_salas, df_partner_Migra)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creacion de DF_AyP agrupados por Sesión
# MAGIC <p> Se calculará el agrupado por partner_player_id de la columna 'movement_amount'

# COMMAND ----------

from pyspark.sql.functions import countDistinct
def Create_Acumulado_AyP(ap):
    acum_ap = ap.groupBy(
        "partner_id",  "fecha_operacion", "partner_player_id", "operation_type_id", "balance_type_id", "Categoria_juego", "Service_legal_type", "partner_room_id", "partner_room_provider_id", "currency_id", "relacion_operation_t_t_c"
        ).agg(
            countDistinct(when(col("operation_type_id") == 4, col("partner_event_reference"))).alias("cant_juegos"),
            sum(col("movement_amount")).alias("movement_amount")
            )    
    res = acum_ap.withColumn("id", concat(  col("partner_player_id"), 
                                            date_format(col("fecha_operacion"), "yyyyMMdd"), 
                                            format_string("%02d", col("operation_type_id").cast(IntegerType())), 
                                            format_string("%02d", col("balance_type_id").cast(IntegerType())),
                                            format_string("%02d", col("currency_id").cast(IntegerType())),
                                            col("partner_room_id")
                                                )).select(
                                                    "id", 
                                                    col("partner_id").cast(DecimalType(15,0)), 
                                                    ("fecha_operacion"), 
                                                    "partner_player_id", 
                                                    col("operation_type_id").cast(DecimalType(2,0)), 
                                                    col("balance_type_id").cast(DecimalType(2,0)), 
                                                    col("partner_room_provider_id").cast(DecimalType(15,0)), 
                                                    "Categoria_juego", 
                                                    "Service_legal_type", 
                                                    col("partner_room_id").cast(DecimalType(15,0)), 
                                                    col("currency_id").cast(DecimalType(2,0)), 
                                                    "relacion_operation_t_t_c", 
                                                    "cant_juegos", 
                                                    "movement_amount"
                                                )
    return res

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ejecución del DataFrame para obtener el Acumulado de Apuestas y Premios por Base de Datos: DWBPLAY_PY, DWBPLAY_SF, DWBPLAY_CABA, DWBPLAY_CORDOBA y GT Jujuy
# MAGIC <p> Se ejecuta la función Create_Acumulado_AyP para obtener el dataframe con la información de las verticales de la sala en las bases de datos DWBPLAY_PY, DWBPLAY_SF, DWBPLAY_CABA y DWBPLAY_CORDOBA

# COMMAND ----------

# DWBPLAY_PY
df_Acum_AyP0 = Create_Acumulado_AyP(df_AyP0)
# DWBPLAY_SF
df_Acum_AyP1 = Create_Acumulado_AyP(df_AyP1)
# DWBPLAY_CABA
df_Acum_AyP2 = Create_Acumulado_AyP(df_AyP2)
# DWBPLAY_CORDOBA
df_Acum_AyP3 = Create_Acumulado_AyP(df_AyP3)

if bl_Carga_GT:
    # GT JUJUY
    df_AyP_GT = df_AyP_Bono_AADD_GT.unionByName(df_AyP_Efectivo_AADD_GT).unionByName(df_AyP_Efectivo_Casino_GT).unionByName(df_AyP_Bono_Casino_GT)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Calculo de cotizacion en USD "amount_USD"

# COMMAND ----------

tipo_de_cambio = spark.read.table("entprod_mdn.bplay.tipos_de_cambio")

# COMMAND ----------

def Calcular_amountUSD(AyP, tc):
        # Calculo de amoun_USD
    AyP_tc = AyP.join(tc, AyP["fecha_operacion"] == tc["Fecha"], "left")\
        .select(
            col("id"), 
            col("partner_id"),              # .cast(DecimalType(15,0)), 
            col("fecha_operacion"), 
            col("partner_player_id"), 
            col("operation_type_id"),       # .cast(DecimalType(2,0)), 
            col("balance_type_id"),         # .cast(DecimalType(2,0)), 
            col("partner_room_provider_id"),    # .cast(DecimalType(15,0)), 
            col("Categoria_juego"), 
            col("Service_legal_type"), 
            col("partner_room_id"),             # .cast(DecimalType(15,0)), 
            col("currency_id"),                 # .cast(DecimalType(2,0)), 
            col("relacion_operation_t_t_c"), 
            col("cant_juegos"),                 # .cast(LongType()), 
            col("movement_amount"),             # .cast(DecimalType(25, 2)),
            when((col("currency_id") == "7") | (col("currency_id") == "41") , (col("movement_amount")/col("ARS_CCL")))\
                .when(col("currency_id") == "15", (col("movement_amount")/col("PYG")))\
                    .when((col("currency_id") == "9") | (col("currency_id") == "8"), (col("movement_amount")/col("BRL")))\
                        .otherwise(col("movement_amount")).cast(DecimalType(15, 2)).alias("amount_USD") 
                                                # .cast(DecimalType(25, 2)
            )
    return AyP_tc

# COMMAND ----------

# DWBPLAY_PY
df_AyP_origen0 = Calcular_amountUSD(df_Acum_AyP0, tipo_de_cambio)
# DWBPLAY_SF
df_AyP_origen1 = Calcular_amountUSD(df_Acum_AyP1, tipo_de_cambio)
# DWBPLAY_CABA
df_AyP_origen2 = Calcular_amountUSD(df_Acum_AyP2, tipo_de_cambio)
# DWBPLAY_CORDOBA
df_AyP_origen3 = Calcular_amountUSD(df_Acum_AyP3, tipo_de_cambio)

if bl_Carga_GT:
    # GT Jujuy, Paraná y Mendoza
    df_AyP_GT_origen = Calcular_amountUSD(df_AyP_GT, tipo_de_cambio)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Union de DF Acumulado AyP
# MAGIC <p> Se realiza la unificacion de las ejecuciones de las bases de datos DWBPLAY_PY, DWBPLAY_SF, DWBPLAY_CABA y DWBPLAY_CORDOBA.

# COMMAND ----------

if bl_Carga_GT:
    # Union Alira & GT
    df_AyP_origen = df_AyP_origen0.unionByName(df_AyP_origen1).unionByName(df_AyP_origen2).unionByName(df_AyP_origen3).unionByName(df_AyP_GT_origen)
    print(bl_Carga_GT)
else:
    # Union Alira
    df_AyP_origen = df_AyP_origen0.unionByName(df_AyP_origen1).unionByName(df_AyP_origen2).unionByName(df_AyP_origen3)
    print(bl_Carga_GT)

# COMMAND ----------

# display(df_AyP_Efectivo_Casino_GT.filter((col('partner_id').isin(130, 460, 256)) & (col('fecha_operacion') >= '2025-05-01')).groupBy('partner_id', 'fecha_operacion', 'operation_type_id').agg(sum('movement_amount').alias('ggr')).orderBy('partner_id', 'fecha_operacion', 'operation_type_id'))

# display(df_AyP_Efectivo_Casino_GT.filter((col('partner_id').isin(130, 460, 256)) & (col('fecha_operacion') >= '2025-05-01')).groupBy('partner_id', 'fecha_operacion', 'operation_type_id').agg(sum("movement_amount").alias('amount')).orderBy('partner_id', 'fecha_operacion', 'operation_type_id'))

# COMMAND ----------

# display(df_AyP_GT_origen.filter((col('partner_id').isin(130, 460, 256)) & (col('fecha_operacion') >= '2025-05-01')).groupBy('partner_id', 'fecha_operacion','balance_type_id').agg(sum('movement_amount').alias('ggr')).orderBy('partner_id', 'fecha_operacion', 'balance_type_id'))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Preparación de la Tabla 'Apuestas y Premios' para la carga

# COMMAND ----------

# Obtengo el df de la tabla Apuestas Y Premios ya creada
def get_dif(nuevo):
    origen = spark.read.table(delta_table_AyP)
    # Transformat a Dataframe y Seleccionar los campos por comparar
    origen = origen.select("id", "partner_id", "fecha_operacion", "partner_player_id", "operation_type_id", "balance_type_id", "partner_room_provider_id", "Categoria_juego", "Service_legal_type", "partner_room_id", "currency_id", "relacion_operation_t_t_c", "cant_juegos","movement_amount", "amount_USD")
    # Obtengo las filas nuevas y actualizadas 
    dif = nuevo.exceptAll(origen)
    # Agrega la columna de actualización
    res = dif.withColumn("fecha_actualizacion", current_timestamp()).withColumn("fecha_creacion", to_timestamp(col("fecha_operacion")))
    return res

# COMMAND ----------

# Filtra los ultimos 15 días
def filter_last_15_days(df): 
    last_15_days = concat(date_sub(current_timestamp(), 20).cast("string"),lit(" 23:59:59")).cast("timestamp")
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
# MAGIC ## 7. Upsert de Tabla Delta "apuestas_y_premios"
# MAGIC <p> Obtenemos la tabla original para actualizar la Delta Table 'apuestas_y_premios' con el dataframe df_AyP recién creado.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Obtención de la Última Tabla Generada para Actualización
# MAGIC <p> Si ya existe la Delta Table de Apuestas y Premios creada se obtendra la tabla y se actualizará con el DataFrame generado en esta ejecución.
# MAGIC <p> Si no existiera, se definirá la limitación las particiones en 8, y se generará un nuevo Delta Table de "Apuestas y Premios".

# COMMAND ----------

print(delta_table_AyP)
print(exist)
print(bl_Carga_GT)
print('Se ejecutará en PRD' if PROD else 'Se ejecutará en DEV')
print('Se realizará la Carga Historica' if bl_Carga_Historica else 'Se cargara el año corriente')

# COMMAND ----------

# df_AyP_origen = df_AyP_GT_origen

# COMMAND ----------

# Verificar si la ruta contiene una tabla Delta
if exist:
    # Se Obtiene las filas por actualizar
    df_AyP_dif = get_dif(df_AyP_origen)
    # Se obtiene las actualizaciones de los ultimos 15 días
    df_AyP_dif = filter_last_15_days(df_AyP_dif)
    # Se elimina hasta el ultimo día de ejecucion
    df_AyP_act = filter_last_day(df_AyP_dif)
    print("Se creo df_AyP_act")

    # Actualizar el DataFrame existente
    dt_origen = DeltaTable.forName(spark, delta_table_AyP)   
    # Upsert del DataFrame de Apuestas y Premios
    dt_origen.alias("dest")\
        .merge(source=df_AyP_act.alias("update"), condition="dest.id = update.id")\
            .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    print("Se actualizo exitosamente la base de Apuestas y Premios")
else:
    # Se elimina hasta el ultimo día de ejecucion
    df_AyP_act = filter_last_day(df_AyP_origen)
    # Agregado de las columnas identificatorias para el upgrade de la base
    df_AyP = df_AyP_act \
        .withColumn("fecha_creacion", to_timestamp(col("fecha_operacion"))) \
            .withColumn("fecha_actualizacion", current_timestamp()) \
                .coalesce(8)  # Limitar a 8 particiones el DF "df_AyP"
    # Agregado de las columnas identificatorias para el upgrade de la base
    df_AyP.write\
        .mode("overwrite")\
            .saveAsTable(delta_table_AyP)
    print("Se creó exitosamente la base de Apuestas y Premios")

# COMMAND ----------



# COMMAND ----------

# # Se elimina hasta el ultimo día de ejecucion
# df_AyP_act = filter_last_day(df_AyP_origen)
# # Agregado de las columnas identificatorias para el upgrade de la base
# df_AyP = df_AyP_act \
#     .withColumn("fecha_creacion", to_timestamp(col("fecha_operacion"))) \
#         .withColumn("fecha_actualizacion", current_timestamp()) \
#             .coalesce(8)  # Limitar a 8 particiones el DF "df_AyP"
# # Agregado de las columnas identificatorias para el upgrade de la base
# df_AyP.write\
#     .mode("overwrite")\
#         .saveAsTable(delta_table_AyP)
# print("Se creó exitosamente la base de Apuestas y Premios")