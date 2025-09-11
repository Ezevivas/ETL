# Databricks notebook source
# MAGIC %md
# MAGIC # Apuestas_Deportivas_MDN-bplay_PySpark.ipynb
# MAGIC <h> by PySpark
# MAGIC #### Estado: **OK**
# MAGIC <p> Source: Se realizará la lectura de archivos en formato parquet para obtener los dataframes necesarios para generar la tabla 'Fact Coupon Row', y cargarla en formato Delta Table para su posterior consulta.
# MAGIC <p> Se tomará la información desde el STG1 de Datalake para generar un consumible de STG2 en formato Delta Table.
# MAGIC <p> Inputs:
# MAGIC   <ol>
# MAGIC     <li>Coupon_Row</li>
# MAGIC     <li>Coupon</li>
# MAGIC     <li>Combination</li>
# MAGIC     <li>Combination_status_name</li>
# MAGIC     <li>Event</li>
# MAGIC     <li>Bet_Ticket</li>
# MAGIC     <li>Player_Freebet</li>
# MAGIC     <li>Freebet_Configuration</li>
# MAGIC     <li>Player</li>
# MAGIC     <li>Balance_t_name</li>
# MAGIC     <li>Movement</li>
# MAGIC     <li>Operation</li>
# MAGIC   </ol>
# MAGIC <p> Output: Delta Table actualizado de "fact_coupon_row" y su carga en el Warehouse de Databricks.

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

# COMMAND ----------

from pyspark.sql.functions import col, concat, lit, expr, when, count, max, current_date, date_sub, current_timestamp, to_timestamp, format_string, date_trunc, from_utc_timestamp, to_date, date_format, substring
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType, DecimalType, DateType, TimestampType, StringType, LongType, DoubleType
from delta.tables import DeltaTable

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
# MAGIC   <li>Se definen los paths de los archivos parquet necesarios para cada Base de Datos DWBPLAY_CABA y DWBPLAY_CORDOBA.</li>
# MAGIC   <li> Luego se leen los archivos parquet para crear los dataframes en formato PySpark.</li>
# MAGIC </ol>

# COMMAND ----------

### Switch Carga Historica/Actualzacion
bl_Carga_Historica = True 

if bl_Carga_Historica:
    print("se realizara la carga historica de Movement y Operation")
else:
    print("Se actualizara el año corriente")

# COMMAND ----------

  # Get APUESTAS CABA
df_cr2 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_caba/APUESTAS/coupon_row')
df_c2 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_caba/APUESTAS/coupon')
df_csn2 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_caba/APUESTAS/coupon_status_name')
df_co2 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_caba/APUESTAS/combination')
df_cosn2 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_caba/APUESTAS/combination_status_name')
df_e2 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_caba/APUESTAS/event')
df_bt2 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_caba/APUESTAS/bet_ticket')
df_pfb2 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_caba/APUESTAS/player_freebet')
df_fbc2 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_caba/APUESTAS/freebet_configuration')

  # Get OGP CABA
df_p2 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_caba/OGP/player')
df_btn2 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_caba/OGP/balance_t_name')

  # Get APUESTAS CORDOBA
df_cr3 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_cordoba/APUESTAS/coupon_row')
df_c3 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_cordoba/APUESTAS/coupon')
df_csn3 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_cordoba/APUESTAS/coupon_status_name')
df_co3 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_cordoba/APUESTAS/combination')
df_cosn3 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_cordoba/APUESTAS/combination_status_name')
df_e3 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_cordoba/APUESTAS/event')
df_bt3 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_cordoba/APUESTAS/bet_ticket')
df_pfb3 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_cordoba/APUESTAS/player_freebet')
df_fbc3 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_cordoba/APUESTAS/freebet_configuration')

  # Get OGP CORDOBA
df_p3 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_cordoba/OGP/player')
df_btn3 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_cordoba/OGP/balance_t_name')

if bl_Carga_Historica:
    print("se realizara la carga historica de Movement y Operation")
      # Get DF Movement
    df_m2 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_caba/ogp/movement/')
    df_m3 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_cordoba/OGP/movement/')
      # Get DF Operation
    df_o2 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_caba/ogp/operation/')
    df_o3 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_cordoba/OGP/operation/')
else:
    print("Se actualizara el año corriente")
      # Get DF Movement
    df_m2 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_caba/ogp/movement/Year=2025/*')

    df_m3 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_cordoba/OGP/movement/Year=2025/*')

      # Get DF Operation
    df_o2 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_caba/ogp/operation/Year=2025/*')

    df_o3 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_cordoba/OGP/operation/Year=2025/*')

# COMMAND ----------

try:
    # Apuestas Deportivas GT
      #Get Bets GT
    df_bet = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/bplaybet_arg/dbo/bets')
      #Get Tickets GT
    df_tick = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/bplaybet_arg/dbo/tickets')
      #Get Users GT
    df_usr = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/bplaybet_arg/dbo/users')
      # Get Bet Selections
    df_bs = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/bplaybet_arg/dbo/bet_selections')
      # Get Channel login activities
    df_la = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/bplaybet_arg/dbo/login_activities')
      # Get Result Statuses
    df_rs = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/bplaybet_arg/dbo/result_statuses')
      # Get Translation Entries
    df_te = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/bplaybet_arg/dbo/translation_entries')
      # Get Markets
    df_m = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/bplaybet_arg/dbo/markets')
      # Get Selections
    df_s = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/bplaybet_arg/dbo/selections')
      # Get Fixtures
    df_f = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/bplaybet_arg/dbo/fixtures')
    ### Switch Carga GT Parana & Jujuy
    bl_Carga_GT = True
except Exception as e:
    print(f"No se pudieron leer las tablas de Apuestas Deportivas GT: {e}")
    bl_Carga_GT = False

# COMMAND ----------

# Definir los datos
data = [(46, 460), (13, 130), (88, 256)]
#data = [(46, 460), (13, 130), (88, 880)]

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

#   Path PRD del Delta Table Apuestas Deportivas
delta_table_FCR_prd = "entprod_mdn.bplay.apuestas_deportivas"

#   Path DEV del Delta Table Apuestas Deportivas
delta_table_FCR_dev = "entprod_mdn.default.apuestas_deportivas"

# #   Variable PROD/DEV
# PROD = True

#   Switch PROD/DEV
if PROD:
    delta_table_FCR = delta_table_FCR_prd
else:
    delta_table_FCR = delta_table_FCR_dev

# Manejar el caso en que la tabla no exista
try:
    # Intentar leer la tabla Delta
    df_origen = spark.read.table(delta_table_FCR)
    count = df_origen.select("partner_coupon_row_id").count()

    # Validar la tabla Fact Coupon Row
    if count > 0:
        exist = True
    else:
        exist = False
except Exception as e:
    # print(f"La tabla {delta_table_Balance} no existe o no se puede leer: {e}")
    exist = False


# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Get Apuesta Deportiva

# COMMAND ----------

# MAGIC %md
# MAGIC ### Definición de la Función Apuesta Deportiva Alira
# MAGIC <p> Esta función genera un dataframe con la información de las transacciones de los player por jurisdiccion y tipo de juego, organizado por fecha.

# COMMAND ----------

from pyspark.sql.functions import count
def Create_Apuestas_Deportivas(cr, c, co, p, o, m, e, bt, pfb, fbc):
    Apuestas_Deportivas = cr.join(c, cr["coupon"] == c["id"])\
                        .join(co, c["id"] == co["coupon"])\
                                .join(p.filter((col("status") != 1) & (col("type") == 3)), p["id"] == c["player"])\
                                    .join(o.filter(col("type").isin(3,4)), (cr["coupon"] == o["event_reference"]) & (c["player"] == o["player"]))\
                                        .join(m.filter(col("balance_type") != 15), o["id"] == m["operation"])\
                                                .join(e, cr["event"] == e["id"])\
                                                    .join(bt, co["coupon"] == bt["bet_id"], 'left')\
                                                            .join(pfb, co["coupon"] == pfb["coupon"], 'left')\
                                                                .join(fbc, pfb["freebet_configuration"].cast(IntegerType()) == fbc["id"].cast(IntegerType()), 'left')\
                    .select(
                        c["created_date"],
                        co["resolution_date"],
                        when((o["type"] == 3) & (co["resolution_date"].isNotNull()), co["resolution_date"].cast(DateType()))
                            .otherwise(c["created_date"]).cast(DateType()).alias("fecha"),
                        concat(p["partner"], c["player"]).cast("decimal(15,0)").alias("partner_player_id"),
                        concat(p["partner"], cr["coupon"], cr["id"]).alias("partner_coupon_row_id"),
                        p["partner"].alias("partner_id"),
                        c["id"].alias("coupon_id"),
                        cr["id"].alias("row_id"),
                        co["size"].alias("bet_size"),
                        when(co["size"] > 1, (count("*").over(Window.partitionBy(cr["coupon"], e["competition"])) / 2))
                            .otherwise(1).alias("cant_ap_evento"),
                        when(co["size"] == 1, lit(1))
                        .when(co["size"] == (count("*").over(Window.partitionBy(cr["coupon"], e["competition"])) / 2), lit(2))
                            .otherwise(lit(0)).alias("evento_unico"),
                        when(co["size"] > 1, max(co["resolution_date"]).over(Window.partitionBy(cr["coupon"], e["competition"])))
                            .otherwise(co["resolution_date"]).alias("max_resolution_date_comb"),
                        e["estimated_end_date"],when(co["size"] > 1, max(e["estimated_end_date"]).over(Window.partitionBy(cr["coupon"], e["competition"]))).otherwise(co["resolution_date"]).alias("max_event_end_date"),
                        c["bet_type"].cast(IntegerType()).alias("bet_type_id"),
                        cr["live_betting"].cast(IntegerType()).alias("live_betting_id"),
                        when(c["channel"].like("%MOBILE%"), "MOBILE")
                            .when(c["channel"].like("%WEB%"), "WEB")
                                .otherwise("revisar").alias("coupon_channel"),
                        concat(p["partner"], cr["event"]).alias("partner_event_id"),
                        cr["event"].alias("event_id"),
                        when((co["resolution_date"].isNotNull()) & (co["resolution_date"].cast("date") < current_date()), "Apuesta Resuelta")
                            .when((co["resolution_date"].isNotNull()) & (co["resolution_date"].cast("date") == current_date()), 'Apuesta Pendiente')
                                .when(e["start_date"] >= current_date(), "Apuesta Pendiente")
                                    .otherwise("Apuesta No Saldada").alias("resolucion"),
                        cr["market"],
                        cr["bet_offer_description"],
                        cr["odds"].cast("decimal(10,4)").alias("row_odds"),
                        co["odds"].cast("decimal(10,4)").alias("combination_odds"),
                        co["stake"].cast("decimal(15,2)").alias("combination_stake"),
                        (m["amount"] / co["size"]).cast(DecimalType(15,2)).alias("row_amount"),
                        m["balance_type"].alias("balance_type_id"),
                        co["status"].alias("combination_status_id"),
                        o["type"].alias("operation_type_id"),
                        c["room"].alias("room_id"),
                        cr["provider"].alias("provider_id"),
                        bt["ticket_id"].alias("cupon_externo"),
                        pfb["freebet_configuration"].cast(IntegerType()).alias("freebet_configuration_id"),
                        fbc["name"].alias("freebet_configuration")
                    ).distinct()\
                        .withColumn("diccionario_aadd", 
                        concat(
                            format_string("%02d", col("bet_type_id").cast(IntegerType())), 
                            format_string("%02d", col("balance_type_id").cast(IntegerType())),
                            format_string("%02d", col("combination_status_id").cast(IntegerType())),
                            format_string("%02d", col("operation_type_id").cast(IntegerType())),
                            format_string("%02d", col("live_betting_id").cast(IntegerType()))
                        ))
                        
    return Apuestas_Deportivas

# COMMAND ----------

# MAGIC %md
# MAGIC ### Definición de la Función Apuesta Deportiva GT
# MAGIC <p> Esta función genera un dataframe con la información de las transacciones de los player por jurisdiccion y tipo de juego, organizado por fecha.

# COMMAND ----------

# MAGIC %md
# MAGIC ### AADD Efectivo

# COMMAND ----------

def Create_AADD_Eftvo_GT(bet, tick, usr, bs, la, rs, te, s, m, f, p):
    usr_f = usr.filter((col("local_register") == '0') & (col("is_backoffice") == '0') & (col("is_services") == '0'))
    
    # Apuestas Efectivo AADD OK
    Ap_Ef_AADD = bet.filter(~col("result_status_id").isin(8, 9, 10, 11)).filter(col("amount").isNotNull()).alias("b_Ap")\
        .join(tick.alias("tick"), tick["id"] == bet["ticket_id"])\
            .join(usr_f, tick["user_id"] == usr_f["id"])\
                .join(p, tick["bookmaker_id"] == p['id'])\
                    .join(bs.alias("bs"), bet["id"] == bs["bet_id"], 'left')\
                        .join(la, tick["login_activity_id"] == la["id"], 'left')\
                            .join(rs, bet['result_status_id'] == rs['id'], 'left')\
                                .join(te.filter(col('language_code') == 'es'), rs['name'] == te['translation_id'], 'left')\
                                    .join(s, bs['selection_id'] == s['id'], 'left')\
                                        .join(m, s['market_id'] == m['id'], 'left')\
                                            .join(f, f['id'] == m['fixture_id'], 'left')\
                    .select(
        date_trunc('second', from_utc_timestamp('b_Ap.created_at', 'America/Buenos_Aires')).alias('created_date'),
        date_trunc('second', from_utc_timestamp('b_Ap.settled_at', 'America/Buenos_Aires')).alias('resolution_date'),
        to_date(expr("b_Ap.created_at - INTERVAL 3 HOURS")).alias("fecha"),        
        concat(p["partner"], when(usr_f['alira_id'].isNotNull(), usr_f['alira_id'])
            .when(usr_f['bookmaker_id'].isin(88), concat(usr_f['bookmaker_id'], format_string("%08d", usr_f["id"])))
                .otherwise(usr_f["id"])).cast(LongType()).alias("partner_player_id"), ##### Migracion ####        
        concat(p["partner"], tick['id'], bet['id']).cast(LongType()).alias('partner_coupon_row_id'),
        p["partner"].cast(LongType()).alias('partner_id'),
        tick['id'].cast(LongType()).alias('coupon_id'),
        bet['id'].alias('row_id'),
        count("*").over(Window.partitionBy(tick['id'])).alias("bet_size"), # Cantidad de Ticket_id (coupon_id)
        lit(None).cast(DoubleType()).alias('cant_ap_evento'),  
        lit(None).cast(IntegerType()).alias('evento_unico'),
        lit(None).cast(TimestampType()).alias('max_resolution_date_comb'),
        lit(None).cast(TimestampType()).alias('max_event_end_date'),
        lit(None).cast(TimestampType()).alias('estimated_end_date'),
        when(tick['ticket_type_id'].cast(IntegerType()) == 2, lit("3"))
            .when(tick['ticket_type_id'].cast(IntegerType()) == 3, lit("4"))
            .otherwise(tick['ticket_type_id'].cast(IntegerType())).alias("bet_type_id"),
        # tick['ticket_type_id'].cast(IntegerType()).alias("bet_type_id"),
        when((bet['created_at'].isNotNull()) & (f['start_at'].isNotNull()) & \
            (date_format(from_utc_timestamp(bet['created_at'], 'America/Buenos_Aires'), 'yyyy-MM-dd HH:mm:ss') \
                >= date_format(from_utc_timestamp(f['start_at'], 'America/Buenos_Aires'), 'yyyy-MM-dd HH:mm:ss')), \
                    lit(1)).otherwise(lit(0)).cast(IntegerType()).alias('live_betting_id'),
        when(la['device_type'].like("%desktop%"), 'WEB')
            .when(la['device_type'].like("%mobile%"), 'MOBILE')
                .otherwise("revisar").alias("coupon_channel"),
        when(tick["bookmaker_id"].isin(88), concat(
            tick["bookmaker_id"], lit(0), s['id']))
        .otherwise(concat(p["partner"], s['id'])).alias("partner_event_id"),
        bs['selection_id'].alias("event_id"),
        when(rs['id'].isin(1, 2, 4, 7), 'Apuesta Resuelta').when(rs['id'].isin(5, 6), 'Apuesta No Saldada').otherwise('Apuesta Pendiente').alias('resolucion'),  
        m['code'].alias('market'),
        lit(None).cast(StringType()).alias('bet_offer_description'),
        bet['total_odd'].alias('row_odds'),
        (bet['potential_payment'] / bet['amount']).cast(DecimalType(10,4)).alias('combination_odds'),
        tick['total_amount'].cast(DecimalType(14,2)).alias('combination_stake'),
        (when(bet["bonus_user_amount"].isNull(), bet["amount"] / count("*").over(Window.partitionBy(bet['id'])))
            .otherwise((bet["amount"] - bet["bonus_user_amount"]) / count("*").over(Window.partitionBy(bet['id'])))*-1).cast(DecimalType(14,2)).alias("row_amount"), # Premios Efectivo
        lit('1').alias("balance_type_id"),  # Efectivo 1
        tick['ticket_status_id'].alias('combination_status_id'),
        lit('4').alias("operation_type_id"), # Apuesta 4
        lit('9009').alias('room_id'),
        lit('999').alias('provider_id'),
        lit(None).cast(StringType()).alias('cupon_externo'),
        lit(None).cast(IntegerType()).alias('freebet_configuration_id'),
        lit(None).cast(StringType()).alias('freebet_configuration')
                )
                                                
    # Premios Efectivo AADD OK
    Pr_Ef_AADD = bet.filter((bet.result_status_id.isin(1, 2, 3, 7)) & (bet.amount.isNotNull()) & (bet.amount != 0)).alias("bet")\
        .join(tick, tick['id'] == bet['ticket_id'])\
            .join(usr_f, tick["user_id"] == usr_f["id"])\
                .join(p, tick["bookmaker_id"] == p['id'])\
                    .join(bs.alias("bs"), bet["id"] == bs["bet_id"], 'left')\
                        .join(la, tick["login_activity_id"] == la["id"], 'left')\
                            .join(rs, bet['result_status_id'] == rs['id'], 'left')\
                                .join(te.filter(col('language_code') == 'es'), rs['name'] == te['translation_id'], 'left')\
                                    .join(s, bs['selection_id'] == s['id'], 'left')\
                                        .join(m, s['market_id'] == m['id'], 'left')\
                                            .join(f, f['id'] == m['fixture_id'], 'left')\
                                                .select(
        date_trunc('second', from_utc_timestamp('bet.created_at', 'America/Buenos_Aires')).alias('created_date'),
        date_trunc('second', from_utc_timestamp('bet.settled_at', 'America/Buenos_Aires')).alias('resolution_date'),
        #when(bet.settled_at.isNotNull(), bet.settled_at).otherwise(bet.created_at).cast(DateType()).alias("fecha"),
        to_date(expr("bet.settled_at - INTERVAL 3 HOURS")).alias("fecha"),
        concat(p["partner"], when(usr_f['alira_id'].isNotNull(), usr_f['alira_id'])
            .when(usr_f['bookmaker_id'].isin(88), concat(usr_f['bookmaker_id'], format_string("%08d", usr_f["id"])))
                .otherwise(usr_f["id"])).cast(LongType()).alias("partner_player_id"), ##### Migracion ####        
        concat(p["partner"], tick['id'], bet['id']).cast(LongType()).alias('partner_coupon_row_id'),
        concat(p["partner"]).cast(LongType()).alias('partner_id'),
        tick['id'].cast(LongType()).alias('coupon_id'),
        bet['id'].alias('row_id'),
        count("*").over(Window.partitionBy(tick['id'])).alias("bet_size"),
        lit(None).cast(DoubleType()).alias('cant_ap_evento'),  
        lit(None).cast(IntegerType()).alias('evento_unico'),
        lit(None).cast(TimestampType()).alias('max_resolution_date_comb'),
        lit(None).cast(TimestampType()).alias('max_event_end_date'),
        lit(None).cast(TimestampType()).alias('estimated_end_date'),
        when(tick['ticket_type_id'].cast(IntegerType()) == 2, lit("3"))
            .when(tick['ticket_type_id'].cast(IntegerType()) == 3, lit("4"))
            .otherwise(tick['ticket_type_id'].cast(IntegerType())).alias("bet_type_id"),
        # tick['ticket_type_id'].cast(IntegerType()).alias("bet_type_id"),
        when((bet['created_at'].isNotNull()) & (f['start_at'].isNotNull()) & \
            (date_format(from_utc_timestamp(bet['created_at'], 'America/Buenos_Aires'), 'yyyy-MM-dd HH:mm:ss') \
                >= date_format(from_utc_timestamp(f['start_at'], 'America/Buenos_Aires'), 'yyyy-MM-dd HH:mm:ss')), \
                    lit(1)).otherwise(lit(0)).cast(IntegerType()).alias('live_betting_id'),
        when(la['device_type'].like("%desktop%"), 'WEB')
            .when(la['device_type'].like("%mobile%"), 'MOBILE')
                .otherwise("revisar").alias("coupon_channel"),
        when(tick["bookmaker_id"].isin(88), concat(
            tick["bookmaker_id"], lit(0), s['id']))
        .otherwise(concat(p["partner"], s['id'])).alias("partner_event_id"),
        s['id'].alias("event_id"),
        when(rs['id'].isin(1, 2, 4, 7), 'Apuesta Resuelta').when(rs['id'].isin(5, 6), 'Apuesta No Saldada').otherwise('Apuesta Pendiente').alias('resolucion'),  
        m['code'].alias('market'),
        lit(None).cast(StringType()).alias('bet_offer_description'),
        bet['total_odd'].alias('row_odds'),
        (bet['potential_payment'] / bet['amount']).cast(DecimalType(10,4)).alias('combination_odds'),
        tick['total_amount'].cast(DecimalType(15,4)).alias('combination_stake'),
        when(bet["bonus_user_amount"].isNull(), bet["potential_payment"] / count("*").over(Window.partitionBy(bet['id'])))
            .otherwise((bet["potential_payment"] * (1 - (bet["bonus_user_amount"] / bet["amount"]))) / count("*").over(Window.partitionBy(bet['id']))).cast(DecimalType(14,2)).alias("row_amount"), # Premios Efectivo
        lit('1').alias("balance_type_id"),  # Efectivo 1
        tick['ticket_status_id'].alias('combination_status_id'),
        lit('3').alias("operation_type_id"), # Premio 3
        lit('9009').alias('room_id'),
        lit('999').alias('provider_id'),
        lit(None).cast(StringType()).alias('cupon_externo'),
        lit(None).cast(IntegerType()).alias('freebet_configuration_id'),
        lit(None).cast(StringType()).alias('freebet_configuration')
            ).distinct()
                                                
    result = Ap_Ef_AADD.unionByName(Pr_Ef_AADD)                              
    return result.withColumn("diccionario_aadd", 
            concat(
                format_string("%02d", col("bet_type_id").cast(IntegerType()))
                , format_string("%02d", col("balance_type_id").cast(IntegerType()))
                , format_string("%02d", col("combination_status_id").cast(IntegerType()))
                , format_string("%02d", col("operation_type_id").cast(IntegerType()))
                , format_string("%02d", col("live_betting_id").cast(IntegerType()))
            )) 

# COMMAND ----------

# MAGIC %md
# MAGIC ### AADD Bono

# COMMAND ----------

def Create_AADD_Bono_GT(bet, tick, usr, bs, la, rs, te, s, m, f, p):
    usr_filter = usr.filter((col("local_register") == '0') & (col("is_backoffice") == '0') & (col("is_services") == '0'))
                                 
    # Apuestas Bonos AADD  OK
    Ap_Bo_AADD = bet.filter(~col("result_status_id").isin(8, 9, 10, 11)).filter(col("bonus_user_amount").isNotNull()).alias("bet")\
        .join(tick, tick['id'] == bet['ticket_id'])\
            .join(usr_filter, tick["user_id"] == usr_filter["id"])\
                .join(bs.alias("bs"), bet["id"] == bs["bet_id"])\
                    .join(la, tick["login_activity_id"] == la["id"])\
                        .join(rs, bet['result_status_id'] == rs['id'])\
                            .join(te.filter(col('language_code') == 'es'), rs['name'] == te['translation_id'])\
                                .join(s, bs['selection_id'] == s['id'])\
                                    .join(m, s['market_id'] == m['id'])\
                                        .join(f, f['id'] == m['fixture_id'])\
                                            .join(p, tick["bookmaker_id"] == p['id'])\
                                                .select(
        date_trunc('second', from_utc_timestamp('bet.created_at', 'America/Buenos_Aires')).alias('created_date'),
        date_trunc('second', from_utc_timestamp('bet.settled_at', 'America/Buenos_Aires')).alias('resolution_date'),
        #when(bet.settled_at.isNotNull(), bet.settled_at).otherwise(bet.created_at).cast(DateType()).alias("fecha"),
        to_date(expr("bet.created_at - INTERVAL 3 HOURS")).alias("fecha"),
        concat(p["partner"], when(usr_filter['alira_id'].isNotNull(), usr_filter['alira_id'])
                                .when(usr_filter['bookmaker_id'].isin(88), concat(usr_filter['bookmaker_id'], format_string("%08d", usr_filter["id"])))
                                    .otherwise(usr_filter["id"])).cast(LongType()).alias("partner_player_id"),  ##### Migracion ####
        concat(p["partner"], tick['id'], bet['id']).cast(LongType()).alias('partner_coupon_row_id'),
        p["partner"].cast(LongType()).alias('partner_id'),
        tick['id'].cast(LongType()).alias('coupon_id'),
        bet['id'].alias('row_id'),
        count("*").over(Window.partitionBy(tick['id'])).alias("bet_size"),
        lit(None).cast(DoubleType()).alias('cant_ap_evento'),  
        lit(None).cast(IntegerType()).alias('evento_unico'),
        lit(None).cast(TimestampType()).alias('max_resolution_date_comb'),
        lit(None).cast(TimestampType()).alias('max_event_end_date'),
        lit(None).cast(TimestampType()).alias('estimated_end_date'),
        when(tick['ticket_type_id'].cast(IntegerType()) == 2, lit("3"))
            .when(tick['ticket_type_id'].cast(IntegerType()) == 3, lit("4"))
            .otherwise(tick['ticket_type_id'].cast(IntegerType())).alias("bet_type_id"),
        # tick['ticket_type_id'].cast(IntegerType()).alias("bet_type_id"),
        when((bet['created_at'].isNotNull()) & (f['start_at'].isNotNull()) & \
            (date_format(from_utc_timestamp(bet['created_at'], 'America/Buenos_Aires'), 'yyyy-MM-dd HH:mm:ss') \
                >= date_format(from_utc_timestamp(f['start_at'], 'America/Buenos_Aires'), 'yyyy-MM-dd HH:mm:ss')), \
                    lit(1)).otherwise(lit(0)).cast(IntegerType()).alias('live_betting_id'),
        when(la['device_type'].like("%desktop%"), 'WEB')
            .when(la['device_type'].like("%mobile%"), 'MOBILE')
                .otherwise("revisar").alias("coupon_channel"),
        when(tick["bookmaker_id"].isin(88), concat(
            tick["bookmaker_id"], lit(0), s['id']))
        .otherwise(concat(p["partner"], s['id'])).alias("partner_event_id"),
        bs['selection_id'].alias("event_id"),
        when(rs['id'].isin(1, 2, 4, 7), 'Apuesta Resuelta').when(rs['id'].isin(5, 6), 'Apuesta No Saldada').otherwise('Apuesta Pendiente').alias('resolucion'),  
        m['code'].alias('market'),
        lit(None).cast(StringType()).alias('bet_offer_description'),
        bet['total_odd'].alias('row_odds'),
        (bet['potential_payment'] / bet['amount']).cast(DecimalType(10,4)).alias('combination_odds'),
        tick['total_amount'].cast(DecimalType(15,4)).alias('combination_stake'),
        ((bet["bonus_user_amount"] / count("*").over(Window.partitionBy(tick['id'])))*-1).cast(DecimalType(14,2)).alias("row_amount"),
        lit('8').alias("balance_type_id"),  # Bono 8
        tick['ticket_status_id'].alias('combination_status_id'),
        lit('4').alias("operation_type_id"), # Apuesta 4
        lit('9009').alias('room_id'),
        lit('999').alias('provider_id'),
        lit(None).cast(StringType()).alias('cupon_externo'),
        lit(None).cast(IntegerType()).alias('freebet_configuration_id'),
        lit(None).cast(StringType()).alias('freebet_configuration')
            ).distinct()
                                                                                                
    # Premios Bonos AADD  OK
    Pr_Bono_AADD = bet.filter((col("result_status_id").isin(1, 2, 3, 7)) & (col("bonus_user_amount").isNotNull()) & (col("bonus_user_amount") > 0)).alias("bet")\
        .join(tick, tick['id'] == bet['ticket_id'])\
            .join(usr_filter, tick["user_id"] == usr_filter["id"])\
                .join(bs.alias("bs"), bet["id"] == bs["bet_id"])\
                    .join(la, tick["login_activity_id"] == la["id"])\
                        .join(rs, bet['result_status_id'] == rs['id'])\
                            .join(te.filter(col('language_code') == 'es'), rs['name'] == te['translation_id'])\
                                .join(s, bs['selection_id'] == s['id'])\
                                    .join(m, s['market_id'] == m['id'])\
                                        .join(f, f['id'] == m['fixture_id'])\
                                            .join(p, tick["bookmaker_id"] == p['id'])\
                                                .select(
        date_trunc('second', from_utc_timestamp('bet.created_at', 'America/Buenos_Aires')).alias('created_date'),
        date_trunc('second', from_utc_timestamp('bet.settled_at', 'America/Buenos_Aires')).alias('resolution_date'),
        #when(bet.settled_at.isNotNull(), bet.settled_at).otherwise(bet.created_at).cast(DateType()).alias("fecha"),
        to_date(expr("bet.settled_at - INTERVAL 3 HOURS")).alias("fecha"),

        concat(p["partner"], when(usr_filter['alira_id'].isNotNull(), usr_filter['alira_id'])
                                .when(usr_filter['bookmaker_id'].isin(88), concat(usr_filter['bookmaker_id'], format_string("%08d", usr_filter["id"])))
                                    .otherwise(usr_filter["id"])).cast(LongType()).alias("partner_player_id"),  ##### Migracion ####

        concat(p["partner"], tick['id'], bet['id']).cast(LongType()).alias('partner_coupon_row_id'),
        concat(p["partner"]).cast(LongType()).alias('partner_id'),
        tick['id'].cast(LongType()).alias('coupon_id'),
        bet['id'].alias('row_id'),
        count("*").over(Window.partitionBy(tick['id'])).alias("bet_size"),
        lit(None).cast(DoubleType()).alias('cant_ap_evento'),  
        lit(None).cast(IntegerType()).alias('evento_unico'),
        lit(None).cast(TimestampType()).alias('max_resolution_date_comb'),
        lit(None).cast(TimestampType()).alias('max_event_end_date'),
        lit(None).cast(TimestampType()).alias('estimated_end_date'),
        when(tick['ticket_type_id'].cast(IntegerType()) == 2, lit("3"))
            .when(tick['ticket_type_id'].cast(IntegerType()) == 3, lit("4"))
            .otherwise(tick['ticket_type_id'].cast(IntegerType())).alias("bet_type_id"),
        # tick['ticket_type_id'].cast(IntegerType()).alias("bet_type_id"),
        when((bet['created_at'].isNotNull()) & (f['start_at'].isNotNull()) & \
            (date_format(from_utc_timestamp(bet['created_at'], 'America/Buenos_Aires'), 'yyyy-MM-dd HH:mm:ss') \
                >= date_format(from_utc_timestamp(f['start_at'], 'America/Buenos_Aires'), 'yyyy-MM-dd HH:mm:ss')), \
                    lit(1)).otherwise(lit(0)).cast(IntegerType()).alias('live_betting_id'),
        when(la['device_type'].like("%desktop%"), 'WEB')
            .when(la['device_type'].like("%mobile%"), 'MOBILE')
                .otherwise("revisar").alias("coupon_channel"),
        when(tick["bookmaker_id"].isin(88), concat(
            tick["bookmaker_id"], lit(0), s['id']))
        .otherwise(concat(p["partner"], s['id'])).alias("partner_event_id"),
        bs['selection_id'].alias("event_id"),
        when(rs['id'].isin(1, 2, 4, 7), 'Apuesta Resuelta').when(rs['id'].isin(5, 6), 'Apuesta No Saldada').otherwise('Apuesta Pendiente').alias('resolucion'),  
        m['code'].alias('market'),
        lit(None).cast(StringType()).alias('bet_offer_description'),
        bet['total_odd'].alias('row_odds'),
        (bet['potential_payment'] / bet['amount']).cast(DecimalType(10,4)).alias('combination_odds'),
        tick['total_amount'].cast(DecimalType(15,2)).alias('combination_stake'),
        when(bet['bonus_user_amount'] == bet["amount"], bet["potential_payment"])\
            .otherwise(bet["potential_payment"] * (bet["bonus_user_amount"] / bet["amount"])).cast(DecimalType(14,2)).alias("row_amount"), # Premios Bonos
        lit('8').alias("balance_type_id"),  # Bonos 8
        tick['ticket_status_id'].alias('combination_status_id'),
        lit('3').alias("operation_type_id"), # Premio 3
        lit('9009').alias('room_id'),
        lit('999').alias('provider_id'),
        lit(None).cast(StringType()).alias('cupon_externo'),
        lit(None).cast(IntegerType()).alias('freebet_configuration_id'),
        lit(None).cast(StringType()).alias('freebet_configuration')
            ).distinct()
                            
    result = Ap_Bo_AADD.unionByName(Pr_Bono_AADD)                                   
    return result.withColumn("diccionario_aadd", 
            concat(
                format_string("%02d", col("bet_type_id").cast(IntegerType()))
                , format_string("%02d", col("balance_type_id").cast(IntegerType()))
                , format_string("%02d", col("combination_status_id").cast(IntegerType()))
                , format_string("%02d", col("operation_type_id").cast(IntegerType()))
                , format_string("%02d", col("live_betting_id").cast(IntegerType()))
            )) 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ejecución del DataFrame Auxiliar Dim Room por Base de Datos: DWBPLAY_CABA, DWBPLAY_CORDOBA, PRN y JUJ
# MAGIC <p> Se ejecuta la función Create_Fact_Coupon_Row para obtener el dataframe de Create Fact Coupon Row de cada base de datos: DWBPLAY_CABA y DWBPLAY_CORDOBA

# COMMAND ----------

# Apuestas Deportivas Alira CABA
df_AADD_CABA = Create_Apuestas_Deportivas(df_cr2, df_c2, df_co2, df_p2, df_o2, df_m2, df_e2, df_bt2, df_pfb2, df_fbc2)
# Apuestas Deportivas Alira CBA
df_AADD_CBA  = Create_Apuestas_Deportivas(df_cr3, df_c3, df_co3, df_p3, df_o3, df_m3, df_e3, df_bt3, df_pfb3, df_fbc3)

if bl_Carga_GT:
    # Apuestas Deportivas GT PRN & JUJ
    df_AD_Bono_GT = Create_AADD_Bono_GT(df_bet, df_tick, df_usr, df_bs, df_la, df_rs, df_te, df_s, df_m, df_f, df_partner_Migra)
    df_AD_Eftvo_GT = Create_AADD_Eftvo_GT(df_bet, df_tick, df_usr, df_bs, df_la, df_rs, df_te, df_s, df_m, df_f, df_partner_Migra)

    df_AADD_GT = df_AD_Bono_GT.unionByName(df_AD_Eftvo_GT)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Union de DF CBA, CABA, PRN y JUJ
# MAGIC <p> Se realiza la unificacion de las ejecuciones de las bases de datos DWBPLAY_CABA, DWBPLAY_CORDOBA, PRN y JUJ

# COMMAND ----------

if bl_Carga_GT:
    # Union Multiboldt & CBA & JUJ & PRN & MZA
    df_AADD = df_AADD_CABA.unionByName(df_AADD_CBA).unionByName(df_AADD_GT)
else:
    # Union Multiboldt & CBA
    df_AADD = df_AADD_CABA.unionByName(df_AADD_CBA)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Upsert de Tabla Delta "Apuestas Deportiva"
# MAGIC <p> Obtenemos la tabla original para actualizar la Delta Table 'Apuestas Deportiva' con el dataframe df_AADD recién creado.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Obtención de la Última Tabla Generada para Actualización
# MAGIC <p> Si ya existe la Delta Table de Apuestas y Premios creada se obtendra la tabla y se actualizará con el DataFrame generado en esta ejecución.
# MAGIC <p> Si no existiera, se definirá la limitación las particiones en 8, y se generará un nuevo Delta Table de "Apuestas y Premios".

# COMMAND ----------

# Filtra hasta las  23:59:59 del dia anterior a la ejecucion
def filter_last_day(df): 
    yesterday_end = concat(date_sub(current_timestamp(), 1).cast("string"),lit(" 23:59:59")).cast(TimestampType())
    result = df.filter((col("created_date") <= yesterday_end))
    return result

# COMMAND ----------

print(delta_table_FCR)
print(exist)
print(PROD)

# COMMAND ----------

from pyspark.sql import SparkSession

# Create a Spark session with the desired configurations
spark = SparkSession.builder \
    .config("spark.executor.memory", "8g") \
    .config("spark.executor.cores", "4") \
    .getOrCreate()

# COMMAND ----------

df_AADD_f = filter_last_day(df_AADD)

# COMMAND ----------

# ID unico partner_coupon_row_id, event_id, operation_type_id, balance_type_id, row_amount
df_AADD_f.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(delta_table_FCR)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Incremental Auxiliar

# COMMAND ----------

# # Filtra los ultimos 5 dias para actualizar
# def filter_last_5_days(df): 
#     last_5_days = concat(date_sub(current_timestamp(), 5).cast("string"),lit(" 23:59:59")).cast("timestamp")
#     result = df.filter(col("created_date") >= last_5_days)
#     return result

#     # Obtengo el df de la tabla Apuestas Y Premios ya creada
# def get_dif(nuevo):
#     origen = spark.read.table(delta_table_FCR)
#     # Obtengo las filas nuevas y actualizadas 
#     res = nuevo.exceptAll(origen)
#     return res

# # Se obtiene las actualizaciones de los ultimos 15 días
# df_FCR_dif = filter_last_5_days(df_FCR)
# # Se Obtiene las filas por actualizar
# df_FCR_dif = get_dif(df_FCR_dif)
# # Se elimina hasta el ultimo día de ejecucion
# df_FCR_dif = filter_last_day(df_FCR_dif)
# print("Se creo df_FCR_dif")

# # Actualizar el DataFrame existente
# dt_origen = DeltaTable.forName(spark, delta_table_FCR)   
# # Upsert del DataFrame de Apuestas y Premios
# dt_origen.alias("dest")\
#     .merge(source=df_FCR_dif.alias("update"), condition="dest.partner_coupon_row_id = update.partner_coupon_row_id")\
#         .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
# print("Se actualizo exitosamente la base de Apuestas Deportivas")

# COMMAND ----------

# MAGIC %md
# MAGIC # Diccionario AADD

# COMMAND ----------

from pyspark.sql.functions import coalesce, format_string, substring

# COMMAND ----------

#   Variable PROD/DEV & Carga de Diccionario AADD
bl_Cargar_Diccionario_AADD = True

# COMMAND ----------

if bl_Cargar_Diccionario_AADD:
    # Get CABA
  df_c2 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_caba/APUESTAS/coupon')
  df_btn2 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_caba/OGP/balance_t_name')
  df_cosn2 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_caba/APUESTAS/combination_status_name')
  df_otn2 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_caba/OGP/operation_t_name')

    # Get CORDOBA
  df_c3 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_cordoba/APUESTAS/coupon')
  df_btn3 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_cordoba/OGP/balance_t_name')
  df_cosn3 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_cordoba/APUESTAS/combination_status_name') 
  df_otn3 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_cordoba/OGP/operation_t_name')

# COMMAND ----------

def Create_Diccionario_AADD(c, btn, cosn, otn):
    # Creación de la tabla hardcodeada
    live_betting_data = [(1, 'Live'), (0, 'Prematch')]
    
    # Creando un DataFrame de la tabla hardcodeada
    lb = spark.createDataFrame(live_betting_data, ['live_betting_id', 'live_betting'])
    
    diccionario = c.select(col('bet_type').alias('bet_type_id'), col('type').alias('bet_type')).distinct()\
        .crossJoin(btn.filter(col('language') == 2).select(col('type').alias('balance_type_id'), col('name').alias('balance_type')))\
            .crossJoin(cosn.filter(col('language') == 2).select(col('STATUS').alias('combination_status_id'), col('NAME').alias('combination_status')))\
                .crossJoin(otn.filter(col('language') == 2).filter(col('type').isin(3,4)).select(col('type').alias('operation_type_id'), col('name').alias('operation_type')))\
                    .crossJoin(lb)\
                        .withColumn('diccionario_aadd', 
                            concat(
                                format_string("%02d", col("bet_type_id").cast(IntegerType())), 
                                format_string("%02d", col("balance_type_id").cast(IntegerType())),
                                format_string("%02d", col("combination_status_id").cast(IntegerType())),
                                format_string("%02d", col("operation_type_id").cast(IntegerType())),
                                format_string("%02d", col("live_betting_id").cast(IntegerType()))
                            ))
    return diccionario

# COMMAND ----------

def Create_Diccionario_bplaybet(df):
    result = df.select("bet_type_id", "balance_type_id", "combination_status_id", "operation_type_id", "live_betting_id").distinct()

# COMMAND ----------

def Create_Diccionario_bplaybet(df_adgt):

    # Busco casos únicos en tabla apuestas deportivas bplaybet
    aadd_bplaybet = df_adgt.select(
                                    substring(col("diccionario_aadd"), 1, 2).alias('bet_type_id'),
                                    substring(col("diccionario_aadd"), 3, 2).alias('balance_type_id'),
                                    substring(col("diccionario_aadd"), 5, 2).alias('combination_status_id'),
                                    substring(col("diccionario_aadd"), 7, 2).alias('operation_type_id'),
                                    substring(col("diccionario_aadd"), 9, 2).alias('live_betting_id')
                                ).distinct()
    
    # Creación de la tabla hardcodeada
    live_betting_data = [(1, 'Live'), (0, 'Prematch')]

    #   44	Bono de FreeBets
    
    # Creando un DataFrame de la tabla hardcodeada
    lb = spark.createDataFrame(live_betting_data, ['live_betting_id', 'live_betting'])
    
    diccionario =   aadd_bplaybet.select(
                                        # when(col('bet_type_id').cast(IntegerType()) == 2, lit("3"))
                                        #     .when(col('bet_type_id').cast(IntegerType()) == 3, lit("4"))
                                        #     .otherwise(col('bet_type_id').cast(IntegerType()))
                                        #     .cast(IntegerType()).alias("bet_type_id"),
                                        col('bet_type_id').cast(IntegerType()).alias("bet_type_id"),
                                        when(col('bet_type_id').cast(IntegerType()) == 1, lit("SIMPLE"))
                                            .when(col('bet_type_id').cast(IntegerType()) == 3, lit("COMBINADA"))
                                            .when(col('bet_type_id').cast(IntegerType()) == 4, lit("SISTEMA"))
                                            .otherwise(lit("REVISAR")).alias("bet_type"),
                                        col('balance_type_id').cast(IntegerType()).alias("balance_type_id"),
                                        when(col('balance_type_id').cast(IntegerType()) == 1, lit("Efectivo"))
                                            .when(col('balance_type_id').cast(IntegerType()) == 8, lit("Bono universal"))
                                            .when(col('balance_type_id').cast(IntegerType()) == 44, lit("Bono de FreeBets"))
                                            .otherwise(lit("REVISAR")).alias("balance_type"),
                                        when(col('combination_status_id').cast(IntegerType()) == 3, lit("1"))
                                            .when(col('combination_status_id').cast(IntegerType()) == 2, lit("3"))
                                            .when(col('combination_status_id').cast(IntegerType()) == 5, lit("4"))
                                            .when(col('combination_status_id').cast(IntegerType()) == 4, lit("5"))
                                            .when(col('combination_status_id').cast(IntegerType()) == 1, lit("7"))
                                            .otherwise(col('combination_status_id').cast(IntegerType()))
                                            .cast(IntegerType()).alias("combination_status_id"),
                                        when(col('combination_status_id').cast(IntegerType()) == 3, 
                                                lit("Aprobada"))
                                            .when(col('combination_status_id').cast(IntegerType()) == 2, 
                                                lit("Denegada"))
                                            .when(col('combination_status_id').cast(IntegerType()) == 5, 
                                                lit("Ganada"))
                                            .when(col('combination_status_id').cast(IntegerType()) == 4, 
                                                lit("Perdida"))
                                            .when(col('combination_status_id').cast(IntegerType()) == 1, 
                                                lit("Cancelada"))
                                            .otherwise(lit("REVISAR")).alias("combination_status"),
                                        col('operation_type_id').cast(IntegerType()).alias("operation_type_id"),
                                        when(col('operation_type_id').cast(IntegerType()) == 3, lit("Premio"))
                                            .when(col('operation_type_id').cast(IntegerType()) == 4, lit("Apuesta"))
                                            .otherwise(lit("REVISAR")).alias("operation_type"),
                                        col('live_betting_id').cast(IntegerType()).alias("live_betting_id"),
                                        when(col('live_betting_id').cast(IntegerType()) == 0, lit("Prematch"))
                                        .when(col('live_betting_id').cast(IntegerType()) == 1, lit("Live"))
                                            .otherwise(lit("REVISAR")).alias("live_betting")
                                        ) \
                                    .withColumn('diccionario_aadd', 
                                        concat(
                                            format_string("%02d", col("bet_type_id").cast(IntegerType())), 
                                            format_string("%02d", col("balance_type_id").cast(IntegerType())),
                                            format_string("%02d", col("combination_status_id")
                                                          .cast(IntegerType())),
                                            format_string("%02d", col("operation_type_id").cast(IntegerType())),
                                            format_string("%02d", col("live_betting_id").cast(IntegerType()))
                                        )
                                    ).distinct()

    return diccionario


# COMMAND ----------

from pyspark.sql.functions import col, lit, concat, coalesce

def Create_merge(df_aadd_t, df_aadd_bb):

    # Realizar un FULL OUTER JOIN en el ID
    merged_df = df_aadd_t.alias("dft").join(
        df_aadd_bb.alias("dfbb"), 
        on="diccionario_aadd", 
        how="outer"
    )

    # Mantener los valores originales de df1 para el resto de las columnas
    final_df = merged_df.select(
        col("diccionario_aadd"),
        coalesce(col("dft.bet_type_id"), col("dfbb.bet_type_id")).cast(IntegerType()).alias("bet_type_id"),
        coalesce(col("dft.bet_type"), col("dfbb.bet_type")).alias("bet_type"),
        coalesce(col("dft.balance_type_id"), col("dfbb.balance_type_id"))
            .cast(IntegerType()).alias("balance_type_id"),
        coalesce(col("dft.balance_type"), col("dfbb.balance_type")).alias("balance_type"),
        coalesce(col("dft.combination_status_id"), col("dfbb.combination_status_id"))
            .cast(IntegerType()).alias("combination_status_id"),
        coalesce(col("dft.combination_status"), col("dfbb.combination_status")).alias("combination_status"),
        coalesce(col("dft.operation_type_id"), col("dfbb.operation_type_id"))
            .cast(IntegerType()).alias("operation_type_id"),
        coalesce(col("dft.operation_type"), col("dfbb.operation_type")).alias("operation_type"),
        coalesce(col("dft.live_betting_id"), col("dfbb.live_betting_id"))
            .cast(IntegerType()).alias("live_betting_id"),
        coalesce(col("dft.live_betting"), col("dfbb.live_betting")).alias("live_betting")
    )

    return final_df

# COMMAND ----------

if bl_Cargar_Diccionario_AADD:
    # Diccionario AADD CABA
    df_Diccionario_AADD2 = Create_Diccionario_AADD(df_c2, df_btn2, df_cosn2, df_otn2)
    # Diccionario AADD CBA
    df_Diccionario_AADD3 = Create_Diccionario_AADD(df_c3, df_btn3, df_cosn3, df_otn3)

    # Unión Diccionario AADD Tecnalis
    df_Diccionario_AADD = (df_Diccionario_AADD2.unionByName(df_Diccionario_AADD3)).distinct()

    # Diccionario AADD bplaybet
    df_Diccionario_AADD_GT = Create_Diccionario_bplaybet(df_AADD_GT)

    # Merge Diccionarios AADD Tecnalis y bplaybet
    df_Diccionario_AADD = Create_merge(df_Diccionario_AADD, df_Diccionario_AADD_GT)

# COMMAND ----------

# Path de Delta Table
#   Path PRD del Delta Table TiposDeCambios en PRD
delta_table_DicAADD_prd = "entprod_mdn.bplay.diccionario_AADD"

#   Path DEV del Delta Table TiposDeCambios en DEV
delta_table_DicAADD_dev = "entprod_mdn.default.diccionario_AADD"

#   Switch PROD/DEV
if PROD:
    delta_table_DicAADD = delta_table_DicAADD_prd
else:
    delta_table_DicAADD = delta_table_DicAADD_dev

if bl_Cargar_Diccionario_AADD:
    # Manejar el caso en que la tabla no exista
    try:
        # Intentar leer la tabla Delta
        df_origen = spark.read.table(delta_table_DicAADD)
        count = df_origen.select('diccionario_aadd').exceptAll(df_Diccionario_AADD.select('diccionario_aadd')).count()

        # Validar la tabla Fact Coupon Row
        if count > 0:
            exist = True
        else:
            exist = False
    except Exception as e:
        print(f"La tabla {delta_table_DicAADD} no existe o no se puede leer: {e}")
        exist = False
    
    print(exist)

# Comprobaciones
print(delta_table_DicAADD)
print(bl_Cargar_Diccionario_AADD)

# COMMAND ----------

if bl_Cargar_Diccionario_AADD:
    if not exist:
        print(f'se cargará la tabla {delta_table_DicAADD}')
        df_Diccionario_AADD.write \
                .mode("overwrite") \
                    .option("overwriteSchema", "true") \
                        .saveAsTable(delta_table_DicAADD)
    else:
        print(f'La tabla {delta_table_DicAADD} ya existe, se sobreescribirá')
        df_Diccionario_AADD.write \
                .mode("overwrite") \
                    .option("overwriteSchema", "true") \
                        .saveAsTable(delta_table_DicAADD)
else:
    print(f'No se cargará la tabla bl_Cargar_Diccionario_AADD = {bl_Cargar_Diccionario_AADD}')