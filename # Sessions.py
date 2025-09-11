# Databricks notebook source
# MAGIC %md
# MAGIC # Sessions_MDN-bplay_PySpark.ipynb
# MAGIC <h> by PySpark
# MAGIC #### Estado: **OK**
# MAGIC <p> Source: Se realizará la lectura de archivos en formato parquet para obtener los dataframes necesarios para generar la tabla 'Sessions', y cargarla en formato Delta Table para su posterior consulta.
# MAGIC <p> Se tomará la información desde el STG1 de Datalake para generar un consumible de STG2 en formato Delta Table.
# MAGIC <p> Inputs:
# MAGIC   <ol>
# MAGIC     <li>Player_Sessions_History</li>
# MAGIC     <li>Player</li>
# MAGIC   </ol>
# MAGIC <p> Output: Delta Table actualizado de "Sessions" y su carga en el Warehouse de Databricks.

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

from pyspark.sql.functions import col, min, max, sum, lit, expr, count, countDistinct, avg, concat, when, datediff, current_date, current_timestamp, to_timestamp, to_date, left, date_sub, format_string
from pyspark.sql.types import DecimalType
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %md
# MAGIC ### Nomenclatura de Fuentes y Auxiliares
# MAGIC <ul>
# MAGIC   <li>df = dataframe</li>
# MAGIC   <li>psh = player_session_history</li>
# MAGIC   <li>p = player</li>
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
# MAGIC   <li>Se definen los paths de los archivos parquet "Player" y "Player_Session_History" para cada Base de Datos DWBPLAY_PY, DWBPLAY_SF, DWBPLAY_CABA y DWBPLAY_CORDOBA.</li>
# MAGIC   <li> Luego se leen los archivos parquet para crear los dataframes en formato PySpark.</li>
# MAGIC </ol>

# COMMAND ----------

  # Get DF Player_Session_history
df_psh0 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_py/OGP/player_session_history')
df_psh1 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_sf/OGP/player_session_history')
df_psh2 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_caba/OGP/player_session_history')
df_psh3 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_cordoba/OGP/player_session_history')
  # Get DF Player
df_p0 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_py/OGP/player')
df_p1 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_sf/OGP/player')
df_p2 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_caba/OGP/player')
df_p3 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_cordoba/OGP/player')

# COMMAND ----------

try:
    # BPLAYBET_ARG
    df_log_actGT = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/bplaybet_arg/dbo/login_activities')
    df_usrGT = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/bplaybet_arg/dbo/users')

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
# MAGIC ### Switch PROD/DEV

# COMMAND ----------

#   Variable PROD/DEV
PROD = dbutils.widgets.get("run_job")
print(f"El parametro de PROD es: {PROD}")

PROD = PROD == "True"
print(type(PROD))

# COMMAND ----------

#Path PRD del Delta Table Sessions
delta_table_path_prd = 'entprod_mdn.bplay.sesion'

#Path DEV del Delta Table Sessions
delta_table_path_dev = 'entprod_mdn.default.sesion'

# #Variable PROD/DEV
# PROD = True

# Switch PROD/DEV
if PROD:
    delta_table_path = delta_table_path_prd
else:
    delta_table_path = delta_table_path_dev

# Manejar el caso en que la tabla no exista
try:
    # Intentar leer la tabla Delta
    df_origen = spark.read.table(delta_table_path)
    count = df_origen.select("partner_player_id").count()

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
# MAGIC ## 2. DF Auxiliar Dim Room

# COMMAND ----------

# MAGIC %md
# MAGIC ### Definición de la Función Create_Sessions
# MAGIC <p> Esta función genera un dataframe con la información de las conexiones de los players a la aplicacion BPLAY.

# COMMAND ----------

# MAGIC %md
# MAGIC ### ALIRA

# COMMAND ----------

from pyspark.sql.functions import count, countDistinct
def Create_Sessions(p, psh):
    yesterday_end = concat(date_sub(current_timestamp(), 1).cast("string"),lit(" 23:59:59")).cast("timestamp")
    p = p.filter((col("status") != 1) & (col("created_date") <= yesterday_end)) # Esta linea quita los empleados y los que no se hayan logueado en el día anterior
    hs = psh.groupBy(col("id").cast(DecimalType(10,0)).alias("player_id")) \
    .agg(
        min(col("date")).alias("primera_conexion"),
        max(col("date")).alias("ultima_conexion"),
        countDistinct(to_date(col("date"))).alias("cant_dias"), # Cantidad de Días que se conecto al menos una vez
        count("*").alias("cant_conexiones"),
        sum(col("time")).alias("minutos_sesion"),
        avg(col("time")).alias("minutos_sesion_promedio")
    )

    sessions = p.join(hs, p["id"] == hs["player_id"], 'left').select(
        concat(p["partner"], p["id"].cast(DecimalType(10,0))).alias("partner_player_id"),
        when(hs["primera_conexion"].isNull(), p["created_date"]).otherwise(hs["primera_conexion"]).alias("primera_conexion"),
        when(hs["cant_dias"].isNull(), 1).otherwise(hs["cant_dias"]).alias("cant_dias"),
        when(hs["cant_conexiones"].isNull(), 1).otherwise(hs["cant_conexiones"]).alias("cant_conexiones"),
        when(hs["ultima_conexion"].isNull(), p["created_date"]).otherwise(hs["ultima_conexion"]).alias("ultima_conexion"),
        when(hs["ultima_conexion"].isNull(), datediff(current_date(), p["created_date"])).otherwise(datediff(current_date(), hs["ultima_conexion"])).alias("dias_ultima_conexion"),
        datediff(current_date(), p["created_date"]).alias("dias_antiguedad")
    )
    return sessions

# COMMAND ----------

# MAGIC %md
# MAGIC ### GT

# COMMAND ----------

def Create_Sessions_GT(usr, act, p):
    yesterday_end = concat(date_sub(current_timestamp(), 1).cast("string"),lit(" 23:59:59")).cast("timestamp")
    usr = usr.filter(col('created_at') <= yesterday_end)
    # Get Conexiones
    act_select = act.select(col("user_id").alias("player_id"),
                            expr("left(created_at, 26)").alias("created_at"),
                            expr("left(updated_at, 26)").alias("updated_at")
                            )
    # Get Users
    usr_filter = usr.filter((col("local_register") == 0) & (col("is_backoffice") == 0) & (col("is_services") == 0))
    #usr_filter = usr

    # Sessions
    conexiones = act_select.join(usr_filter, (act_select["player_id"] == usr_filter["id"]))\
        .join(p, usr_filter["bookmaker_id"] == p['id'])\
            .select(
        # concat(usr_filter["bookmaker_id"], lit(0), usr_filter["id"]).alias("partner_player_id"),
        p["partner"].alias("partner_id"),
        concat(p["partner"], when(usr_filter['alira_id'].isNotNull(), usr_filter['alira_id'])
            .when(usr_filter['bookmaker_id'].isin(88), concat(usr_filter['bookmaker_id'], format_string("%08d", usr_filter["id"])))
                .otherwise(usr_filter["id"])).alias("partner_player_id"), ##### Migracion ####
        act_select["created_at"].alias("act_created_at"),
        act_select["updated_at"].alias("act_updated_at"),
        usr_filter["created_at"].alias("usr_created_at")
    )

    session = conexiones.groupBy(
        col("partner_id"),
        col("partner_player_id"),
        col("usr_created_at")
            ).agg(
                min(col('act_created_at')).alias("primera_conexion"),
                count(to_date(col('act_created_at'))).alias("cant_dias"),
                count("*").alias("cant_conexiones"),
                max(col('act_updated_at')).alias("ultima_conexion"),
                datediff(current_date(), max(col('act_updated_at'))).alias("dias_ultima_conexion"),
                datediff(current_date(), col('usr_created_at')).alias("dias_antiguedad"))\
                    .select(
        #"partner_id",
        "partner_player_id",
        col("primera_conexion").cast("timestamp").alias("primera_conexion"),
        when(col("cant_dias").isNull(), 1).otherwise(col("cant_dias")).alias("cant_dias"),
        when(col("cant_conexiones").isNull(), 1).otherwise(col("cant_conexiones")).alias("cant_conexiones"),
        col("ultima_conexion").cast("timestamp").alias("ultima_conexion"),
        "dias_ultima_conexion",
        "dias_antiguedad")
    return session

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ejecución del DataFrame Sessions por Base de Datos: DWBPLAY_PY, DWBPLAY_SF, DWBPLAY_CABA Y DWBPLAY_CORDOBA
# MAGIC <p> Se ejecuta la función Create_Sessions para las bases de datos DWBPLAY_PY, DWBPLAY_SF, DWBPLAY_CABA y DWBPLAY_CORDOBA.

# COMMAND ----------

# DWBPLAY_PY
df_Sessions0 = Create_Sessions(df_p0, df_psh0)
# DWBPLAY_SF
df_Sessions1 = Create_Sessions(df_p1, df_psh1)
# DWBPLAY_CABA
df_Sessions2 = Create_Sessions(df_p2, df_psh2)
# DWBPLAY_CORDOBA
df_Sessions3 = Create_Sessions(df_p3, df_psh3)

if bl_Carga_GT:
    # BPLAYBET_ARG
    df_SessionsGT = Create_Sessions_GT(df_usrGT, df_log_actGT, df_partner_Migra)

# COMMAND ----------

# MAGIC %md
# MAGIC ##3. Union de DF Sessions
# MAGIC <p> Se realiza la unificacion de las ejecuciones de las bases de datos DWBPLAY_PY, DWBPLAY_SF, DWBPLAY_CABA y DWBPLAY_CORDOBA.

# COMMAND ----------

if bl_Carga_GT:
    df_Sessions = df_Sessions0.unionByName(df_Sessions1).unionByName(df_Sessions2).unionByName(df_Sessions3).unionByName(df_SessionsGT)
    #df_Sessions = df_Sessions0.unionByName(df_Sessions1).unionByName(df_Sessions2).unionByName(df_SessionsGT)
    print("Se cargará la base completa")
else: 
    df_Sessions = df_Sessions0.unionByName(df_Sessions1).unionByName(df_Sessions2).unionByName(df_Sessions3)
    print("Se cargará la base sin GT")

# COMMAND ----------

df_Sessions = df_Sessions.groupBy(
                                "partner_player_id"
                                ).agg(
                                    min(col('primera_conexion')).alias("primera_conexion"),
                                    sum(col("cant_dias")).alias("cant_dias"),
                                    sum(col("cant_conexiones")).alias("cant_conexiones"),
                                    max(col('ultima_conexion')).alias("ultima_conexion"),
                                    max(col('dias_ultima_conexion')).alias('dias_ultima_conexion'),
                                    max(col('dias_antiguedad')).alias('dias_antiguedad')                                        
                                )

# COMMAND ----------

print(f"Se cargará en: {delta_table_path}")
print(f"Se cargará la carga de la DB de GT: {bl_Carga_GT}")
print(exist)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Upsert de Tabla Delta "Sessions"
# MAGIC <p> Obtenemos la tabla original para actualizar la Delta Table 'Sessions' con el dataframe df_Sessions recién creado.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Obtener actualización de la tabla Sessions

# COMMAND ----------

# def get_dif(nuevo):
#     # Obtengo el df de la tabla Sessions ya creada
#     dt_origen = DeltaTable.forPath(spark, delta_table_path)

#     # Transformat a Dataframe y Seleccionar los campos por comparar
#     origen = dt_origen.toDF().select('partner_player_id', 'primera_conexion', 'cant_dias', 'cant_conexiones', 'ultima_conexion', 'dias_ultima_conexion', 'dias_antiguedad')

#     # Obtengo las filas nuevas y actualizadas 
#     dif = nuevo.exceptAll(origen)
    
#     # Agrega la columna de actualización
#     res = dif.withColumn("fecha_actualizacion", current_timestamp()).withColumn("fecha_creacion", to_timestamp(col("primera_conexion")))

#     return res



# COMMAND ----------

# MAGIC %md
# MAGIC ### Obtención de la Última Tabla Generada para Actualización
# MAGIC <p> Si ya existe la Delta Table de Apuestas y Premios creada se obtendra la tabla y se actualizará con el DataFrame generado en esta ejecución.
# MAGIC <p> Si no existiera, se definirá la limitación las particiones en 8, y se generará un nuevo Delta Table de "Sessions".

# COMMAND ----------

# # Verificar si la ruta contiene una tabla Delta
# if exist:
#     # Se Obtiene las filas por actualizar
#     df_Sessions_act = get_dif(df_Sessions)
#     #df_Sessions_act = df_Sessions
#     # print("Se creo df_Sessions_act")
#     # Actualizar el DataFrame existente
#     dt_origin = DeltaTable.forPath(spark, delta_table_path)
#     # Upsert del DataFrame de Apuestas y Premios
#     dt_origin.alias("dest") \
#         .merge(df_Sessions_act.alias("update"), "dest.partner_player_id = update.partner_player_id") \
#             .whenMatchedUpdateAll()\
#             .whenNotMatchedInsertAll()\
#             .execute()
#     # print("Se actualizó la base de Sessions")
# else:
#     # Agregado de las columnas identificatorias para el upgrade de la base
#     df_Sessions = df_Sessions \
#         .withColumn("fecha_creacion", to_timestamp(col("primera_conexion"))) \
#             .withColumn("fecha_actualizacion", current_timestamp()) \
#                 .coalesce(8)  # Limitar a 8 particiones el DF "df_Sessions"
#     # Guardar el DataFrame en formato Delta con overwrite habilitado
#     df_Sessions.write.format("delta")\
#         .save(delta_table_path)
#     # print("Se creó exitosamente la base de Sessions")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Carga del DataFrame completo de Sessions
# MAGIC <p> Se carga la tabla actualizada en el Warehouse de Databricks para su posterior utilización o consumo.

# COMMAND ----------

print(exist)
print(delta_table_path)

# COMMAND ----------

 # Agregado de las columnas identificatorias para el upgrade de la base
df_Sessions = df_Sessions \
        .withColumn("fecha_creacion", to_timestamp(col("primera_conexion"))) \
            .withColumn("fecha_actualizacion", current_timestamp()) \
                .coalesce(8)  # Limitar a 8 particiones el DF "df_Sessions"
df_Sessions.write \
    .mode("overwrite")\
    .option("overwriteSchema", "true")\
        .saveAsTable(delta_table_path)