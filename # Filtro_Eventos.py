# Databricks notebook source
# MAGIC %md
# MAGIC # 5.Eventos_MDN-bplay_PySpark.ipynb
# MAGIC <h> by PySpark
# MAGIC #### Estado: **OK**
# MAGIC <p> Source: Se realizará la lectura de archivos en formato parquet para obtener los dataframes necesarios para generar la tabla 'Eventos', y cargarla en formato Delta Table para su posterior consulta.
# MAGIC <p> Se tomará la información desde el STG1 de Datalake para generar un consumible de STG2 en formato Delta Table.
# MAGIC <p> Inputs:
# MAGIC   <ol>
# MAGIC     <li>Event</li>
# MAGIC   </ol>
# MAGIC <p> Output: Delta Table actualizado de "Eventos" y su carga en el Warehouse de Databricks.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Start Up
# MAGIC <p> Etapa de preparación para su ejecución

# COMMAND ----------

# MAGIC %md
# MAGIC ### Declaración de librerias utilizadas
# MAGIC <p> pyspark.sql.functions = Importa funciones de PySpark para manipular y trabajar con columnas y datos en los DataFrames.

# COMMAND ----------

from pyspark.sql.functions import col, concat, cast, date_format, from_utc_timestamp, lit, count, when, format_string, current_timestamp, to_timestamp
from pyspark.sql.types import TimestampType, DateType, LongType
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %md
# MAGIC ### Nomenclatura de Fuentes y Auxiliares
# MAGIC <ul>
# MAGIC   <li>df = dataframe</li>
# MAGIC   <li>e = event</li>
# MAGIC   <li>2 = DWBPLAY_CABA</li>
# MAGIC </ul>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Conexion de Source y Obtención de DataFrames
# MAGIC <p> Descripción: 
# MAGIC <ol>
# MAGIC   <li>Establece la conexión al STG1 del Data Lake.</li>
# MAGIC   <li>Se obtienen los archivos parquet de la Base de Datos DWBPLAY_CABA para crear los dataframes en formato PySpark.</li>
# MAGIC </ol>

# COMMAND ----------

  # Leer tablas de Apuestas y de Eventos
df_aadd = spark.read.table("entprod_mdn.bplay.apuestas_deportivas").select(col("partner_event_id")).distinct()
df_eventos = spark.read.table("entprod_mdn.bplay.eventos_completo")

# COMMAND ----------

df_e = df_eventos.join(df_aadd, "partner_event_id", "inner").distinct()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. DF Eventos DW CABA y CBA
# MAGIC <p> Esta función genera un dataframe con la información necesaria de los Eventos.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Switch PROD/DEV

# COMMAND ----------

# Definir los datos
# data = [(46, "460"), (13, "130"), (88, "25688")]
data = [(46, "460"), (13, "130"), (88, "880")]

# Definir los nombres de las columnas
columns = ["id", "partner"]

# Crear el DataFrame
df_partner_Migra = spark.createDataFrame(data, columns)

# COMMAND ----------

#   Variable PROD/DEV
PROD = dbutils.widgets.get("run_job")
print(f"El parametro de PROD es: {PROD}")

PROD = PROD == "True"
print(type(PROD))

# COMMAND ----------

#   Path PRD del Delta Table Eventos
delta_table_Eventos_prd = "entprod_mdn.bplay.eventos"
#   Path DEV del Delta Table Eventos
delta_table_Eventos_dev = "entprod_mdn.default.eventos"

# #   Variable PROD/DEV
# PROD = True

#   Switch PROD/DEV
if PROD:
    delta_table_Eventos = delta_table_Eventos_prd
else:
    delta_table_Eventos = delta_table_Eventos_dev

# Manejar el caso en que la tabla no exista
try:
    # Intentar leer la tabla Delta
    df_origen = spark.read.table(delta_table_Eventos)
    count = df_origen.select("partner_event_id").count()

    # Validar la tabla Fact Coupon Row
    if count > 0:
        exist = True
    else:
        exist = False
except Exception as e:
    # print(f"La tabla {delta_table_Balance} no existe o no se puede leer: {e}")
    exist = False

print(f"La tabla {delta_table_Eventos} existe: {exist}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Upsert de Tabla Delta "eventos"
# MAGIC <p> Obtenemos la tabla original para actualizar la Delta Table 'eventos' con el dataframe df_Eventos recién creado.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Obtención de la Última Tabla Generada para Actualización
# MAGIC <p> Si ya existe la Delta Table de Eventos creada se obtendra la tabla y se actualizará con el DataFrame generado en esta ejecución.
# MAGIC <p> Si no existiera, se definirá la limitación las particiones en 8, y se generará un nuevo Delta Table de "Eventos".

# COMMAND ----------

print(delta_table_Eventos)
print(exist)
print(PROD)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Carga del DataFrame Actualizado de Eventos
# MAGIC <p> Se carga la tabla actualizada en el Warehouse de Databricks para su posterior utilización o consumo.

# COMMAND ----------

# Obtengo el df de la tabla Eventos ya creada
def get_dif(nuevo):
    origen = spark.read.table(delta_table_Eventos)
    # Transformat a Dataframe y Seleccionar los campos por comparar
    origen = origen.select('partner_event_id', 'event_id', 'event_name', 'event_date', 'Fecha_Evento', 'Hora_Evento', 
                           'event_end_date', 'sport', 'competition', 'region', 'provider_id', 'deporte'
                        )
    # Obtengo las filas nuevas y actualizadas 
    res = nuevo.exceptAll(origen)
    # Agrega la columna de actualización
    #res = dif.withColumn("fecha_actualizacion", current_timestamp()).withColumn("fecha_creacion", to_timestamp(col("fecha_operacion")))
    return res

# COMMAND ----------

# # Filtra los ultimos 15 días
# def filter_last_15_days(df): 
#     last_15_days = concat(date_sub(current_timestamp(), 20).cast("string"),lit(" 23:59:59")).cast("timestamp")
#     result = df.filter(col("fecha_actualizacion") >= last_15_days)
#     return result

# COMMAND ----------

# Filtra hasta las  23:59:59 del dia anterior a la ejecucion
# def filter_last_day(df): 
#     yesterday_end = concat(date_sub(current_timestamp(), 1).cast("string"),lit(" 23:59:59")).cast("timestamp")
#     result = df.filter(col("fecha_operacion") <= yesterday_end)
#     return result

# COMMAND ----------

# Verificar si la ruta contiene una tabla Delta
if exist:
    # Se Obtiene las filas por actualizar
    df_Eventos_dif = get_dif(df_e)
    # Se obtiene las actualizaciones de los ultimos 15 días
    # df_Eventos_act = filter_last_15_days(df_Eventos_dif)
    # Se elimina hasta el ultimo día de ejecucion
    # df_Eventos_act = filter_last_day(df_Eventos_dif)
    print("Se actualizará la tabla de Eventos")

    # Actualizar el DataFrame existente
    dt_origen = DeltaTable.forName(spark, delta_table_Eventos)   
    # Upsert del DataFrame de Apuestas y Premios
    dt_origen.alias("dest")\
        .merge(source=df_Eventos_dif.alias("update"), condition="dest.partner_event_id = update.partner_event_id")\
            .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    print("Se actualizó exitosamente la tabla de Eventos")
else:
    # Se elimina hasta el ultimo día de ejecucion
    # df_Eventos_act = filter_last_day(df_e)
    # Agregado de las columnas identificatorias para el upgrade de la base
    df_Eventos = df_e \
        .coalesce(8)  # Limitar a 8 particiones el DF "df_AyP"
    # Agregado de las columnas identificatorias para el upgrade de la base
    df_Eventos.write\
        .mode("overwrite")\
            .saveAsTable(delta_table_Eventos)
    print("Se creó exitosamente la tabla de Eventos")


        # .withColumn("fecha_creacion", to_timestamp(col("fecha_operacion"))) \
            
            #         .withColumn("fecha_creacion", current_timestamp()) \
            # .withColumn("fecha_actualizacion", current_timestamp()) \
