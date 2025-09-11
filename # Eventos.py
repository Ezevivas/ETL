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

from pyspark.sql.functions import col, concat, cast, date_format, from_utc_timestamp, lit, count, when, format_string
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

  #Alira
    #Get Event CABA
df_e2 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_caba/APUESTAS/event/*')
df_pn2 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_caba/OGP/partner').filter(col("id") != 255)
    #Get Event CORDOBA
df_e3 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_cordoba/APUESTAS/event/*')
df_pn3 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_cordoba/OGP/partner')

try:
    # GT
      #Get competitions
    df_c = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/bplaybet_arg/dbo/competitions')
      #Get fixtures
    df_f = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/bplaybet_arg/dbo/fixtures')
      #Get markets
    df_m = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/bplaybet_arg/dbo/markets')
      #Get selections
    df_s = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/bplaybet_arg/dbo/selections')
      #Get sports
    df_sp = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/bplaybet_arg/dbo/sports')
      #Get translation_entries
    df_te = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/bplaybet_arg/dbo/translation_entries')
      #Get regions
    df_r = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/bplaybet_arg/dbo/regions')

      ### Switch Carga GT Parana & Jujuy
    bl_Carga_GT = True

except Exception as e:
    print(f"No se pudieron leer las tablas de Apuestas Deportivas GT: {e}")
    bl_Carga_GT = False    

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
delta_table_Eventos_prd = "entprod_mdn.bplay.eventos_completo"
#   Path DEV del Delta Table Eventos
delta_table_Eventos_dev = "entprod_mdn.default.eventos_completo"

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
# MAGIC ### Definición de la Función Create_DF_Event
# MAGIC <p> Esta función genera un dataframe con la información necesaria de los Eventos.

# COMMAND ----------

def Create_DF_Event(e, p):

    p = p.select(col("id").alias("partner")).distinct()
    
    event =  e.select(
                    e["id"].alias("event_id"),
                    e["name"].alias("event_name"),
                    e["start_date"].alias("event_date"),
                    e["start_date"].cast("date").alias("Fecha_Evento"),
                    col("start_date").cast("string").substr(12, 5).alias("Hora_Evento"),
                    e["estimated_end_date"].alias("event_end_date"),
                    e["sport"],
                    e["competition"],
                    e["region"],
                    e["provider"].alias("provider_id"),
                    )
    event = event.crossJoin(p)

    event = event.withColumn("partner_event_id", concat("partner", "event_id").cast("bigint")).select("partner_event_id", "event_id", "event_name", "event_date", "Fecha_Evento", "Hora_Evento", "event_end_date", "sport", "competition", "region", "provider_id")
    return event

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Create Eventos GT JUJ y PRN
# MAGIC

# COMMAND ----------

def Create_eGT(c, f, m, s, sp, te, r, jur):
        te = te.filter((col('language_code') == 'en') & (col('bookmaker_id').isNull())).select('translation_id', 'body').filter(~(col('translation_id').isin(322612, 2386))).distinct()

        f  = f.join(te, te.translation_id == f.name, "inner") \
                        .select(f['id'], f['name'], te['body'].alias('fixture_body'), f['start_at'], f['finished_at'], f['competition_id'], f['sport_id']).distinct()

        c   = c.join(te, te.translation_id == c.name, "inner") \
                .select(c['id'], te['body'].alias('competition_body'), c['region_id'], c['sport_id'])

        sp  = sp.join(te, te.translation_id == sp.name, "inner") \
                .select(sp['id'], te['body'].alias('sport_body'))

        r = r.join(te, r.name == te.translation_id, 'inner')\
                .select(r['id'], te['body'].alias('region_body'))

        eGT  = s.join(m, m.id == s.market_id, "left") \
                                .join(f, f.id == m.fixture_id, "left") \
                                        .join(c, c.id == f.competition_id, "left") \
                                                .join(sp, sp.id == f.sport_id, "left") \
                                                        .join(r, c.region_id == r.id, "left")\
                        .select(
                                s['id'].cast('bigint').alias('event_id'),
                                f['fixture_body'].alias('event_name'),
                                date_format(from_utc_timestamp(f['start_at'], 'America/Buenos_Aires'), 'yyyy-MM-dd HH:mm:ss').cast(TimestampType()).alias('event_date'), #cambiar fecha utc a la zona horaria y escribirla en formato timestamp	
                                date_format(from_utc_timestamp(f['start_at'], 'America/Buenos_Aires'), 'yyyy-MM-dd').cast(DateType()).alias('fecha_evento'), #cambiar fecha utc a la zona horaria y escribirla en formato timestamp
                                date_format(from_utc_timestamp(f['start_at'], 'America/Buenos_Aires'), 'HH:mm').alias('hora_evento'), #cambiar fecha utc a la zona horaria y escribirla en formato timestamp
                                date_format(from_utc_timestamp(f['finished_at'], 'America/Buenos_Aires'), 'yyyy-MM-dd HH:mm:ss').cast(TimestampType()).alias('event_end_date'), #cambiar fecha utc a la zona horaria y escribirla en formato timestamp
                                sp['sport_body'].alias('sport'),
                                c['competition_body'].alias('competition'),
                                r['region_body'].alias('region'),  
                                lit(999).alias('provider_id')
                        ).distinct()

        joined_df = eGT.crossJoin(df_partner_Migra.select(col("partner")))

        joined_df = joined_df.withColumn(
                                        "partner_event_id",
                                        concat(
                                                col("partner"), col("event_id")
                                                #when(col("partner") == "25688", format_string("%012d", col("event_id")).cast("string"))
                                                #.otherwise(col("event_id").cast("string"))
                                        ) .cast(LongType())   # rompe la lógica el casteo al final
                                        ).distinct()

        result = joined_df.drop("partner")

        return result

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ejecución del DataFrame Eventos por Base de Datos: DWBPLAY_CABA y DWBPLAY_CORDOBA
# MAGIC <p> Se ejecuta la función Create_DF_Event para obtener el dataframe con la información de los evnetos en la base de datos: DWBPLAY_CABA y DWBPLAY_CORDOBA.

# COMMAND ----------

if bl_Carga_GT:  
    # GT JUJ, PRN y MZA  
    df_Eventos_GT = Create_eGT(df_c, df_f, df_m, df_s, df_sp, df_te, df_r, df_partner_Migra)

    # DWBPLAY_CABA
df_Eventos2 = Create_DF_Event(df_e2, df_pn2)
    # DWBPLAY_CORDOBA
df_Eventos3 = Create_DF_Event(df_e3, df_pn3)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Union de DF Eventos
# MAGIC <p> Se realiza la unificacion de las ejecuciones de las bases de datos DWBPLAY_CABA y DWBPLAY_CORDOBA.

# COMMAND ----------

if bl_Carga_GT:
    df_Eventos = df_Eventos2.unionByName(df_Eventos3).unionByName(df_Eventos_GT).distinct()
else:
    df_Eventos = df_Eventos2.unionByName(df_Eventos3).distinct()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Unificación de columna Sport

# COMMAND ----------

from pyspark.sql.functions import when, col

df_Eventos = df_Eventos.withColumn(
    "deporte",
    when(col("sport").isin("Baloncesto", "Basketball", "Básquet"), "Básquet")
    .when(col("sport").isin("Fútbol", "Football"), "Fútbol")
    .when(col("sport").isin("Tenis", "Tennis"), "Tenis")
    .otherwise("Otros deportes")
)


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

# Comportamiento ante Duplicados
import pyspark.sql.functions as F
duplicados = df_Eventos.groupBy('partner_event_id').agg(F.count('*').alias('count')).filter(col('count') > 1).count() 
print(duplicados)
fl_duplicados = duplicados == 0
print(fl_duplicados)

# display(df_Eventos.count())
# display(df_Eventos.select('partner_event_id').distinct().count())
# 309018395

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Carga del DataFrame Actualizado de Eventos
# MAGIC <p> Se carga la tabla actualizada en el Warehouse de Databricks para su posterior utilización o consumo.

# COMMAND ----------

# Verificar si la ruta contiene una tabla Delta
if exist:
    print(exist)
    print(f'Se actualizará la tabla {delta_table_Eventos}')
    if fl_duplicados:
        print(fl_duplicados)
        # Si la ruta contiene una tabla Delta, creamos el objeto DeltaTable
        origen = DeltaTable.forName(spark, delta_table_Eventos)
        # Upsert del DataFrame de Eventos
        (origen.alias("dest")
            .merge(
                source=df_Eventos.alias("update"), 
                condition='dest.partner_event_id = update.partner_event_id'
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute())
    else:
        print('Existen duplicados')
else:
    print('Se escribirá el DF')
    if fl_duplicados:
        print(fl_duplicados)
        # Limitar a 8 particiones el DF "Eventos"
        df_Eventos = df_Eventos.coalesce(8)
        # Guardar el DataFrame en formato Delta con overwrite habilitado
        df_Eventos.write\
            .mode("overwrite")\
                .option("overwriteSchema", "true") \
                    .saveAsTable(delta_table_Eventos)

# COMMAND ----------

# # Guardar el DataFrame en formato Delta con overwrite habilitado
# df_Eventos.write\
#     .mode("overwrite")\
#         .option("overwriteSchema", "true") \
#             .saveAsTable(delta_table_Eventos)