# Databricks notebook source
# MAGIC %md
# MAGIC # Salas_MDN-bplay_PySpark.ipynb
# MAGIC <h> by PySpark
# MAGIC #### Estado: **OK**
# MAGIC <p> Source: Se realizará la lectura de archivos en formato parquet para obtener los dataframes necesarios para generar la tabla 'Salas', y cargarla en formato Delta Table para su posterior consulta.
# MAGIC <p> Se tomará la información desde el STG1 de Datalake para generar un consumible de STG2 en formato Delta Table.
# MAGIC <p> Inputs:
# MAGIC   <ol>
# MAGIC     <li>Room</li>
# MAGIC     <li>Service</li>
# MAGIC     <li>Room Partner</li>
# MAGIC   </ol>
# MAGIC <p> Output: Delta Table actualizado de "salas" y su carga en el Warehouse de Databricks.

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

from pyspark.sql.functions import col, concat, when, lit, max, regexp_replace, trim, rtrim
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %md
# MAGIC ### Nomenclatura de Fuentes y Auxiliares
# MAGIC <ul>
# MAGIC   <li>df = dataframe</li>
# MAGIC   <li>r = room</li>
# MAGIC   <li>s = service</li>
# MAGIC   <li>rp = room partner</li>
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
# MAGIC   <li>Se definen los paths de los archivos parquet "Room", "Service" y "Room Partner" para cada Base de Datos DWBPLAY_PY, DWBPLAY_SF, DWBPLAY_CABA y DWBPLAY_CORDOBA.</li>
# MAGIC   <li> Luego se leen los archivos parquet para crear los dataframes en formato PySpark.</li>
# MAGIC </ol>

# COMMAND ----------

## Query Dim Room
  # Get DF Room
df_r0 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_py/OGP/room')
df_r1 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_sf/OGP/room')
df_r2 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_caba/OGP/room')
df_r3 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_cordoba/OGP/room')
  # Get DF Service
df_s0 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_py/OGP/service')
df_s1 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_sf/OGP/service')
df_s2 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_caba/OGP/service')
df_s3 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_cordoba/OGP/service')
  # Get DF Room Partner
df_rp0 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_py/OGP/room_partner')
df_rp1 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_sf/OGP/room_partner')
df_rp2 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_caba/OGP/room_partner')
df_rp3 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_cordoba/OGP/room_partner')
  # Get DF Provider
df_p0 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_py/OGP/provider')
df_p1 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_sf/OGP/provider')
df_p2 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_caba/OGP/provider')
df_p3 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_cordoba/OGP/provider')

try:
    # Get Casino Games
  df_cg46 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/bplaybet_arg/dbo/casino_games')
    # Get Bookmaker Casino Games
  df_bcg46 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/bplaybet_arg/dbo/bookmaker_casino_game')
    # Get Casino Providers
  df_cp46 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/bplaybet_arg/dbo/casino_providers')
    # Get Translation Entries
  df_te46 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/bplaybet_arg/dbo/translation_entries')
    # Get Casino Category Casino Game
  df_cgc46 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/bplaybet_arg/dbo/casino_category_casino_game')
    # Get Casino Category Casino Game
  df_cc46 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/bplaybet_arg/dbo/casino_categories')

    ### Switch Carga GT Parana & Jujuy
  bl_Carga_GT = True

except Exception as e:
  print(f"No se pudieron leer las tablas de Apuestas Deportivas GT: {e}")
  bl_Carga_GT = False 

# COMMAND ----------

#   Variable PROD/DEV
PROD = dbutils.widgets.get("run_job")
print(f"El parametro de PROD es: {PROD}")

PROD = PROD == "True"
print(type(PROD))

# COMMAND ----------

# Definir los datos
data = [(46, 460), (13, 130), (88, 256)]

# Definir los nombres de las columnas
columns = ["id", "partner_id"]

# Crear el DataFrame
df_partner_Migra = spark.createDataFrame(data, columns)

# COMMAND ----------

#   Path PRD del Delta Table Salas
delta_table_Salas_prd = "entprod_mdn.bplay.salas"
#   Path DEV del Delta Table Salas
delta_table_Salas_dev = "entprod_mdn.default.salas"

#   Variable PROD/DEV
# PROD = False

#   Switch PROD/DEV
if PROD:
    delta_table_Salas = delta_table_Salas_prd
else:
    delta_table_Salas = delta_table_Salas_dev

try:
        # Get Delta Table Apuestas y Premios
        df_origen = spark.read.table(delta_table_Salas)
        count = df_origen.select("partner_room_id").count()

        # Valida tabla AyP
        if count > 0:
            exist = True
        else:
            exist = False
            
except:
        exist = False

# print(count)
print(exist)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. DF Salas
# MAGIC <p> Esta función genera un dataframe con la información de las verticales de las salas.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Definición de la Función Create_DF_Salas Alira

# COMMAND ----------

def Create_DF_Salas(s, r, rp, p):
    s = s.join(r, s["id"] == r["service"])\
            .select(s["id"],
                s["name"],
                s["product"],
                s["game_type"],
                s["legal_type"],
                s["jackpot_network"],
                s["adjusts_network"],
                s["provider"],
                s["enabled"],
            ).distinct()
    s = s.join(p,s["provider"] == p["id"], "left")\
            .select(
                s["id"],
                s["product"],
                s["game_type"],
                s["legal_type"],
                s["jackpot_network"],
                s["adjusts_network"],
                s["provider"],
                p["name"],
                s["enabled"],
            ).distinct()

    rp = rp.join(r, rp["room"] == r["id"])\
            .select(
                rp["partner"],
                rp["room_instance"],
                rp["room"]
            ).distinct()
    
    result = r.join(s, s["id"] == r["service"], "inner") \
    .select(
        r["id"].alias("room_id"),
        r["name"].alias("room_name"),
        r["provider"].alias("room_provider"),
        s["name"].alias("provider_name"),
        s["legal_type"].alias("service_legal_type_id"),
        when(s["legal_type"] == "TGM", "Slots").when(s["legal_type"] == "ADP", "Apuestas Deportivas").when(s["legal_type"].isin("BLJ", "RLT", "PUN", "POC"), "Casino Vivo").otherwise("No aplica").alias("service_legal_type"),
        when(s["legal_type"].isin("TGM", "BLJ", "RLT", "PUN", "POC"), "Casino").when(s["legal_type"] == "ADP", "Apuestas Deportivas").otherwise("Revisar").alias("service_product"),
    )

    SUpdate = result.join(rp, rp["room"] == result["room_id"], "inner") \
    .select(
        rp["partner"].alias("partner_id"),
        concat(rp["partner"], result["room_id"]).cast("string").alias("partner_room_id"),
        concat(rp["partner"], result["room_provider"]).alias("partner_room_provider_id"),
        result["room_name"],
        when(col("room_name").like("%obile%"), "Mobile").otherwise("PC").alias("Dispositivo"),
        result["provider_name"],
        result["service_legal_type_id"],
        result["service_legal_type"],
        result["service_product"],
    ).distinct()

    return SUpdate

# COMMAND ----------

# MAGIC %md
# MAGIC ### Definición de la Función Create_DF_Salas GT

# COMMAND ----------

def Create_DF_Salas_GT(cg, bcg, cp, te, cgc, cc, p):
    te = te.filter(col("language_code") == 'es')
    aux = cg.join(te, te["translation_id"] == cg["name"]).select(te["translation_id"], te["id"]).groupby("translation_id").agg(max("id").alias("translation_entries_id"))
    te = aux.select("translation_entries_id").join(te, aux["translation_entries_id"] == te["id"], "left").select("translation_id", "body", "id")

    salas = cg.join(bcg, bcg["casino_game_id"] == cg["id"], 'left')\
                .join(cp, cp["id"] == cg["casino_provider_id"], 'left')\
                .join(te, te["translation_id"] == cg["name"], 'left')\
                .join(cgc, (cgc["casino_game_id"] == cg["id"]) & (cgc["bookmaker_id"] == bcg["bookmaker_id"]), 'left')\
                .join(cc, cc["id"] == cgc["casino_category_id"], 'left')\
                .join(p, bcg["bookmaker_id"] == p["id"], 'inner')\
                    .select(
                        p['partner_id'],
                        concat(p['partner_id'], cg["id"]).alias("partner_room_id"),
                        concat(p['partner_id'], cg["casino_provider_id"]).alias("partner_room_provider_id"),
                        te["body"].alias("room_name"),
                        when(te["body"].like("%obile%"), "Mobile").otherwise("PC").alias("Dispositivo"),
                        cp["code"].alias("provider_name"),
                        when(cc["id"] == 1, 'TGM')
                        .when(cc["id"] == 3, 'RLT')
                        .when(((cc["id"] == 6) | (cc["id"] == 10)) & ((te["body"].like("%Rul%")) | (te["body"].like("%Roul%"))), "RLT")
                        .when(((cc["id"] == 6) | (cc["id"] == 10)) | ((te["body"].like("%Black%") | te["body"].like("%BlacJ%"))), "BLJ")
                        .when(((cc["id"] == 6) | (cc["id"] == 10)) & (te["body"].like("%Bacc%")), "PUN")
                        .when(cc["id"] == 10, "TGM")
                        .otherwise("No aplica").alias("service_legal_type_id"),
                        # cc["id"].alias("service_legal_typecc_id"),
                        lit("Casino").alias("service_product"),
                    ).withColumn('service_legal_type', 
                                 when((col("service_legal_type_id") == "TGM"), "Slots")
                                 .when(col("service_legal_type_id") == "ADP", "Apuestas Deportivas")
                                 .when(col("service_legal_type_id").isin("BLJ", "RLT", "PUN", "POC"), "Casino Vivo")
                            .otherwise("No aplica")).distinct()
    return salas

# COMMAND ----------

# Creación de la Fila para Apuestas Deportivas para GT
def Create_DF_Sala_AADD_GT(Salas_GT):
    # Definir los nombres de las columnas
    columns =   ["partner_id",  "partner_room_id",  "partner_room_provider_id", "room_name",    "Dispositivo",  "provider_name",   "service_legal_type_id", "service_legal_type", "service_product"]
    data =      [("460", 4609009, "4609009999", "ADP", "PC", "GT", "ADP",  "Apuestas Deportivas", "Apuestas Deportivas"),
                 ("130", 1309009, "1309009999", "ADP", "PC", "GT", "ADP",  "Apuestas Deportivas", "Apuestas Deportivas"),
                 ("256", 2569009, "2569009999", "ADP", "PC", "GT", "ADP",  "Apuestas Deportivas", "Apuestas Deportivas")]
    return Salas_GT.unionByName(spark.createDataFrame(data, columns))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ejecución del DataFrame Salas por Base de Datos: DWBPLAY_PY, DWBPLAY_SF, DWBPLAY_CABA y DWBPLAY_CORDOBA.
# MAGIC <p> Se ejecuta la función Create_DF_Salas para obtener el dataframe con la información de las verticales de la sala en las bases de datos DWBPLAY_PY, DWBPLAY_SF, DWBPLAY_CABA y DWBPLAY_CORDOBA.

# COMMAND ----------

# DWBPLAY_PY
df_Salas0 = Create_DF_Salas(df_s0, df_r0, df_rp0, df_p0)
# DWBPLAY_SF
df_Salas1 = Create_DF_Salas(df_s1, df_r1, df_rp1, df_p1)
# DWBPLAY_CABA
df_Salas2 = Create_DF_Salas(df_s2, df_r2, df_rp2, df_p2)
# DWBPLAY_CORDOBA
df_Salas3 = Create_DF_Salas(df_s3, df_r3, df_rp3, df_p3)
# Filtrado Salas CORDOBA '255'
df_Salas3 = df_Salas3.filter(col("partner_id") == '255')

if bl_Carga_GT:
    # BPLAYBET_ARG PRN y JUJ
    df_SalasGT = Create_DF_Salas_GT(df_cg46, df_bcg46, df_cp46, df_te46, df_cgc46, df_cc46, df_partner_Migra)
    # GT AADD
    df_SalasGT = Create_DF_Sala_AADD_GT(df_SalasGT)

    #     ### Borrar Cuando se solucione
    # df_SalasGT = df_SalasGT.filter(~(
    #     (col('partner_room_id') == 2561035) & 
    #     (col('service_legal_type_id') == 'TGM')
    # )).distinct()

print(bl_Carga_GT)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Union de DF Salas
# MAGIC <p> Se realiza la unificacion de las ejecuciones de las bases de datos DWBPLAY_PY, DWBPLAY_SF, DWBPLAY_CABA y DWBPLAY_CORDOBA.

# COMMAND ----------

if bl_Carga_GT:
    # Union ALIRA y GT
    df_Salas = df_Salas0.unionByName(df_Salas1).unionByName(df_Salas2).unionByName(df_Salas3).unionByName(df_SalasGT).distinct()
else:
    # Union ALIRA
    df_Salas = df_Salas0.unionByName(df_Salas1).unionByName(df_Salas2).unionByName(df_Salas3).distinct()


# COMMAND ----------

# MAGIC %md
# MAGIC ### Filtrado De Duplicados (A la espera de que sea Fixeado)

# COMMAND ----------

from pyspark.sql.functions import count
# Paso1: Se obtiene la lista de IDs duplicados
df_duplicados = df_Salas.groupBy("partner_room_id") \
    .agg(count("*").alias("cant_ids")) \
    .filter(col("cant_ids") > 1)
# display(df_duplicados)

# Paso 2: Extraer los valores duplicados como una lista
ids_duplicados = [row["partner_room_id"] for row in df_duplicados.collect()]
print(ids_duplicados)

# Paso 3: Filtrar df_Salas para excluir los duplicados
df_Salas = df_Salas.filter(~(col("partner_room_id").isin(ids_duplicados) & ((col('service_legal_type_id') == 'TGM'))))
#display(df_Salas.join(df_duplicados, 'partner_room_id', 'inner').select(df_Salas['*']))

# COMMAND ----------

# Crea la columna "name" con los room_name simplificados para el agrupamiento
df_Salas = df_Salas.withColumn("name", rtrim(regexp_replace("room_name",
                                r" - Mobile|- Mobile| - Mobi| - mobile|MZA |PBA |CO |CABA |ER | -  Mobile|  - Mobile|  -  Mobile| - Mobiel| Mobile| - COR| - CABA|  - MEND| - RIOS| - MEND", "")))
# Luego eliminar cualquier tipo de espacio final de la columna 'name'
df_Salas = df_Salas.withColumn("name", trim(regexp_replace(col("name"), r"\s+$", "")))

# COMMAND ----------

print(f"Se cargará en: {delta_table_Salas}")
print(f"Se cargará la carga de la DB de GT: {bl_Carga_GT}")
print(exist)

# COMMAND ----------

# print(df_Salas.count())
# print(df_Salas.distinct().count())
# print(df_Salas.select('partner_room_id').count())
# print(df_Salas.select('partner_room_id').distinct().count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Upsert de Tabla Delta "Salas"
# MAGIC <p> Obtenemos la tabla original para actualizar la Delta Table 'Salas' con el dataframe df_Salas recién creado.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Obtención de la Última Tabla Generada para Actualización
# MAGIC <p> Si ya existe la Delta Table de Salas creada se obtendra la tabla y se actualizará con el DataFrame generado en esta ejecución.
# MAGIC <p> Si no existiera, se definita la limitación las particiones en 8 y se generará un nuevo Delta Table de "Salas".

# COMMAND ----------

# Verificar si la ruta contiene una tabla Delta
if exist:
    # Si la ruta contiene una tabla Delta, creamos el objeto DeltaTable
    origen = DeltaTable.forName(spark, delta_table_Salas)
    # Upsert del DataFrame de Apuestas y Premios
    (origen.alias("dest")\
        .merge(source=df_Salas.alias("update"), condition='dest.partner_room_id = update.partner_room_id')\
            .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())
else:
    # Limitar a 8 particiones el DF "Salas"
    df_Salas = df_Salas.coalesce(8)
    # Guardar el DataFrame en formato Delta con overwrite habilitado
    df_Salas.write\
        .mode("overwrite")\
                .saveAsTable(delta_table_Salas)
    # df_Salas.write\
    #     .mode("overwrite")\
    #         .option("overwriteSchema", "true")\
    #             .saveAsTable(delta_table_Salas)

# COMMAND ----------

# # Limitar a 8 particiones el DF "Salas"
# df_Salas = df_Salas.coalesce(8)
# # Guardar el DataFrame en formato Delta con overwrite habilitado
# df_Salas.write\
#     .mode("overwrite")\
#             .saveAsTable(delta_table_Salas)