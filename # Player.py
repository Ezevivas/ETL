# Databricks notebook source
# MAGIC %md
# MAGIC # Player_MDN-bplay_PySpark.ipynb
# MAGIC <h> by PySpark
# MAGIC #### Estado: **OK**
# MAGIC <p> Source: Se realizará la lectura de archivos en formato parquet para obtener los dataframes necesarios para generar la tabla 'Players', y cargarla en formato Delta Table para su posterior consulta.
# MAGIC <p> Se tomará la información desde el STG1 de Datalake para generar un consumible de STG2 en formato Delta Table.
# MAGIC <p> Inputs:
# MAGIC   <ol>
# MAGIC     <li>Player</li>
# MAGIC     <li>State</li>
# MAGIC     <li>Country</li>
# MAGIC     <li>Sourse</li>
# MAGIC   </ol>
# MAGIC <p> Output: Delta Table actualizado de "Players" y su carga en el Warehouse de Databricks.
# MAGIC
# MAGIC ![migracion.drawio.png](./migracion.drawio.png "migracion.drawio.png")
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Start Up

# COMMAND ----------

# MAGIC %md
# MAGIC ### Declaración de librerias utilizadas
# MAGIC <p> pyspark.sql.functions = Importa funciones de PySpark para manipular y trabajar con columnas y datos en los DataFrames.
# MAGIC <p> pyspark.sql.types = Importa tipos de datos, para definir la estructura de los DataFrames.

# COMMAND ----------

from pyspark.sql.functions import col, sum, max, min, concat, datediff, expr, lit, when, month, year, format_string, current_timestamp, floor, current_date, row_number, to_timestamp, date_sub, date_format
from pyspark.sql.types import IntegerType, StringType, DateType, TimestampType, LongType, DecimalType
from pyspark.sql import Window
from delta.tables import DeltaTable
from datetime import datetime

# COMMAND ----------

# MAGIC %md
# MAGIC ### Nomenclatura de Fuentes y Auxiliares
# MAGIC <ul>
# MAGIC   <li>df = dataframe</li>
# MAGIC   <li>p = player</li>
# MAGIC   <li>s = state</li>
# MAGIC   <li>c = country</li>
# MAGIC   <li>source = Source (Afiliadores e Influencers)</li>
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
# MAGIC   <li>Se definen los paths de los archivos parquet "Player", "State" y "Country" para cada Base de Datos DWBPLAY_PY, DWBPLAY_SF, DWBPLAY_CABA y DWBPLAY_CORDOBA.</li>
# MAGIC   <li>Luego se leen los archivos parquet para crear los dataframes en formato PySpark.</li>
# MAGIC </ol>

# COMMAND ----------

# ALIRA
  # Get State
df_s0 = spark.read.parquet("/Volumes/boldtproddatalake/boldtdatalake/dwbplay_py/OGP/state")
df_s1 = spark.read.parquet("/Volumes/boldtproddatalake/boldtdatalake/dwbplay_sf/OGP/state")
df_s2 = spark.read.parquet("/Volumes/boldtproddatalake/boldtdatalake/dwbplay_caba/OGP/state")
df_s3 = spark.read.parquet("/Volumes/boldtproddatalake/boldtdatalake/dwbplay_cordoba/OGP/state")
  # Get Country
df_c0 = spark.read.parquet("/Volumes/boldtproddatalake/boldtdatalake/dwbplay_py/OGP/country")
df_c1 = spark.read.parquet("/Volumes/boldtproddatalake/boldtdatalake/dwbplay_sf/OGP/country")
df_c2 = spark.read.parquet("/Volumes/boldtproddatalake/boldtdatalake/dwbplay_caba/OGP/country")
df_c3 = spark.read.parquet("/Volumes/boldtproddatalake/boldtdatalake/dwbplay_cordoba/OGP/country")
  # Get Player
df_p0 = spark.read.parquet("/Volumes/boldtproddatalake/boldtdatalake/dwbplay_py/OGP/player")
df_p1 = spark.read.parquet("/Volumes/boldtproddatalake/boldtdatalake/dwbplay_sf/OGP/player")
df_p2 = spark.read.parquet("/Volumes/boldtproddatalake/boldtdatalake/dwbplay_caba/OGP/player")
df_p3 = spark.read.parquet("/Volumes/boldtproddatalake/boldtdatalake/dwbplay_cordoba/OGP/player")

  # Get Source (Afiliadores e Influencers)
df_source = spark.read.table("entprod_mdn.bplay.source")
# df_source = spark.read.table("entprod_mdn.default.source")

    #Get Vips
df_vips = spark.read.csv('/Volumes/entproddatalake/default/mdn_bplay/Tablas_CSV/Vips.csv', header=True, inferSchema=True, sep=';')

try:
  # << GT >>
    #Get Users GT
  df_usr46 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/bplaybet_arg/dbo/users').filter(col('bookmaker_id').isin(13, 46, 88))
    #Get Cities GT
  df_cts46 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/bplaybet_arg/dbo/cities')
    #Get States GT
  df_sts46 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/bplaybet_arg/dbo/states')
    #Get Countries GT
  df_ctr46 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/bplaybet_arg/dbo/countries')
    ### Switch Carga GT Parana & Jujuy
  bl_Carga_GT = True

except Exception as e:
  print(f"No se pudieron leer las tablas de GT: {e}")
  bl_Carga_GT = False


# COMMAND ----------

#   Variable PROD/DEV
PROD = dbutils.widgets.get("run_job")
print(f"El parametro de PROD es: {PROD}")

PROD = PROD == "True"
print(type(PROD))

# COMMAND ----------

#Path PRD del Delta Table Player
delta_table_Player_prd = 'entprod_mdn.bplay.player'

#Path DEV del Delta Table Player
delta_table_Player_dev = 'entprod_mdn.default.player'

#   Switch PROD/DEV
if PROD:
    delta_table_Player = delta_table_Player_prd
else:
    delta_table_Player = delta_table_Player_dev

# Manejar el caso en que la tabla no exista
try:
    # Intentar leer la tabla Delta
    df_origen = spark.read.table(delta_table_Player)
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
# MAGIC ### Tabla Auxiliar Migración

# COMMAND ----------

# Definir los datos
data = [(46, 460), (13, 130), (88, 256)]

# Definir los nombres de las columnas
columns = ["id", "partner_id"]

# Crear el DataFrame
df_partner_Migra = spark.createDataFrame(data, columns)

# display(df_partner_Migra)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Definición de la Función Create Player
# MAGIC <p> Esta función genera un dataframe con la información de las verticales de las salas.

# COMMAND ----------

# MAGIC %md
# MAGIC ### DW Alira

# COMMAND ----------

def Create_Player(p, s, c, af):
    # Filtros Player
        p = p.filter(col("status") != 1) # Esta linea quita los empleados
        p = p.withColumn("mes", month("created_date"))
        p = p.withColumn("anio", year("created_date"))
        p = p.withColumn("camada_registro", concat(p["anio"], lit("-"), format_string("%02d", p["mes"])))
        p = p.withColumn("relacion_diccionario_g_t_s",concat(p["partner"], p["gender"], format_string("%02d", p["type"]), format_string("%02d", p["status"])).cast("bigint"))
        p = p.withColumn("edad", floor(datediff(current_date(), p["birth_date"]) / 365.25))
        p = p.withColumn("partner_affiliate_id", when(col("affiliate").isNull(), None)\
            .otherwise(concat(p["partner"], format_string("%05d", p["affiliate"])).cast(IntegerType())))
        p = p.withColumn("partner_affiliate_source_id", when(col("affiliate").isNull(), None)\
            .otherwise(concat(col("partner"), 
                              format_string("%05d", col("affiliate")), 
                              format_string("%08d", col("source")))).cast(LongType()))
        
    # Filtros Afiliadores
        af = af.filter(col("activo") == 1)

    # Join Player Afiliadores
        p1 = p.join(af, p["partner_affiliate_source_id"] == af["partner_affiliate_source_id"], "left")\
            .select(
            p["partner"].alias("partner_id"),
            p["type"].alias("player_type_id"),
            concat(p["partner"], p["id"].cast(DecimalType(10,0))).alias("partner_player_id"),
            p["id"].cast(IntegerType()).alias("player_id"),
            lit(None).cast(LongType()).alias("GT_id"),
            p["alias"].alias("player_alias"),
            p["status"].alias("player_status_id"),
            when(p["source"].isNull(), 0).otherwise(p["source"]).alias("player_source_id"),
            p["first_name"].alias("nombre"),
            p["middle_name"].alias("apellido"),
            p["birth_date"].cast(DateType()).alias("fecha_nacimiento"),
            p["edad"],
            when((p["edad"] >= 18) & (p["edad"] <= 25), "18 a 25")
                .when((p["edad"] >= 26) & (p["edad"] <= 32), "26 a 32")
                .when((p["edad"] >= 33) & (p["edad"] <= 40), "33 a 40")
                .when((p["edad"] >= 41) & (p["edad"] <= 50), "41 a 50")
                .when((p["edad"] >= 51) & (p["edad"] <= 60), "51 a 60")
                .when((p["edad"] >= 61) & (p["edad"] <= 67), "61 a 67")
                .when(p["edad"] >= 68, "68 o más")
                .otherwise("No Aplica").alias("rango_edad"),
            p["national_id"].alias("DNI"),
            p["gender"].alias("sexo"),
            p["email"],
            p["phone"],
            p["address"],
            p["zip_code"],
            p["city"].alias("player_city"),
            p["country"],
            concat(p["partner"], p["state"]).cast(LongType()).alias("partner_state_id"),
            concat(p["partner"], p["country"]).cast(LongType()).alias("partner_country_id"),
            p["state"],
            p["exclusion_date"].cast(DateType()).alias("fecha_exclusion"),
            p["created_date"].cast(TimestampType()).alias("fecha_registro"),
            p['camada_registro'].cast(StringType()),
            datediff(current_date(), p["created_date"]).alias("dias_antiguedad"),
            p["partner_affiliate_source_id"],
            p["partner_affiliate_id"],
            when((( p["partner_affiliate_id"] == (concat(af["partner_id"], format_string("%05d", af["affiliate_id"]))).cast(IntegerType()) ) & (af["tipo_tabla"] == 'Afiliador') ), (when(p["created_date"] >= af["fecha_contrato"], "Nuevo").otherwise("Fidelizado"))).otherwise(None).alias("control_afiliador"),
            when(((p["partner_affiliate_source_id"] == af["partner_affiliate_source_id"])), "Es Afiliado").otherwise(None).alias("es_afiliado"),
            af["nombre_affiliate"].alias("afiliador"),
            af["nombre_source"].alias("source"),
            af["Tipo_Afiliador"].alias("tipo_afiliador"),
            p["relacion_diccionario_g_t_s"].cast(LongType())
            )
        
    # Join Player State Currency
        player = p1.join(s, p1["state"] == s["id"], 'left')\
            .join(c, p1["country"] == c["id"], 'left')\
            .select(
            p1["partner_id"],
            p1["partner_player_id"],
            p1["player_id"],
            p1["GT_id"],
            p1["player_type_id"],
            p1["player_alias"],
            p1["player_status_id"],
            p1["player_source_id"],
            p1["nombre"],
            p1["apellido"],
            p1["fecha_nacimiento"],
            p1["edad"],
            p1["rango_edad"],
            p1["DNI"],
            p1["sexo"],
            p1["email"],
            p1["phone"],
            p1["address"],
            p1["zip_code"],
            p1["player_city"],
            p1["partner_state_id"],
            s["name"].alias("player_state"),
            p1["partner_country_id"],
            c["name"].alias("player_country"),
            p1["fecha_exclusion"],
            p1["fecha_registro"],
            p1["camada_registro"],
            p1["dias_antiguedad"],
            p1["partner_affiliate_source_id"],
            p1["partner_affiliate_id"],
            p1["control_afiliador"], 
            p1["es_afiliado"],
            p1["afiliador"],
            p1["source"],
            p1["tipo_afiliador"],
            p1["relacion_diccionario_g_t_s"]
            ).distinct()
                
        return player

# COMMAND ----------

# MAGIC %md
# MAGIC ### DW GT

# COMMAND ----------

def Create_Player_GT(u, c, s, cy, af, prn):
    u = u.withColumn("created_at", to_timestamp(expr("created_at - INTERVAL 3 HOURS"))).withColumn("updated_at", to_timestamp(expr("updated_at - INTERVAL 3 HOURS")))
    # u = u.filter((col("local_register") == '0') & (col("is_backoffice") == '0') & (col("is_services") == '0'))
    u = u.withColumn("edad", floor(datediff(current_date(), u["birth_date"]) / 365.25))
    u = u.withColumn("mes", month("created_at"))
    u = u.withColumn("anio", year("created_at"))
    u = u.withColumn("camada_registro", concat(u["anio"], lit("-"), format_string("%02d", u["mes"])))

    p = u.join(c, c["id"] == u["city_id"], "left")\
                .join(s, s["id"] == c["state_id"], "left")\
                .join(cy, cy["id"] == s["country_id"], "left")\
                .join(prn, prn["id"] == u["bookmaker_id"], "left")\
                    .select(
                        prn["partner_id"].cast(LongType()).alias("partner_id"),
                        concat(prn['partner_id'], 
                               when(u['alira_id'].isNotNull(), u['alira_id'])
                               .when(u["bookmaker_id"].isin(88), concat(u["bookmaker_id"], format_string("%08d", u["id"])))
                               .otherwise(u["id"])).alias("partner_player_id"),
                        #lit(3).cast(LongType()).alias("player_type_id"),
                        when(~((col("local_register") == '0') & (col("is_backoffice") == '0') & (col("is_services") == '0')), lit(1)).otherwise(lit(3))\
                            .cast(LongType()).alias("player_type_id"),
                        when(u['alira_id'].isNotNull(), u['alira_id']).otherwise(u["id"]).cast(IntegerType()).alias("player_id"),
                        u["id"].cast(LongType()).alias("GT_id"),
                        u["username"].alias("player_alias"),
                        lit(2).cast(LongType()).alias("player_status_id"),
                        # lit(None).cast(LongType()).alias("player_source_id"),
                        u["referrer_reseller_id"].cast(LongType()).alias("player_source_id"),
                        # concat(prn["partner_id"], format_string("%05d", u["referrer_reseller_id"])).cast(LongType()).alias("partner_affiliate_source_id"),
                        lit(None).alias("partner_affiliate_source_id"),
                        when(u["middle_name"].isNotNull(), concat(u["first_name"], lit(' '), u["middle_name"])).otherwise(u["first_name"]).alias("nombre"),
                        u["last_name"].alias("apellido"),
                        u["birth_date"].cast(DateType()).alias("fecha_nacimiento"),
                        u["edad"],
                        when((u["edad"] >= 18) & (u["edad"] <= 22), '18 a 22')
                        .when((u["edad"] >= 23) & (u["edad"] <= 27), '23 a 27')
                        .when((u["edad"] >= 28) & (u["edad"] <= 32), '28 a 32')
                        .when((u["edad"] >= 33) & (u["edad"] <= 37), '33 a 37')
                        .when((u["edad"] >= 38) & (u["edad"] <= 42), '38 a 42')
                        .when((u["edad"] >= 43) & (u["edad"] <= 47), '43 a 47')
                        .when((u["edad"] >= 48) & (u["edad"] <= 52), '48 a 52')
                        .when((u["edad"] >= 53) & (u["edad"] <= 57), '52 a 57')
                        .when((u["edad"] >= 58) & (u["edad"] <= 62), '58 a 62')
                        .when((u["edad"] >= 63) & (u["edad"] <= 67), '62 a 67')
                        .when((u["edad"] >= 68), '68 o más')
                        .otherwise("No Aplica").alias("rango_edad"),
                        u["identification"].alias("DNI"),
                        when(u["gender_id"] == 1, lit(2)).when(u["gender_id"] == 2, lit(1)).cast(LongType()).alias("sexo"),
                        u["email"],
                        u["mobile_phone"].alias("phone"),
                        u["address1"].alias("address"),
                        u["postcode"].alias("zip_code"),
                        c["code"].alias("player_city"),
                        concat(prn["partner_id"], c["state_id"]).cast(LongType()).alias("partner_state_id"),
                        s["code"].alias("player_state"),
                        concat(prn["partner_id"], s["country_id"]).cast(LongType()).alias("partner_country_id"),
                        cy["code"].alias("player_country"),
                        lit(None).cast(DateType()).alias("fecha_exclusion"),
                        u["created_at"].alias("fecha_registro"),
                        u["camada_registro"],
                        datediff(current_date(), u["created_at"]).alias("dias_antiguedad"),
                        # concat(prn["partner_id"], format_string("%05d", u["referrer_reseller_id"])).cast(IntegerType()).alias("partner_affiliate_id"),
                        lit(None).alias("partner_affiliate_id"),
                        lit(None).cast(StringType()).alias("control_afiliador"),
                        concat(
                            prn["partner_id"],
                            when(u["gender_id"] == 1, lit('2')).when(u["gender_id"] == 2, lit('1')).otherwise(u["gender_id"]), 
                            #lit('03'),
                            when((col("local_register") == '0') & (col("is_backoffice") == '0') & (col("is_services") == '0'), lit('01')).otherwise(lit('03')), 
                            lit('02')
                        ).cast(LongType()).alias("relacion_diccionario_g_t_s")
                    )

    players = p.join(af, (p["partner_id"] == af["partner_id"]) & (p["player_source_id"] == af["source_id"]), "left") \
                .select(p['partner_id'], p['partner_player_id'], p['player_type_id'], p['player_id'], 'GT_id', 'player_alias',
                        'player_status_id', 'player_source_id', af["partner_affiliate_source_id"], p['nombre'], p['apellido'], p['fecha_nacimiento'], 'edad', 'rango_edad', 'DNI', 'sexo', 'email', 'phone', 'address', 'zip_code', 'player_city', 'partner_state_id', 'player_state', 'partner_country_id', 'player_country', 'fecha_exclusion', 'fecha_registro', 'camada_registro', 'dias_antiguedad', af['partner_affiliate_id'], 
                        when(((p["partner_id"] == af["partner_id"]) & (p["player_source_id"] == af["source_id"]) & (af["tipo_tabla"] == 'Afiliador') ), \
                            (when(p["fecha_registro"] >= af["fecha_contrato"], "Nuevo").otherwise("Fidelizado"))) \
                                .otherwise(None).alias("control_afiliador"),
                        when(((p["partner_id"] == af["partner_id"]) & (p["player_source_id"] == af["source_id"]) & (af["tipo_tabla"] == 'Afiliador')), "Es Afiliado") \
                            .otherwise(None).alias("es_afiliado"), 
                        af["nombre_affiliate"].alias("afiliador"), af["nombre_source"].alias("source"), 
                        af["Tipo_Afiliador"].alias("tipo_afiliador"), 
                        'relacion_diccionario_g_t_s'
                    )
                
    return players

# COMMAND ----------

# MAGIC %md
# MAGIC ### Migración GT MZA

# COMMAND ----------

def Create_Player_GT_Migra(u, c, s, cy, af, prn):
    u = u.withColumn("created_at", to_timestamp(expr("created_at - INTERVAL 3 HOURS"))).withColumn("updated_at", to_timestamp(expr("updated_at - INTERVAL 3 HOURS")))
    u = u.filter((col("local_register") == '0') & (col("is_backoffice") == '0') & (col("is_services") == '0'))
    u = u.withColumn("edad", floor(datediff(current_date(), u["birth_date"]) / 365.25))
    u = u.withColumn("mes", month("created_at"))
    u = u.withColumn("anio", year("created_at"))
    u = u.withColumn("camada_registro", concat(u["anio"], lit("-"), format_string("%02d", u["mes"])))

    p = u.join(c, c["id"] == u["city_id"], "left")\
                .join(s, s["id"] == c["state_id"], "left")\
                .join(cy, cy["id"] == s["country_id"], "left")\
                .join(prn, prn["id"] == u["bookmaker_id"], "left")\
                    .select(
                        prn["partner_id"].cast(LongType()).alias("partner_id"),
                        concat(prn['partner_id'], 
                               when(u['alira_id'].isNotNull(), u['alira_id'])
                               .when(u["bookmaker_id"].isin(88), concat(u["bookmaker_id"], format_string("%08d", u["id"])))
                               .otherwise(u["id"])).alias("partner_player_id"),
                        lit(3).cast(LongType()).alias("player_type_id"),
                        when(u['alira_id'].isNotNull(), u['alira_id']).otherwise(u["id"]).cast(LongType()).alias("player_id"),
                        u["id"].cast(LongType()).alias("GT_id"),
                        u["username"].alias("player_alias"),
                        lit(2).cast(LongType()).alias("player_status_id"),
                        lit(None).cast(LongType()).alias("player_source_id"),
                        when(u["referrer_reseller_id"].isNotNull(), concat(u["bookmaker_id"], lit('0'), format_string("%05d", u["referrer_reseller_id"])))
                               .otherwise(None).cast(LongType()).alias("partner_affiliate_source_id"),
                        u["referrer_reseller_id"],
                        when(u["middle_name"].isNotNull(), concat(u["first_name"], lit(' '), u["middle_name"])).otherwise(u["first_name"]).alias("nombre"),
                        u["last_name"].alias("apellido"),
                        u["birth_date"].cast(DateType()).alias("fecha_nacimiento"),
                        u["edad"],
                        when((u["edad"] >= 18) & (u["edad"] <= 25), '18 a 25')
                        .when((u["edad"] >= 26) & (u["edad"] <= 32), '26 a 32')
                        .when((u["edad"] >= 33) & (u["edad"] <= 40), '33 a 40')
                        .when((u["edad"] >= 41) & (u["edad"] <= 50), '41 a 50')
                        .when((u["edad"] >= 51) & (u["edad"] <= 60), '51 a 60')
                        .when((u["edad"] >= 61) & (u["edad"] <= 67), '61 a 67')
                        .when((u["edad"] <= 68), '68 o más')
                        .otherwise("No Aplica").alias("rango_edad"),
                        u["identification"].alias("DNI"),
                        when(u["gender_id"] == 1, lit(2)).when(u["gender_id"] == 2, lit(1)).cast(LongType()).alias("sexo"),
                        u["email"],
                        u["mobile_phone"].alias("phone"),
                        u["address1"].alias("address"),
                        u["postcode"].alias("zip_code"),
                        c["code"].alias("player_city"),
                        concat(u["bookmaker_id"], lit('u'), c["state_id"]).cast(LongType()).alias("partner_state_id"),
                        s["code"].alias("player_state"),
                        concat(u["bookmaker_id"], lit('0'), s["country_id"]).cast(LongType()).alias("partner_country_id"),
                        cy["code"].alias("player_country"),
                        lit(None).cast(DateType()).alias("fecha_exclusion"),
                        u["created_at"].alias("fecha_registro"),
                        u["camada_registro"],
                        datediff(current_date(), u["created_at"]).alias("dias_antiguedad"),
                        when(u["referrer_reseller_id"].isNotNull(), concat(u["bookmaker_id"], lit('0'), format_string("%05d", u["referrer_reseller_id"])))
                               .otherwise(None).cast(IntegerType()).alias("partner_affiliate_id"),
                        lit(None).cast(StringType()).alias("control_afiliador"),
                        concat(
                            when(u['alira_id'].isNotNull(), prn['partner_id'])
                               .otherwise(concat(u["bookmaker_id"], lit('0'))),
                            when(u["gender_id"] == 1, lit('2')).when(u["gender_id"] == 2, lit('1')).otherwise(u["gender_id"]), 
                            lit('03'), 
                            lit('02')
                        ).cast(LongType()).alias("relacion_diccionario_g_t_s")
                    )

    players = p.join(af, p["partner_affiliate_source_id"] == af["partner_affiliate_source_id"], "left")\
        .select(p['partner_id'], 
                p['partner_player_id'], 
                p['player_type_id'], 
                p['player_id'], 
                "GT_id", 'player_alias', 'player_status_id', 'player_source_id', 
                p["partner_affiliate_source_id"], 
                p['nombre'], 
                p['apellido'], 
                p['fecha_nacimiento'], 
                'edad', 
                'rango_edad', 
                'DNI', 'sexo', 'email', 'phone', 'address', 'zip_code', 'player_city', 'partner_state_id', 'player_state', 'partner_country_id', 'player_country', 'fecha_exclusion', 'fecha_registro', 'camada_registro', 'dias_antiguedad', 
                p['partner_affiliate_id'],
                # "referrer_reseller_id",
                 'control_afiliador', 
                when(((p["partner_affiliate_source_id"] == af["partner_affiliate_source_id"])), "Es Afiliado").otherwise(None).alias("es_afiliado"),
                af["nombre_affiliate"].alias("afiliador"),
                af["nombre_source"].alias("source"),
                af["Tipo_Afiliador"].alias("tipo_afiliador"),
                'relacion_diccionario_g_t_s')
    return players

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Calcular Tipo de Jugador y Fecha Ultima Apuesta

# COMMAND ----------

# MAGIC %md
# MAGIC ### Definición de la Función Calculate_Player_AyP
# MAGIC <p> Esta función toma como input variable el Dataframe creado "df_Player", obtiene el Deltatable "Apuestas y Premios", para luego calcular y agregar las columnas de "Tipo_jugador" y "Fecha_Ultima_Apuesta" al Dataframe.

# COMMAND ----------

def Calculate_Player_AyP(Player):
   # A partir de la tabla de Apuestas y Premios se obtendran las columnas
   # Tipo_Jugador
   # Fecha_Ultima_Apuesta
   # Categoria

   AyP = spark.read.table("entprod_mdn.bplay.apuestas_y_premios")

   # Filtrar por operation_type_id igual a 4 Apuestas
   Apuestas = AyP.filter(col("operation_type_id") == 4)
   # Filtrar Apuestas consolidadas
   Apuestas_consolidadas = Apuestas.filter(col("movement_amount") <= 0.00)

   # Agrupar por partner_id, partner_player_id y Service_legal_type y acumulado de movement_amount
   Apuestas_groupby = Apuestas_consolidadas.groupBy("partner_id", "partner_player_id", "Service_legal_type") \
                           .agg(sum(col("movement_amount")*-1).alias("movement_amount"))


   #### CALCULO CATEGORIA JUGADOR ####
      # Calcular las columnas de apuesta_CAS y apuesta_CAS
   AyP_Cat = Apuestas_groupby.withColumn("apuesta_CAS", when(col("Service_legal_type") == "Casino Vivo", col("movement_amount")).otherwise(0)) \
                           .withColumn("apuesta_AADD", when(col("Service_legal_type") == "Apuestas Deportivas", col("movement_amount")).otherwise(0)) \
                              .withColumn("apuesta_Slots", when(col("Service_legal_type") == "Slots", col("movement_amount")).otherwise(0))

   # Sumar cantidades por partner_id y partner_player_id
   AyP_Cat = AyP_Cat.groupBy("partner_id", "partner_player_id") \
            .agg(sum("apuesta_CAS").alias("apuesta_CAS"),
                  sum("apuesta_AADD").alias("apuesta_AADD"),
                  sum("apuesta_Slots").alias("apuesta_Slots"))\
                     .withColumn("apuesta_Total", col("apuesta_CAS") + col("apuesta_AADD") + col("apuesta_Slots"))                          

   ### Categoria ###
   # Calcular porcentajes y formatear como Decimal(5,2)
   AyP_Cat = AyP_Cat.withColumn("porcentaje_apuesta_CAS",
                              when(col("apuesta_Total") != 0, (col("apuesta_CAS") / col("apuesta_Total")) * 100)
                              .otherwise(0).cast(DecimalType(5, 2))) \
                  .withColumn("porcentaje_apuesta_AADD",
                              when(col("apuesta_Total") != 0, (col("apuesta_AADD") / col("apuesta_Total")) * 100)
                              .otherwise(0).cast(DecimalType(5, 2))) \
                  .withColumn("porcentaje_apuesta_Slots",
                              when(col("apuesta_Total") != 0, (col("apuesta_Slots") / col("apuesta_Total")) * 100)
                              .otherwise(0).cast(DecimalType(5, 2))) \
                  .withColumn("Categoria",
                              when((col("porcentaje_apuesta_AADD") >= 70.00), "AADD")
                              .when((col("porcentaje_apuesta_CAS") >= 70.00), "Casino Live")
                              .when((col("porcentaje_apuesta_Slots") >= 70.00), "Slots")
                              .when((col("porcentaje_apuesta_AADD") == 0.00) & (col("porcentaje_apuesta_CAS") == 0.00) & (col("porcentaje_apuesta_Slots") == 0.00), "Apuesta Rechazada")
                              .otherwise("Mixto")
                              ).select("partner_player_id", "Categoria", "porcentaje_apuesta_AADD", "porcentaje_apuesta_CAS", "porcentaje_apuesta_Slots", "apuesta_CAS", "apuesta_AADD", "apuesta_Slots", "apuesta_Total")
                              
   #### CALCULO TIPO JUGADOR ####
   AyP_TJ = Apuestas_groupby.withColumn("Service_legal_type",
                     when(col("Service_legal_type").isin("Slots", "Casino Vivo"), "Casino")
                     .otherwise(col("Service_legal_type")))
   
   # Calcular las columnas de apuesta_CAS y apuesta_CAS
   AyP_TJ = AyP_TJ.withColumn("apuesta_CAS", when(col("Service_legal_type") == "Casino", col("movement_amount")).otherwise(0)) \
         .withColumn("apuesta_AADD", when(col("Service_legal_type") == "Apuestas Deportivas", col("movement_amount")).otherwise(0))
   
   # Sumar cantidades por partner_id y partner_player_id
   AyP_TJ = AyP_TJ.groupBy("partner_id", "partner_player_id") \
            .agg(sum("apuesta_CAS").alias("apuesta_CAS"),
                  sum("apuesta_AADD").alias("apuesta_AADD"))\
                     .withColumn("apuesta_Total", col("apuesta_CAS") + col("apuesta_AADD"))

   ### Tipo_Jugador ###
   # Calcular porcentajes y formatear como Decimal(5,2)
   AyP_TJ = AyP_TJ.withColumn("porcentaje_apuesta_CAS",
                              when(col("apuesta_CAS") != 0, (col("apuesta_CAS") / col("apuesta_Total")) * 100)
                              .otherwise(0).cast(DecimalType(5, 2))) \
                  .withColumn("porcentaje_apuesta_AADD",
                              when(col("apuesta_AADD") != 0, (col("apuesta_AADD") / col("apuesta_Total")) * 100)
                              .otherwise(0).cast(DecimalType(5, 2))) \
                  .withColumn("Tipo_Jugador",
                              when((col("porcentaje_apuesta_AADD") == 100.00), "AADD Puro")
                              .when((col("porcentaje_apuesta_AADD") == 0.00) & (col("porcentaje_apuesta_CAS") == 100.00), "Casino Puro")
                              .when((col("porcentaje_apuesta_AADD") >= 70.00), "AADD Mixtos")
                              .when((col("porcentaje_apuesta_AADD") >= 30.00), "Mixto")
                              .when((col("porcentaje_apuesta_AADD") == 0.00) & (col("porcentaje_apuesta_CAS") == 0.00), "Apuesta Rechazada")
                              .when((col("porcentaje_apuesta_AADD").isNull()) & (col("porcentaje_apuesta_CAS").isNull()), "Error Apuesta")
                              .when((col("porcentaje_apuesta_AADD") <= 30.00), "Casino Mixto")
                              .otherwise("Revisar")
                              ).select("partner_player_id", "Tipo_Jugador")
                  
   ##### CALCULO PRIMERA APUESTA EFECTIVO #####
         # Filtrar Apuestas EFECTIVO
   Apuestas_efectivo = Apuestas.filter(col("balance_type_id") == 1)
         # Definir la ventana
   windowSpec = Window.partitionBy("partner_player_id").orderBy("fecha_operacion")
         # Agregar columna con el número de fila
   apuestas_with_row_number = Apuestas_efectivo.withColumn("row_num", row_number().over(windowSpec))

   # Filtrar la primera fila de cada partición
   primera_apuesta_efectivo = apuestas_with_row_number.filter(col("row_num") == 1).select(
      "partner_player_id",
      col("Service_legal_type").alias("primera_apuesta_ef"), 
      col("fecha_operacion").alias("fecha_primera_apuesta_ef")
      )

   ##### CALCULO PRIMERA APUESTA ABSOLUTO #####
   Apuestas_absoluto = Apuestas.filter(col("movement_amount") <= 0.00)
         # Definir la ventana
   windowSpec = Window.partitionBy("partner_player_id").orderBy("fecha_operacion")
         # Agregar columna con el número de fila
   apuestas_with_row_number = Apuestas_absoluto.withColumn("row_num", row_number().over(windowSpec))

   # Filtrar la primera fila de cada partición
   primera_apuesta_absoluta = apuestas_with_row_number.filter(col("row_num") == 1).select(
      "partner_player_id",
      col("Service_legal_type").alias("primera_apuesta_abs"), 
      col("fecha_operacion").alias("fecha_primera_apuesta_abs")
      )
   
   ##### CALCULO DE COLUMNA FECHA_ULTIMA_APUESTA #####
   ultima_apuesta_x_player = Apuestas.groupBy("partner_player_id").agg(max(col("fecha_operacion")).alias("fecha_ultima_apuesta"))
   
      ##### JOIN TIPO JUGADOR ######                     
   Player_tp = Player.join(AyP_TJ, "partner_player_id", "left")

      ##### JOIN CATEGORIA ###### 
   Player_tp_c = Player_tp.join(AyP_Cat, "partner_player_id", "left")

      ##### JOIN FECHA_ULTIMA_APUESTA ######                                           
   Player_res = Player_tp_c.join(ultima_apuesta_x_player, "partner_player_id", "left")

      ##### JOIN PRIMERA APUESTA EFECTIVO ######                                           
   Player_res = Player_res.join(primera_apuesta_efectivo, "partner_player_id", "left")

      ##### JOIN PRIMERA APUESTA ABSOLUTA ######                                           
   Player_res = Player_res.join(primera_apuesta_absoluta, "partner_player_id", "left")\
         .withColumn("Categoria", \
            when(col("Categoria").isNull(), "Sin Apuesta")
            .when(col("primera_apuesta_ef").isNull() & col("primera_apuesta_abs").isNotNull() & (col("porcentaje_apuesta_AADD") == 0.00) & (col("porcentaje_apuesta_CAS") == 0.00), "Sin Apuesta Efectivo")\
               .otherwise(col("Categoria")))\
         .withColumn("Tipo_Jugador", \
            when(col("Tipo_Jugador").isNull(), "Sin Apuesta")\
               .when((col("primera_apuesta_ef").isNull() & col("primera_apuesta_abs").isNotNull()) & (col("porcentaje_apuesta_AADD") == 0.00) & (col("porcentaje_apuesta_CAS") == 0.00), "Sin Apuesta Efectivo")\
               .otherwise(col("Tipo_Jugador")))
         
   # print("total p player_tipoJugador")
   # print(Player_tp.filter(col("player_type_id") == 3).count())

      ##### SELECT PLAYERS #####         
   Player_res = Player_res.select("partner_player_id", "partner_id", "player_type_id", "player_id", "GT_id", "player_alias", "player_status_id", "player_source_id", "partner_affiliate_source_id", "nombre", "apellido", "fecha_nacimiento", "edad", "rango_edad", "DNI", "sexo", "email", "phone", "address", "zip_code", "player_city", "partner_state_id", "player_state", "partner_country_id", "player_country", "fecha_exclusion", "fecha_registro", "camada_registro",  "partner_affiliate_id", "control_afiliador", "es_afiliado", "afiliador", "source", "tipo_afiliador", "relacion_diccionario_g_t_s", "fecha_ultima_apuesta",  "Tipo_Jugador", "Categoria", "primera_apuesta_ef", "fecha_primera_apuesta_ef", "primera_apuesta_abs", "fecha_primera_apuesta_abs", "porcentaje_apuesta_AADD", "porcentaje_apuesta_CAS", "porcentaje_apuesta_Slots", "apuesta_CAS", "apuesta_AADD", "apuesta_Slots", "apuesta_Total")
   
   return Player_res

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Calcular Fecha Ultimo Deposito y Fecha Ultimo Retiro

# COMMAND ----------

# MAGIC %md
# MAGIC ### Definición de la Función Calculate_Player_DyR
# MAGIC <p> Esta función toma como input variable el Dataframe creado "df_Player", obtiene el Deltatable "Depositos y Retiros", para luego calcular y agregar las columnas de "Fecha_Ultimo_Deposito" y "Fecha_Ultimo_Retiro" al Dataframe.

# COMMAND ----------

# import pyspark.sql.functions as F
# DyP = spark.read.table("entprod_mdn.bplay.depositos_y_retiros")

# ## Fecha_Ultima_Deposito ##
# depositos = DyP.filter((col("transaction_type_id") == 1) & (col('transaction_status_id') == 2))

# ftds = depositos.filter(col('partner_transaction_id') == col('FTD_partner_transaction_id'))\
#         .select('partner_player_id', date_format('FTD_approval_date', 'yyyy-MM').alias('Camada_deposito')).distinct()

# display(ftds.groupBy(date_format(col('Camada_deposito'), 'yyyy-MM').alias('camada_ftd')).agg(F.countDistinct('partner_player_id')).alias('count'))


# COMMAND ----------

def Calculate_Player_DyR(Players):
    # A partir de la tabla de Apuestas y Premios se obtendran las columnas
        # Fecha_Ultima_Deposito
        # Fecha_Ultima_Retiro
        # Camada_deposito

    DyP = spark.read.table("entprod_mdn.bplay.depositos_y_retiros")
    #DyP = spark.read.table("entprod_mdn.default.depositos_y_retiros")


    ## Fecha_Ultima_Deposito ##
    depositos = DyP.filter((col("transaction_type_id") == 1) & (col('transaction_status_id') == 2))

    ultimo_deposito_x_player = depositos.select("transaction_approval_date", "partner_player_id")\
                                        .groupBy("partner_player_id").agg(max(col("transaction_approval_date")).alias("fecha_ultimo_deposito"))
    
    ## Fecha_Ultima_Retiro ##
    retiros = DyP.filter(col("transaction_type_id") == 2)

    ultimo_retiro_x_player = retiros.select(col("transaction_sent_date"), col("partner_player_id"))\
                                    .groupBy("partner_player_id").agg(max(col("transaction_sent_date")).alias("fecha_ultimo_retiro"))
                                                     
    Players_udp = Players.join(ultimo_deposito_x_player, "partner_player_id", "left").distinct()

    Players_udp_urp = Players_udp.join(ultimo_retiro_x_player, "partner_player_id", "left").distinct()

    ## Camada_deposito ##
    ftds = depositos.filter(col('partner_transaction_id') == col('FTD_partner_transaction_id'))\
        .select('partner_player_id', date_format('FTD_approval_date', 'yyyy-MM').alias('Camada_deposito')).distinct()
    # ftds = depositos.groupBy("partner_player_id").agg(min(col("FTD_approval_date")).alias("FTD_approval_date"))\
    #     .select('partner_player_id', date_format('FTD_approval_date', 'yyyy-MM').alias('Camada_deposito')).distinct()

    res = Players_udp_urp.join(ftds, "partner_player_id", "left").select(Players_udp_urp['partner_player_id'], "partner_id", "player_type_id", "player_id", "GT_id", "player_alias", "player_status_id", "player_source_id", "partner_affiliate_source_id", "nombre", "apellido", "fecha_nacimiento", "edad", "rango_edad", "DNI", "sexo", "email", "phone", "address", "zip_code", "player_city", "partner_state_id", "player_state", "partner_country_id", "player_country", "fecha_exclusion", "fecha_registro", "camada_registro",  "partner_affiliate_id", "control_afiliador", "es_afiliado", "afiliador", "source", "tipo_afiliador", "relacion_diccionario_g_t_s", 'Camada_deposito', "fecha_ultima_apuesta", "fecha_ultimo_deposito", "fecha_ultimo_retiro","Tipo_Jugador", "Categoria", "primera_apuesta_ef", "fecha_primera_apuesta_ef", "primera_apuesta_abs", "fecha_primera_apuesta_abs")

    return res

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Calcular Fecha Ultima Conexion

# COMMAND ----------

# MAGIC %md
# MAGIC ### Definición de la Función Calculate_Player_Sesion
# MAGIC <p> Esta función toma como input variable el Dataframe creado "df_Player", obtiene el Deltatable "Sesion", para luego calcular y agregar las columnas de "Fecha_Ultima_Sesion" al Dataframe.

# COMMAND ----------

def Calculate_Player_Sesion(Player):
    #A partir de la tabla de Sesion se obtendran las columnas
            # Fecha_Ultima_Sesion

    df_Sessions = spark.read.table("entprod_mdn.bplay.sesion")

        # Sessions to DataFrame
    Sessions = df_Sessions.select("partner_player_id", col("ultima_conexion").alias("fecha_ultima_conexion"))

        # Join
    Player_S = Player.join(Sessions, "partner_player_id", "left").select(
        "partner_player_id", 
        col("partner_id").cast(LongType()), 
        col("player_type_id").cast(LongType()), 
        col("player_id"), 
        "GT_id",
        "player_alias", 
        col("player_status_id").cast(LongType()), 
        col("player_source_id").cast(LongType()), 
        col("partner_affiliate_source_id").cast(LongType()), 
        "nombre", 
        "apellido", 
        "fecha_nacimiento", 
        "edad", 
        "rango_edad", 
        "DNI", 
        "sexo", 
        "email", 
        "phone", 
        "address", 
        "zip_code", 
        "player_city", 
        "partner_state_id", 
        "player_state", 
        "partner_country_id", 
        "player_country", 
        "fecha_exclusion", 
        "fecha_registro", 
        "camada_registro", 
        #"dias_antiguedad", 
        "partner_affiliate_id", 
        "control_afiliador", 
        "es_afiliado", 
        "afiliador", 
        "source", 
        "tipo_afiliador", 
        col("relacion_diccionario_g_t_s").cast(LongType()), 
        'Camada_deposito', 
        "fecha_ultima_conexion", 
        "fecha_ultima_apuesta", 
        "fecha_ultimo_deposito", 
        "fecha_ultimo_retiro", 
        "Tipo_Jugador", 
        "Categoria", 
        "VIP", 
        "primera_apuesta_ef", 
        "fecha_primera_apuesta_ef", 
        "primera_apuesta_abs", 
        "fecha_primera_apuesta_abs")
    return Player_S

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Obtener Players VIPs

# COMMAND ----------

# MAGIC %md
# MAGIC ### Definición de la Función Calculate_Player_VIP
# MAGIC <p> Esta función toma como input variable el Dataframe creado "df_Player", obtiene el documento csv "VIPs", para luego calcular y agregar las columnas de segmentacions "VIP" al Dataframe.

# COMMAND ----------

def create_Player_VIP(vip, players):
    # Paso 1: Se unen los jugadores con los grupos VIP
    result = players.join(vip, players['partner_player_id'] == concat(vip['Instancia'], vip['ID']), how='left').select(
    *players.columns,
    when(vip['Segmento'].isNull(), None).otherwise(vip['Segmento']).alias('VIP')
)
    return result

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ejecución del DataFrame Auxiliar Dim Room por Base de Datos: DWBPLAY_PY, DWBPLAY_SF, DWBPLAY_CABA y DWBPLAY_CORDOBA.
# MAGIC <p> Se ejecuta la función Create_DF_Aux_Dim_Room para obtener el dataframe con la información de las verticales de la sala en las bases de datos DWBPLAY_PY, DWBPLAY_SF, DWBPLAY_CABA y DWBPLAY_CORDOBA.

# COMMAND ----------

# DWBPLAY_PY
df_player0 = Create_Player(df_p0, df_s0, df_c0, df_source)
# DWBPLAY_SF
df_player1 = Create_Player(df_p1, df_s1, df_c1, df_source)
# DWBPLAY_CABA
df_player2 = Create_Player(df_p2, df_s2, df_c2, df_source)
# DWBPLAY_CORDOBA
df_player3 = Create_Player(df_p3, df_s3, df_c3, df_source)

if bl_Carga_GT:
    # BPLAYBET_ARG
    df_players46_GT = Create_Player_GT(df_usr46, df_cts46, df_sts46, df_ctr46, df_source, df_partner_Migra)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Actualización de Jugadores GT MZA

# COMMAND ----------

# Paso 1: Select Jugadores MZA
df_p88 = df_players46_GT.filter(col('partner_id') == 256)

##
df_player_MB_S256 = df_player2.filter(~(col('partner_id') == 256))
##
df_player256 = df_player2.filter(col('partner_id') == 256)


# Paso 2: Jugadores Previos a la Migracion Join Columna GT_id
df_player_256 = df_player256.join(df_p88, 'partner_player_id', 'left').select(
    df_player2["partner_id"], df_player2["partner_player_id"], df_player2["player_id"], df_p88['GT_id'], 
    when(df_p88["player_alias"].isNull(), df_player2["player_alias"]).otherwise(df_p88["player_alias"]).alias('player_alias'),
    df_player2["player_status_id"],   # REVISAR VALORES PROVENIENTES DE GT
    # df_player2["player_type_id"],   # REVISAR VALORES PROVENIENTES DE GT
    when(df_p88["player_type_id"].isNull(), df_player2["player_type_id"]).otherwise(df_p88["player_type_id"]).alias("player_type_id"),
    df_player2["player_source_id"], df_player2["partner_affiliate_source_id"], df_player2["nombre"], df_player2["apellido"],
    df_player2["fecha_nacimiento"], df_player2["edad"], df_player2["rango_edad"], df_player2["DNI"], df_player2["sexo"], 
    when(df_p88["email"].isNull(), df_player2["email"]).otherwise(df_p88["email"]).alias('email'), 
    when(df_p88["phone"].isNull(), df_player2["phone"]).otherwise(df_p88["phone"]).alias('phone'), 
    when(df_p88["address"].isNull(), df_player2["address"]).otherwise(df_p88["address"]).alias('address'), 
    df_p88["zip_code"], df_player2["player_city"], df_player2["partner_state_id"], df_player2["player_state"], df_player2["partner_country_id"], df_player2["player_country"], df_p88["fecha_exclusion"], df_player2["fecha_registro"], df_player2["camada_registro"], df_player2["dias_antiguedad"], df_player2["partner_affiliate_id"], df_player2["control_afiliador"], df_player2["es_afiliado"], df_player2["afiliador"], df_player2["source"], df_player2["tipo_afiliador"], df_player2["relacion_diccionario_g_t_s"]
    )

# Paso 3: Filtrar los jugadores nuevos (los que no están en df_player_2)
nuevos_jugadores = df_players46_GT.join(df_player_256, on="partner_player_id", how="anti")

# Paso 4: Unir los jugadores históricos actualizados con los nuevos
df_player_Multiboldt =  (df_player_256.unionByName(df_player_MB_S256)).unionByName(nuevos_jugadores)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Filtro last day

# COMMAND ----------

# MAGIC %md
# MAGIC ### Definición de la Función filter_last_day
# MAGIC <p> Esta función Filtra hasta las  23:59:59 del día anterior a la ejecución.

# COMMAND ----------

def filter_last_day(df): # Filtra hasta las  23:59:59 del día anterior a la ejecución
    yesterday_end = concat(date_sub(current_timestamp(), 1).cast("string"),lit(" 23:59:59")).cast("timestamp")
    result = df.filter(col("fecha_registro") <= yesterday_end)
    return result

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Upsert de Tabla Delta "Player"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Obtención de la Última Tabla Generada para Actualización
# MAGIC <p> Si ya existe la Delta Table de Player creada se obtendra la tabla y se actualizará con el DataFrame generado en esta ejecución.
# MAGIC <p> Si no existiera, se definirá la limitación las particiones en 8, y se generará un nuevo Delta Table de "Player".

# COMMAND ----------

# Comprobación de variables
print(delta_table_Player)
print(exist)
print('Se ejecutará en PRD' if PROD else 'Se ejecutará en DEV')


# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Union y Ejecución de Funciones de DF Player
# MAGIC <p> Se carga la tabla actualizada en el Warehouse de Databricks para su posterior utilización o consumo.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Union df_Players

# COMMAND ----------

# ALIRA & GT
df_Players = df_player0.unionByName(df_player1).unionByName(df_player_Multiboldt).unionByName(df_player3).unionByName(df_players46_GT.filter(~(col('partner_id').isin(256))))

# COMMAND ----------

from pyspark.sql.functions import count
display(df_Players.groupBy('partner_id', 'rango_edad').agg(count('partner_player_id').alias('cant_p')))

display(df_Players.filter(col('rango_edad') == 'No Aplica'))

# COMMAND ----------

# # u = u.filter((col("local_register") == '0') & (col("is_backoffice") == '0') & (col("is_services") == '0'))
# display(df_usr46.filter(col('alira_id') == '2776611').select('id', 'alira_id', 'username', 'email', "local_register", "is_backoffice", "is_services"))
# # display(df_player_Multiboldt.filter(col('player_id') == '2776611'))
# # display(df_players46_GT.filter(col('player_id') == '2776611'))
# display(df_usr46.filter(col('alira_id') == '2776611'))
# display(df_p2.filter(col('id') == '2776611'))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ejecucion de Funciones

# COMMAND ----------

# Se obtiene las columnas de Apuestas y Premios
df_Player_AyP = Calculate_Player_AyP(df_Players)

# Se obtiene las columnas de Depositos y Retiros
df_Player_AyP_DyR = Calculate_Player_DyR(df_Player_AyP)

# Se obtiene las columna de Jugadores VIP
df_Player_AyP_DyR_VIP = create_Player_VIP(df_vips, df_Player_AyP_DyR)

# Se obtiene las columnas de Sessions
df_Player_AyP_DyR_VIP_S = Calculate_Player_Sesion(df_Player_AyP_DyR_VIP)

# Se agregan la columna de "fecha_acualizacion"
df_Player_AyP_DyR_S_VIP_FA = df_Player_AyP_DyR_VIP_S.withColumn("fecha_actualizacion", current_timestamp())

# Se Filtra los players que se registraron en el día de la ejecución hasta las 23:59:59
df_Player_f = filter_last_day(df_Player_AyP_DyR_S_VIP_FA)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Carga de Tabla Delta Player

# COMMAND ----------

print(delta_table_Player)

# COMMAND ----------

# from pyspark.sql.functions import count
# display(df_Player_f.groupBy('partner_player_id').agg(count('*').alias('count')).filter(col('count') > 1))

# COMMAND ----------

from pyspark.sql.functions import count
df_p_duplicados = df_Player_f.groupBy('partner_player_id').agg(count('*').alias('count')).filter(col('count') > 1).select('partner_player_id')

# Paso 2: Extraer los valores duplicados como una lista
ids_duplicados = [row["partner_player_id"] for row in df_p_duplicados.collect()]
print(ids_duplicados)
#display(df_p_duplicados)

# Paso 3: Filtrar df_Salas para excluir los duplicados
df_Player_f_sin_duplicads = df_Player_f.filter(~(col("partner_player_id").isin(ids_duplicados)))

# COMMAND ----------

# Limitar a 8 particiones el DF "Player"
df_Player = df_Player_f.coalesce(8)

# Carga de datos a la tabla Player
df_Player.write \
    .mode("overwrite")\
        .option("overwriteSchema", "true")\
            .saveAsTable(delta_table_Player)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Diccionario Players

# COMMAND ----------

## Diccionario Player
  #Get DWBPLAY_PY
df_ps0 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_py/OGP/player_s_name')
df_pt0 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_py/OGP/player_t_name')
df_g0 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_py/OGP/gender_name')
df_pn0 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_py/OGP/partner')
  #Get DWBPLAY_SF
df_ps1 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_sf/OGP/player_s_name')
df_pt1 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_sf/OGP/player_t_name')
df_g1 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_sf/OGP/gender_name')
df_pn1 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_sf/OGP/partner')
  #Get DWBPLAY_CABA
df_ps2 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_caba/OGP/player_s_name')
df_pt2 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_caba/OGP/player_t_name')
df_g2 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_caba/OGP/gender_name')
df_pn2 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_caba/OGP/partner')
  #Get DWBPLAY_CORDOBA
df_ps3 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_cordoba/OGP/player_s_name')
df_pt3 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_cordoba/OGP/player_t_name')
df_g3 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_cordoba/OGP/gender_name')
df_pn3 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_cordoba/OGP/partner')

# COMMAND ----------

def Diccionario_Player(ps, pt, g, pn):
    # Aplicar Filtros Language 2
    ps = ps.filter(col("language") == 2)
    pt = pt.filter(col("language") == 2)
    g = g.filter(col("language") == 2)

    # Convert partner to string if it's an integer
    partner = col("partner").cast("string")

    # Genera Diccionario Player
    dic_players = ps.crossJoin(pt).crossJoin(g).crossJoin(pn) \
        .select(
            g["gender"],
            g["name"].alias("sexo"),
            pt["type"],
            pt["name"].alias("player_type"),
            ps["status"],
            ps["name"].alias("player_status"),
            concat(
                pn["id"],
                g["gender"],
                format_string("%02d", pt["type"]),
                format_string("%02d", ps["status"])
            ).cast(LongType()).alias("relacion_player_g_t_s")
        )
    return dic_players

# COMMAND ----------

def Diccionario_Player_GT(df):
    columns = ["gender", "sexo", "type", "player_type", "status", "player_status", "relacion_player_g_t_s"]
    data	= [("1", "Hombre", "03", "Jugador", "02", "Activo", "46010302"), 
                ("2", "Mujer", "03", "Jugador", "02", "Activo", "46020302"), 
                ("3", "No binario", "03", "Jugador", "02", "Activo", "46030302"),
                ("1", "Hombre", "03", "Jugador", "02", "Activo", "13010302"), 
                ("2", "Mujer", "03", "Jugador", "02", "Activo", "13020302"), 
                ("3", "No binario", "03", "Jugador", "02", "Activo", "13030302"),
                ("1", "Hombre", "01", "Empleado", "02", "Activo", "46010302"), 
                ("2", "Mujer", "01", "Empleado", "02", "Activo", "46020302"), 
                ("3", "No binario", "01", "Empleado", "02", "Activo", "46030302"),
                ("1", "Hombre", "01", "Empleado", "02", "Activo", "13010302"), 
                ("2", "Mujer", "01", "Empleado", "02", "Activo", "13020302"), 
                ("3", "No binario", "01", "Empleado", "02", "Activo", "13030302")]
    result = df.unionByName(spark.createDataFrame(data, columns)).select(
        col("gender").cast("bigint"),
        "sexo",
        col("type").cast("bigint"),
        "player_type",
        col("status").cast("bigint"),
        "player_status",
        col("relacion_player_g_t_s").cast("bigint")        
    )
    return result

# COMMAND ----------

# DWBPLAY_PY
df_Diccionario_Player0 = Diccionario_Player(df_ps0, df_pt0, df_g0, df_pn0)
# DWBPLAY_SF
df_Diccionario_Player1 = Diccionario_Player(df_ps1, df_pt1, df_g1, df_pn1)
# DWBPLAY_CABA
df_Diccionario_Player2 = Diccionario_Player(df_ps2, df_pt2, df_g2, df_pn2)
# DWBPLAY_CORDOBA
df_Diccionario_Player3 = Diccionario_Player(df_ps3, df_pt3, df_g3, df_pn3)

# Alira Player Diccionario
df_diccionario_players_alira = df_Diccionario_Player0.unionByName(df_Diccionario_Player1).unionByName(df_Diccionario_Player2).unionByName(df_Diccionario_Player3)

# COMMAND ----------

df_diccionario_players = Diccionario_Player_GT(df_diccionario_players_alira)

# COMMAND ----------

# path_player = 'entprod_mdn.bplay.diccionario_player'

# df_diccionario_players.write \
#     .mode("overwrite")\
#         .option("overwriteSchema", "true")\
#             .saveAsTable(path_player)