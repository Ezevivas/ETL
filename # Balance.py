# Databricks notebook source
# MAGIC %md
# MAGIC # Balance_MDN-bplay_PySpark
# MAGIC <h> by PySpark
# MAGIC #### Estado: **OK**
# MAGIC <p> Source: Se realizará la lectura de archivos en formato parquet para obtener los dataframes necesarios para generar la tabla 'Balance', y cargarla en formato Delta Table para su posterior consulta.
# MAGIC <p> Se tomará la información desde el STG1 de Datalake para generar un consumible de STG2 en formato Delta Table.
# MAGIC <p> Inputs:
# MAGIC   <ol>
# MAGIC     <li>balance</li>
# MAGIC     <li>player</li>
# MAGIC     <li>balance t name</li>
# MAGIC     <li>currency</li>
# MAGIC   </ol>
# MAGIC <p> Output: Delta Table actualizado de "Balance" y su carga en el Warehouse de Databricks.

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

from pyspark.sql.functions import concat, max, col, when, format_string, lit, current_timestamp, to_timestamp
from pyspark.sql.types import DecimalType, LongType, IntegerType
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %md
# MAGIC ### Nomenclatura de Fuentes y Auxiliares
# MAGIC <ul>
# MAGIC   <li>df = dataframe</li>
# MAGIC   <li>b = balance</li>
# MAGIC   <li>p = player</li>
# MAGIC   <li>bn = balance t name</li>
# MAGIC   <li>c = currency</li>
# MAGIC   <li>0 = DWBPLAY_PY</li>
# MAGIC   <li>1 = DWBPLAY_SF</li>
# MAGIC   <li>2 = DWBPLAY_CABA</li>
# MAGIC   <li>3 = DWBPLAY_CORDOBA</li>
# MAGIC   <li>B = Balance
# MAGIC </ul>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Conexion de Source y Obtención de DataFrames
# MAGIC <p> Descripción: 
# MAGIC <ol>
# MAGIC   <li>Establece la conexión al STG1 del Data Lake.</li>
# MAGIC   <li>Se definen los paths de los archivos parquet "Balance", "Player", "Balance t name" y "Currency" para cada Base de Datos DWBPLAY_PY, DWBPLAY_SF, DWBPLAY_CABA y DWBPLAY_CORDOBA.</li>
# MAGIC   <li> Luego se leen los archivos parquet para crear los dataframes en formato PySpark.</li>
# MAGIC </ol>

# COMMAND ----------

  # Get Balance
df_b0 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_py/OGP/balance')
df_b1 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_sf/OGP/balance')
df_b2 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_caba/OGP/balance')
df_b3 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_cordoba/OGP/balance')
  # Get Balance t name
df_bn0 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_py/OGP/balance_t_name')
df_bn1 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_sf/OGP/balance_t_name')
df_bn2 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_caba/OGP/balance_t_name')
df_bn3 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_cordoba/OGP/balance_t_name')
  # Get Currency
df_c0 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_py/OGP/currency')
df_c1 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_sf/OGP/currency')
df_c2 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_caba/OGP/currency')
df_c3 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_cordoba/OGP/currency')
  # Get Player
df_p0 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_py/OGP/player')
df_p1 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_sf/OGP/player')
df_p2 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_caba/OGP/player')\
                  .filter(col("partner").isin(252,253,254))
df_p3 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_cordoba/OGP/player')


# COMMAND ----------

try:
    # Get wallets
  df_w = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/bplaybet_arg/dbo/wallets')\
                    .filter(col('wallet_type_id') == 7)\
                      .select('id', 'user_id', 'currency_id', col('balance').cast(DecimalType(15,2)).alias("balance"))
    # Get users
  df_u = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/bplaybet_arg/dbo/users')\
                    .filter(col('local_register') == 0).filter(col('is_services') == 0).filter(col('is_backoffice') == 0)\
                      .select('id', 'bookmaker_id', 'alira_id')
    ### Switch Carga GT Parana & Jujuy
  bl_Carga_GT = True

except Exception as e:
  print(f"No se pudieron leer las tablas de balance GT: {e}")
  bl_Carga_GT = False

# COMMAND ----------

display(df_u.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Switch PROD/DEV

# COMMAND ----------

#   Variable PROD/DEV
PROD = dbutils.widgets.get("run_job")
print(f"El parametro de PROD es: {PROD}")

PROD = PROD == "True"
print(type(PROD))

# COMMAND ----------

#Path DEV del Delta Table Eventos
delta_table_Balance_dev = 'entprod_mdn.default.balance'

#Path PRD del Delta Table Eventos
delta_table_Balance_prd = 'entprod_mdn.bplay.balance'

# #Variable PROD/DEV
# PROD = True

# Switch PROD/DEV
if PROD:
    delta_table_Balance = delta_table_Balance_prd
else:
    delta_table_Balance = delta_table_Balance_dev

# Manejar el caso en que la tabla no exista
try:
    # Intentar leer la tabla Delta
    df_origen = spark.read.table(delta_table_Balance)
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

# Definir los datos
# data = [(46, "460"), (13, "130"), (88, "25688")]
data = [(46, "460"), (13, "130"), (88, "256")]

# Definir los nombres de las columnas
columns = ["id", "partner"]

# Crear el DataFrame
df_partner_Migra = spark.createDataFrame(data, columns)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. DF Create Balance
# MAGIC <p> Se genera un DataFrame de Balance con las columnas necesarias.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Definición de la Función Create_Balance Alira / Tecnalis / GiG
# MAGIC <p> Esta función genera un dataframe con la información necesaria

# COMMAND ----------

def Create_Balance(b, p, bn, c):
    p = p.filter(col("status") != 1)
    bn = bn.filter(col("language") == 2)
    b = b.filter(col("balance_type").isin(1,8,42,43,44))

    Balance = b.join(p, b["player"] == p["id"])\
                    .join(bn, b["balance_type"] == bn["type"])\
                        .join(c, b["currency"] == c["id"])\
                            .select(
                                concat(p["partner"], p["id"].cast(DecimalType(10,0)), b["balance_type"], b["currency"]).alias("id"),
                                concat(p["partner"], p["id"].cast(DecimalType(10,0))).alias("partner_player_id"),
                                b["balance_type"].cast(IntegerType()).alias("balance_type_id"),
                                b["currency"].cast(IntegerType()).alias("currency_id"),
                                b["amount"].cast(DecimalType(25, 2)).alias("balance_amount"),
                                concat(
                                    format_string('%02d',  lit(4)),
                                    format_string('%02d', b["balance_type"]),
                                    format_string('%02d', b["currency"])
                                ).alias("relacion_operation_t_t_c")
                            ).distinct()
    return Balance

# COMMAND ----------

# MAGIC %md
# MAGIC ### Definición de la Función Create_Balance bplaybet
# MAGIC <p> Esta función genera un dataframe con la información necesaria

# COMMAND ----------

def Create_Balance_GT(u, w, p):

    Balance = w.join(u, u["id"] == w["user_id"])\
                    .join(p, p["id"] == u["bookmaker_id"])\
                            .select(
                                concat(p["partner"], 
                                       u["id"].cast(DecimalType(10,0)), 
                                       lit(1), w["currency_id"]).alias("id"),
                                concat(p["partner"], 
                                       when(u["alira_id"].isNotNull(), u.alira_id.cast(LongType()))
                                        .when(u["bookmaker_id"].isin(88), 
                                              concat(u["bookmaker_id"], format_string("%08d", u["id"])))
                                            .otherwise(u["id"])).alias("partner_player_id"),
                                lit(1).alias("balance_type_id"),
                                when(w["currency_id"] == 41, lit(7))
                                    .when(w["currency_id"] == 8, lit(9))
                                    .when(w["currency_id"] == 2, lit(4))
                                    .otherwise(0).cast(IntegerType()).alias("currency_id"),
                                w["balance"].cast(DecimalType(25, 2)).alias("balance_amount"),
                                concat(
                                    format_string('%02d',  lit(4)),
                                    format_string('%02d', col("balance_type_id")),
                                    format_string('%02d', 
                                                  when(w["currency_id"] == 41, lit(7))
                                                    .when(w["currency_id"] == 8, lit(9))
                                                    .when(w["currency_id"] == 2, lit(4))
                                                    .otherwise(0).cast(IntegerType()))
                                ).alias("relacion_operation_t_t_c")
                            ).distinct()
    return Balance

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ejecución del DataFrame para obtener el Balance por Base de Datos: DWBPLAY_PY, DWBPLAY_SF, DWBPLAY_CABA  y DWBPLAY_CORDOBA.
# MAGIC <p> Se ejecuta la función Create_Balance para obtener el dataframe de 'Balance' en las bases de datos DWBPLAY_PY, DWBPLAY_SF, DWBPLAY_CABA  y DWBPLAY_CORDOBA.

# COMMAND ----------

# DWBPLAY_PY
df_B0 = Create_Balance(df_b0, df_p0, df_bn0, df_c0)
# DWBPLAY_SF
df_B1 = Create_Balance(df_b1, df_p1, df_bn1, df_c1)
# DWBPLAY_CABA
df_B2 = Create_Balance(df_b2, df_p2, df_bn2, df_c2)
# DWBPLAY_CORDOBA
df_B3 = Create_Balance(df_b3, df_p3, df_bn3, df_c3)

if bl_Carga_GT:
    df_Balance_GT = Create_Balance_GT(df_u, df_w, df_partner_Migra)

# COMMAND ----------

'''
# Se decidió filtrar en caba los partners que siguen válidos en Alira. Por ésto último, no hace falta hacer merge. 
def Create_merge(df_balance_alira, df_balance_gt):

    # Realizar un FULL OUTER JOIN en el ID
    merged_df = df_balance_alira.alias("dfba").join(
        df_balance_gt.alias("dfbgt"), 
        on="partner_player_id", 
        how="outer"
    )

    # Si se tuviera que modificar algún registro de algún dataframe se puede adaptar el siguiente párrafo
    # Concatenar emails si el ID existe en ambos DataFrames
    # updated_email = when(
    #     col("dfa.mail").isNotNull() & col("dfsa.mail").isNotNull(), 
    #     concat_ws(", ", col("dfa.mail"), col("dfsa.mail"))
    # ).when(col("dfa.mail").isNotNull(), col("dfa.mail")
    # ).otherwise(col("dfsa.mail"))

    # Mantener los valores originales de dfbgt para el resto de las columnas
    final_df = merged_df.select(
        col("partner_player_id"),
        # Si se tuviera que modificar algún registro de algún dataframe se puede adaptar la siguiente línea
        # updated_email.alias("mail"),
        coalesce(col("dfbgt.balance_type_id"), col("dfba.balance_type_id")).alias("balance_type_id"),
        coalesce(col("dfbgt.currency_id"), col("dfba.currency_id")).alias("currency_id"),
        coalesce(col("dfbgt.balance_amount"), col("dfba.balance_amount")).alias("balance_amount"),
        coalesce(col("dfbgt.relacion_operation_t_t_c"), col("dfba.relacion_operation_t_t_c")).alias("relacion_operation_t_t_c")
    )

    return final_df
    '''

# COMMAND ----------

# MAGIC %md
# MAGIC ### Union de DF Balance
# MAGIC <p> Se realiza la unificacion de las ejecuciones de las bases de datos DWBPLAY_PY, DWBPLAY_SF, DWBPLAY_CABA y DWBPLAY_CORDOBA.

# COMMAND ----------

if bl_Carga_GT:
    # Uno los balances con GT
    df_Balance = (df_B0.union(df_B1)).union(df_B2.union(df_B3)).union(df_Balance_GT)
else:
    df_Balance = (df_B0.union(df_B1)).union(df_B2.union(df_B3))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Get amount_USD

# COMMAND ----------

def get_USDamount(balance):
    # Get Tipo de Cambio
    tc = spark.read.table("entprod_mdn.bplay.tipos_de_cambio")

    # Get ultima Cotización
    maxFecha = tc.agg(max(col("Fecha")).alias("Fecha"))
    ultimaCot = maxFecha.join(tc, "Fecha", "left").select("ARS_CCL", "ARS", "BRL", "PYG")

    result = balance.crossJoin(ultimaCot).withColumn("amount_USD",
                                                    when(col("currency_id") == "7", (col("balance_amount")/col("ARS_CCL")))
                                                    .when(col("currency_id") == "15", (col("balance_amount")/col("PYG")))
                                                    .when(col("currency_id") == "9", (col("balance_amount")/col("BRL")))
                                                    .otherwise(col("balance_amount")).cast(DecimalType(25, 5))
                                                    ).select(
                                                        "partner_player_id",
                                                        "balance_type_id",
                                                        "currency_id",
                                                        "balance_amount",
                                                        "relacion_operation_t_t_c",
                                                        "amount_USD"
                                                    )
    return result

# COMMAND ----------

df_Balance_USD = get_USDamount(df_Balance)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Upsert de Tabla Delta "Balance"
# MAGIC <p> Obtenemos la tabla original para actualizar la Delta Table 'Balance' con el dataframe df_Balance recién creado.

# COMMAND ----------

print(delta_table_Balance)
print(exist)
print('Se ejecutará en PRD' if PROD else 'Se ejecutará en DEV')

# COMMAND ----------

# def get_dif(nuevo):
#     # Obtengo el df de la tabla Apuestas Y Premios ya creada
#     df_origen = spark.read.table("entprod_mdn.bplay.balance")
#     # Transformat a Dataframe y Seleccionar los campos por comparar
#     origen = df_origen.select("id", "partner_player_id", "balance_type_id", "currency_id", "balance_amount", "relacion_operation_t_t_c")
#     # Obtengo las filas nuevas y actualizadas 
#     dif = nuevo.exceptAll(origen)
#     return dif

# COMMAND ----------

# MAGIC %md
# MAGIC ### Obtención de la Última Tabla Generada para Actualización
# MAGIC <ul>
# MAGIC   <li>Si ya existe la Delta Table de "Balance" creada se obtendra la tabla y se actualizará con el DataFrame generado en esta ejecución.
# MAGIC   <li>Si no existiera, se definita la limitación las particiones en 8 y se generará un nuevo Delta Table de "Balance".
# MAGIC </ul>

# COMMAND ----------

# # Verificar si la ruta contiene una tabla Delta
# if exist:
#     # Si la ruta contiene una tabla Delta, creamos el objeto DeltaTable
#     dt_origen = DeltaTable.forName(spark, "entprod_mdn.bplay.balance")
#     # Upsert del DataFrame de Balance
#     dt_origen.alias("dest").merge(df_Balance.alias("update"), "dest.id = update.id")\
#         .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
# else:
#     # Agregado de las columnas identificatorias para el upgrade de la base
#     df_Balance = df_Balance.coalesce(8)  # Limitar a 8 particiones el DF "df_AyP"
#     # Guardar el DataFrame en formato Delta con overwrite habilitado
#     df_Balance.write \
#         .mode("overwrite")\
#             .option("overwriteSchema", "true")\
#                 .saveAsTable('entprod_mdn.bplay.balance')

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Carga del DataFrame Actualizado de Balance
# MAGIC <p> Se carga la tabla actualizada en el Warehouse de Databricks para su posterior utilización o consumo.

# COMMAND ----------

df_Balance_USD = df_Balance_USD.coalesce(8)

df_Balance_USD.write \
    .mode("overwrite")\
    .option("overwriteSchema", "true")\
        .saveAsTable(delta_table_Balance)