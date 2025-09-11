# Databricks notebook source
# MAGIC %md
# MAGIC # Source_MDN-bplay_PySpark.ipynb
# MAGIC <h> by PySpark
# MAGIC #### Estado: **OK**
# MAGIC <p> Source: Se realizará la lectura de archivos en formato parquet para obtener los dataframes necesarios para generar la tabla 'Source', y cargarla en formato Delta Table para su posterior consulta.
# MAGIC <p> Se tomará la información desde el STG1 de Datalake para generar un consumible de STG2 en formato Delta Table.
# MAGIC <p> Inputs:
# MAGIC   <ol>
# MAGIC     <li>Afiliadores</li>
# MAGIC     <li>Influencers</li>
# MAGIC   </ol>
# MAGIC <p> Output: Delta Table actualizado de "Source" y su carga en el Warehouse de Databricks.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Start Up
# MAGIC <p> Etapa de preparación para su ejecución

# COMMAND ----------

# MAGIC %md
# MAGIC ### Declaración de librerias utilizadas
# MAGIC <p> pyspark.sql.functions = Importa funciones de PySpark para manipular y trabajar con columnas y datos en los DataFrames.

# COMMAND ----------

from pyspark.sql.functions import col, concat, lit, format_string, when, concat_ws, coalesce
from pyspark.sql.types import DateType, IntegerType, LongType, DecimalType
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %md
# MAGIC ### Nomenclatura de Fuentes
# MAGIC <ul>
# MAGIC   <li>df = dataframe</li>
# MAGIC   <li>afi = afiliadores</li>
# MAGIC   <li>inf = influencers</li>
# MAGIC </ul>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Conexion de Source y Obtención de DataFrames
# MAGIC <p> Descripción: 
# MAGIC <ol>
# MAGIC   <li>Establece la conexión al STG1 del Data Lake.</li>
# MAGIC   <li>Se obtienen los archivos parquet de la Base de Datos Boldt para crear los dataframes en formato PySpark.</li>
# MAGIC </ol>

# COMMAND ----------

    #Get affiliate
df_af0 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_py/OGP/affiliate')
df_af1 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_sf/OGP/affiliate')
df_af2 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_caba/OGP/affiliate') # \
                        # .filter(col("partner").isin(252,253,254))
df_af3 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_cordoba/OGP/affiliate')
    #Get source
df_s0 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_py/OGP/source')
df_s1 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_sf/OGP/source')
df_s2 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_caba/OGP/source') # \
                        # .filter(col("partner").isin(252,253,254))
df_s3 = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/dwbplay_cordoba/OGP/source')

    #Get Influencer
# df_i = spark.read.parquet("/Volumes/entproddatalake/default/bplay/Influencers")
df_i = spark.read.csv('/Volumes/entproddatalake/default/mdn_bplay/Tablas_CSV/influencers.csv', 
                      header=True, inferSchema=True, sep=';')

    #Get Afiliadores
# Versión anterior que entró en desuso    
#df_a = spark.read.csv("/Volumes/entproddatalake/default/bplay/Afiliadores/afiliadorescoma_RN.csv", header=True, inferSchema=True, sep=',')

    #Get RLS
df_rls = spark.read.csv('/Volumes/entproddatalake/default/mdn_bplay/Tablas_CSV/RLS.csv', 
                       header=True, inferSchema=True, sep=';')

    # GT Parná, Jujuy y Mendoza [resellers] (se filtran todos los campos para dejar nomenclado acorde a modelo tecnalis)
df_rsGT = spark.read.parquet('/Volumes/boldtproddatalake/boldtdatalake/bplaybet_arg/dbo/resellers') \
                    .select(
                        col("bookmaker_id").alias("bookmaker_id"),
                        col("id").cast("integer").alias("source_id"),
                        col("fullname").alias("source_name"),
                        col("parent_id").cast("integer").alias("parent_id"),
                        col("active").cast("smallint").alias("active"),
                        col("level_id").cast("integer").alias("level_id")
                        )

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

#   Path PRD del Delta Table Source
delta_table_Source_prd = "entprod_mdn.bplay.source"

#   Path DEV del Delta Table Source
delta_table_Source_dev = "entprod_mdn.default.source"

#   Switch PROD/DEV
if PROD:
    delta_table_Source = delta_table_Source_prd
else:
    delta_table_Source = delta_table_Source_dev

# Manejar el caso en que la tabla no exista
try:
    # Intentar leer la tabla Delta
    df_origen = spark.read.table(delta_table_Source)
    count = df_origen.select("partner_affiliate_source_id").count()

    # Validar la tabla AyP
    if count > 0:
        exist = True
    else:
        exist = False
except Exception as e:
    # print(f"La tabla {delta_table_Balance} no existe o no se puede leer: {e}")
    exist = False

# COMMAND ----------

# Modificación resellers según migración

# Definir los datos
data = [(46, 460), (13, 130), (88, 256)]

# Definir los nombres de las columnas
columns = ["id", "partner"]

# Crear el DataFrame
df_partner_Migra = spark.createDataFrame(data, columns)


# COMMAND ----------

# Modificación resellers según migración
df_rsGT = df_rsGT.join(df_partner_Migra, df_rsGT["bookmaker_id"] == df_partner_Migra["id"], "inner") \
                    .select(
                            df_partner_Migra["partner"].alias("partner_id"),
                            df_rsGT["source_id"].cast("integer").alias("source_id"),
                            df_rsGT["source_name"].alias("source_name"),
                            df_rsGT["parent_id"].cast("integer").alias("parent_id"),
                            df_rsGT["active"].cast("smallint").alias("active"),
                            df_rsGT["level_id"].cast("integer").alias("level_id")
                        )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Definición de la Función Create_Afiliadores
# MAGIC <p> Esta función genera un dataframe con la información necesaria de los Afiliadores.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create_Afiliadores (Tecnalis/GiG)
# MAGIC <p> Se define la función Create_Afiliadores para obtener el dataframe con los afiliadores de la plataforma Tecnalis

# COMMAND ----------

def Create_Afiliadores(a, af, s):
    a = a.filter(col("Tipo_Tabla") == "Afiliador")   
    afiliadores = a.join(af, (a["partner_id"] == af["partner"]) & (af["id"] == a["affiliate_id"]), "inner")\
        .join(s, (a["partner_id"] == s["partner"]) & (s["affiliate"] == a["affiliate_id"]), "inner")\
            .select(
    a["partner_id"],
    a["Tipo_Tabla"].alias("tipo_tabla"),
    a["affiliate_id"],
    s["id"].alias("source_id"),
    a["afiliador"].alias("nombre"),
    a["fecha_contrato"].cast(DateType()).alias("fecha_contrato"),
    when(a["%_TAX"] == "NULL", None).otherwise(a["%_TAX"]).alias("%_TAX"),
    a["activo"],
    a["Tipo_Afiliador"],
    a['mail'],
    af["name"].alias("nombre_affiliate"),
    s["name"].alias("nombre_source"),
    concat(a["partner_id"], 
           format_string("%05d", a["affiliate_id"]), 
           format_string("%08d", s["id"])
           ).alias("partner_affiliate_source_id"),
    concat(a["partner_id"], 
           format_string("%05d", a["affiliate_id"])
           ).alias("partner_affiliate_id")
    )
    return afiliadores

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create_Afiliadores (bplaybet)
# MAGIC <p> Se define la función Create_Afiliadores para obtener el dataframe con los afiliadores de la plataforma bplaybet

# COMMAND ----------

# Función para crear la jerarquía 0 de los afiliadores bplaybet (máxima jerarquía)
def Get_resellers_0(rsGT):
    # rls = dataframe de rls y afiliadores
    # rsGT = dataframe de tabla resellers bplaybet

    # Busco jerarquía más alta (abuelo)
    ab = rsGT.filter(rsGT["parent_id"].isNull())\
                .select(
                        rsGT['partner_id'].alias("partner_id"),
                        rsGT['source_name'].alias("nombre_affiliate"),
                        rsGT['source_name'].alias("nombre_source"),
                        rsGT['source_id'].alias("affiliate_id"),
                        rsGT['source_id'].alias("source_id"),
                        lit("0").alias("jerarquia"),
                        concat( rsGT["partner_id"], 
                                format_string("%05d", rsGT["source_id"]),
                                format_string("%08d", rsGT["source_id"])).alias("partner_affiliate_source_id"),
                        rsGT["active"].cast("smallint").alias("active"),
                        rsGT["level_id"].cast("integer").alias("level_id")
                )

    return ab  

# Función para cualquier jerarquía de afiliadores bplaybet (que no sea la máxima jerarquía)
def Get_resellers(listado_base, listado_nuevo, jerarquia):

    ancestro = jerarquia - 1
    lb = listado_base.alias("lb")
    ln = listado_nuevo.alias("ln")

    if ancestro >= 0:
        hi = ln.join(lb, (lb["parent_id"].isNotNull()) & (lb["partner_id"] == ln["partner_id"]) \
                    & (lb["parent_id"] == ln["source_id"])) \
                .select(
                        ln['partner_id'].alias("partner_id"),
                        ln['nombre_affiliate'].alias("nombre_affiliate"),
                        lb['source_name'].alias("nombre_source"),
                        ln['affiliate_id'].alias("affiliate_id"),
                        lb['source_id'].alias("source_id"),
                        lit(jerarquia).alias("jerarquia"),
                        concat( ln["partner_id"], 
                                format_string("%05d", ln["affiliate_id"]),
                                format_string("%08d", lb["source_id"])).alias("partner_affiliate_source_id"),
                        lb["active"].cast("smallint").alias("active"),
                        lb["level_id"].cast("integer").alias("level_id")
                )

        return hi                
    else:
        print(f"Error en la función Get_resellers. Jerarquía errónea")
        return None       

# Crear listado de resellers con todas las jerarquías correctamente asignadas y en un modelo parecido al de Tecnalis
# Se genera el valor de partner_affiliate_source_id para luego cruzar los datos con los usuarios    
def Get_Resellers_GT(rsGT):
    # Nivel base (jerarquía 0)
    jerarquia = 0
    historial = []
    
    df_nivel = Get_resellers_0(rsGT)
    historial.append(df_nivel)

    while True:
        # Llamamos a la siguiente jerarquía
        df_siguiente = Get_resellers(rsGT, df_nivel, jerarquia + 1)
        
        # Si no hay más datos, cortamos el ciclo
        if df_siguiente is None or df_siguiente.count() == 0: # df_siguiente.rdd.isEmpty(): # no lo toma Python en ClustrPrEnt
            break
        
        historial.append(df_siguiente)
        df_nivel = df_siguiente
        jerarquia += 1

    # Unimos todos los niveles en un único DataFrame
    return historial[0].unionByName(*historial[1:])   


# Crear listado de afiliadores bplaybet cruzandolo con los afiliadores listados en el csv de RLS   
def Create_Afiliadores_bplaybet(rls, rsGT):
    # rls = dataframe de rls y afiliadores
    # rsGT = dataframe de tabla resellers bplaybet

    # Filtra las filas del csv de RLS
    listado = rls.filter(col("partner_id").isin(130, 460, 256)).filter(col("Tipo_Tabla") == "Afiliador") 

    # Cruzo los datos de afiliadores en listado rls contra los datos de resellers bplaybet 
    source_gt = listado.join(rsGT, (rsGT["affiliate_id"] == listado["affiliate_id"]) \
                & (rsGT["partner_id"] == listado["partner_id"])) \
                .select(
                        listado['partner_id'],
                        listado['jurisdiccion'],
                        lit("Afiliador").alias("tipo_tabla"),
                        listado['affiliate_id'],
                        rsGT['source_id'],
                        listado['afiliador'].alias("nombre"),
                        listado['fecha_contrato'],
                        listado['%_TAX'],
                        listado['Activo'].cast('integer').alias("activo"),
                        listado['Tipo_Afiliador'],
                        listado['mail'],
                        rsGT['nombre_affiliate'],
                        rsGT['nombre_source'], 
                        rsGT['partner_affiliate_source_id'],
                        concat(rsGT["partner_id"],
                                format_string("%05d", rsGT["affiliate_id"])
                                ).cast('integer').alias("partner_affiliate_id")
                        )
    
    return source_gt


# COMMAND ----------

# MAGIC %md
# MAGIC ### Ejecución del DataFrame Afiliadores por Base de Datos: DWBPLAY_PY, DWBPLAY_SF, DWBPLAY_CABA y DWBPLAY_CORDOBA
# MAGIC <p> Se ejecuta la función Create_Afiliadores para obtener el dataframe con la información de los evnetos en la base de datos: DWBPLAY_PY, DWBPLAY_SF, DWBPLAY_CABA y DWBPLAY_CORDOBA.

# COMMAND ----------

df_afiliadores = df_rls

# COMMAND ----------

    # Get DWBPLAY_PY
df_Afiliadores0 = Create_Afiliadores(df_afiliadores, df_af0, df_s0)
    # Get DWBPLAY_SF
df_Afiliadores1 = Create_Afiliadores(df_afiliadores, df_af1, df_s1)
    # Get DWBPLAY_CABA
df_Afiliadores2 = Create_Afiliadores(df_afiliadores, df_af2, df_s2)
    # Get DWBPLAY_CORDOBA
df_Afiliadores3 = Create_Afiliadores(df_afiliadores, df_af3, df_s3)

# COMMAND ----------

# Crear Listado completo resellers GT
df_resellers = Get_Resellers_GT(df_rsGT)
# Crear Listado de afiliadores bplaybet
df_AfiliadoresGT = Create_Afiliadores_bplaybet(df_afiliadores, df_resellers)


# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Definición de la Función Create_Influencers
# MAGIC <p> Esta función genera un dataframe con la información necesaria de los Influencers.

# COMMAND ----------

def Create_Influencer(i, af, s):
    influencers = i.join(af, (af["partner"] == i["partner_id"]) & (af["id"] == i["affiliate_id"]), "inner")\
        .join(s, (af["partner"] == s["partner"]) & (af["id"] == s["affiliate"]) & (i["source_id"] == s["id"]))\
            .select(
                i["partner_id"].cast("integer").alias("partner_id"),
                lit("Influencer").alias("tipo_tabla"),
                i["affiliate_id"].cast("integer").alias("affiliate_id"),
                i["source_id"].cast("bigint").alias("source_id"),
                i["influencer"].alias("nombre"),
                lit(None).cast(DateType()).alias("fecha_contrato"),
                lit(None).cast("string").alias("%_TAX"),
                when(i["filtro_influencer"].like("%ctivo%"), 1).otherwise(0).cast("integer").alias("activo"),
                lit(None).cast("string").alias("Tipo_Afiliador"),
                lit(None).cast("string").alias('mail'),
                af["name"].alias("nombre_affiliate"),
                s["name"].alias("nombre_source"),
                concat(i["partner_id"], 
                       format_string("%05d", i["affiliate_id"]), 
                       format_string("%08d", s["id"])
                       ).alias("partner_affiliate_source_id"),
                 concat(i["partner_id"], 
                       format_string("%05d", i["affiliate_id"])
                       ).alias("partner_affiliate_id")
            )

    return influencers


def Create_Influencer_GT(i, rs):
    influencers_GT = i.filter(i["partner_id"].isin(130,460,880))\
        .join(rs, (rs["partner_id"] == i["partner_id"]) & (rs["source_id"] == i["source_id"]), "inner")\
            .select(
                rs["partner_id"].alias("partner_id"),
                lit("Influencer").alias("tipo_tabla"),
                rs["affiliate_id"].alias("affiliate_id"),
                rs["source_id"].alias("source_id"),
                i["influencer"].alias("nombre"),
                lit(None).cast(DateType()).alias("fecha_contrato"),
                lit(None).cast("string").alias("%_TAX"),
                when(i["filtro_influencer"].like("%ctivo%"), 1).otherwise(0).cast("integer").alias("activo"),
                lit(None).cast("string").alias("Tipo_Afiliador"),
                lit(None).cast("string").alias('mail'),
                rs["nombre_affiliate"].alias("nombre_affiliate"),
                rs["nombre_source"].alias("nombre_source"),
                rs["partner_affiliate_source_id"].alias("partner_affiliate_source_id"),
                concat(rs["partner_id"], 
                       format_string("%05d", rs["affiliate_id"])
                       ).alias("partner_affiliate_id")
            )
    
    return influencers_GT    
    

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ejecución del DataFrame Influencers por Base de Datos: DWBPLAY_PY, DWBPLAY_SF, DWBPLAY_CABA y DWBPLAY_CORDOBA
# MAGIC <p> Se ejecuta la función Create_Influencers para obtener el dataframe con la información de los evnetos en la base de datos: DWBPLAY_PY, DWBPLAY_SF, DWBPLAY_CABA y DWBPLAY_CORDOBA.

# COMMAND ----------

    # Get DWBPLAY_PY
df_Influencers0 = Create_Influencer(df_i, df_af0, df_s0)
    # Get DWBPLAY_SF
df_Influencers1 = Create_Influencer(df_i, df_af1, df_s1)
    # Get DWBPLAY_CABA
df_Influencers2 = Create_Influencer(df_i, df_af2, df_s2)
    # Get DWBPLAY_CORDOBA
df_Influencers3 = Create_Influencer(df_i, df_af3, df_s3)

    # Get Influencers_GT
df_InfluencersGT = Create_Influencer_GT(df_i, df_resellers)
# display(df_InfluencersGT)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Create SubAfiliadores

# COMMAND ----------

def Create_SubAfiliadores(a, af, s):
   
    a = a.filter(col("Tipo_Tabla") == "SubAfiliador")   
    subafiliadores = a.join(af, (a["partner_id"] == af["partner"]) & (af["id"] == a["affiliate_id"]), "inner")\
        .join(s, (a["partner_id"] == s["partner"]) & (s["affiliate"] == a["affiliate_id"]) & (a["source_id"] == s["id"]), "inner")\
            .select(
                    a["partner_id"],
                    a["Tipo_Tabla"].alias("tipo_tabla"),
                    a["affiliate_id"],
                    a["Source_id"].alias("source_id"),
                    a["afiliador"].alias("nombre"),
                    a["fecha_contrato"].cast(DateType()).alias("fecha_contrato"),
                    when(a["%_TAX"] == "NULL", None).otherwise(a["%_TAX"]).alias("%_TAX"),
                    a["activo"],
                    a["Tipo_Afiliador"],
                    a['mail'],
                    af["name"].alias("nombre_affiliate"),
                    s["name"].alias("nombre_source"),
                    a["partner_affiliate_source_id"].cast("string").alias("partner_affiliate_source_id"),
                    a["partner_affiliate_id"].cast("string").alias("partner_affiliate_id")
                    )
    return subafiliadores


def Create_SubAfiliadores_GT(a, rslls):
   
    a = a.filter(col("Tipo_Tabla") == "SubAfiliador").filter(col("partner_id").isin(130 ,256, 460, 880))   
    subafiliadores = a.join(rslls, (a["partner_id"] == rslls["partner_id"]) & (a["source_id"] == rslls["source_id"]), "inner")\
            .select(
                    a["partner_id"],
                    a["Tipo_Tabla"].alias("tipo_tabla"),
                    rslls["affiliate_id"].alias("affiliate_id"),
                    a["Source_id"].alias("source_id"),
                    a["afiliador"].alias("nombre"),
                    a["fecha_contrato"].cast(DateType()).alias("fecha_contrato"),
                    when(a["%_TAX"] == "NULL", None).otherwise(a["%_TAX"]).alias("%_TAX"),
                    a["activo"],
                    a["Tipo_Afiliador"],
                    a['mail'],
                    rslls["nombre_affiliate"].alias("nombre_affiliate"),
                    rslls["nombre_source"].alias("nombre_source"),
                    rslls["partner_affiliate_source_id"].cast("string").alias("partner_affiliate_source_id"),
                    concat(rslls["partner_id"], 
                       format_string("%05d", rslls["affiliate_id"])
                       ).alias("partner_affiliate_id")
                    )
    return subafiliadores        


# COMMAND ----------

    # Get DWBPLAY_PY
df_SubAfiliadores0 = Create_SubAfiliadores(df_afiliadores, df_af0, df_s0)
    # Get DWBPLAY_SF
df_SubAfiliadores1 = Create_SubAfiliadores(df_afiliadores, df_af1, df_s1)
    # Get DWBPLAY_CABA
df_SubAfiliadores2 = Create_SubAfiliadores(df_afiliadores, df_af2, df_s2)
    # Get DWBPLAY_CORDOBA
df_SubAfiliadores3 = Create_SubAfiliadores(df_afiliadores, df_af3, df_s3)

    # Get bplaybet
df_SubAfiliadoresGT = Create_SubAfiliadores_GT(df_afiliadores, df_resellers)


# COMMAND ----------

def Create_merge(df_Afiliadores, df_SubAfiliadores):

    # Realizar un FULL OUTER JOIN en el ID
    merged_df = df_Afiliadores.alias("dfa").join(
        df_SubAfiliadores.alias("dfsa"), 
        on="partner_affiliate_source_id", 
        how="outer"
    )

    # Concatenar emails si el ID existe en ambos DataFrames
    updated_email = when(
        col("dfa.mail").isNotNull() & col("dfsa.mail").isNotNull(), 
        concat_ws(", ", col("dfa.mail"), col("dfsa.mail"))
    ).when(col("dfa.mail").isNotNull(), col("dfa.mail")
    ).otherwise(col("dfsa.mail"))

    # Mantener los valores originales de df1 para el resto de las columnas
    final_df = merged_df.select(
        col("partner_affiliate_source_id"),
        updated_email.alias("mail"),
        coalesce(col("dfa.partner_id"), col("dfsa.partner_id")).alias("partner_id"),
        coalesce(col("dfa.tipo_tabla"), col("dfsa.tipo_tabla")).alias("tipo_tabla"),
        coalesce(col("dfa.affiliate_id"), col("dfsa.affiliate_id")).alias("affiliate_id"),
        coalesce(col("dfa.source_id"), col("dfsa.source_id")).alias("source_id"),
        coalesce(col("dfa.nombre"), col("dfsa.nombre")).alias("nombre"),
        coalesce(col("dfa.fecha_contrato"), col("dfsa.fecha_contrato")).alias("fecha_contrato"),
        coalesce(col("dfa.%_TAX"), col("dfsa.%_TAX")).alias("%_TAX"),
        coalesce(col("dfa.Activo"), col("dfsa.Activo")).alias("activo"),
        coalesce(col("dfa.Tipo_Afiliador"), col("dfsa.Tipo_Afiliador")).alias("Tipo_Afiliador"),
        coalesce(col("dfa.nombre_affiliate"), col("dfsa.nombre_affiliate")).alias("nombre_affiliate"),
        coalesce(col("dfa.nombre_source"), col("dfsa.nombre_source")).alias("nombre_source"),
        coalesce(col("dfa.partner_affiliate_id"), col("dfsa.partner_affiliate_id")).alias("partner_affiliate_id")
    )

    return final_df

# COMMAND ----------

    # Get Merge DWBPLAY_PY
df_Afiliadores0 = Create_merge(df_Afiliadores0, df_SubAfiliadores0)
    # Get Merge DWBPLAY_SF
df_Afiliadores1 = Create_merge(df_Afiliadores1, df_SubAfiliadores1)
    # Get Merge DWBPLAY_CABA
df_Afiliadores2 = Create_merge(df_Afiliadores2, df_SubAfiliadores2)
    # Get Merge DWBPLAY_CORDOBA
df_Afiliadores3 = Create_merge(df_Afiliadores3, df_SubAfiliadores3)

    # Get Merge bplaybet
df_AfiliadoresGT = Create_merge(df_AfiliadoresGT, df_SubAfiliadoresGT)


# COMMAND ----------

# display(df_resellers)

# COMMAND ----------

# display(df_AfiliadoresGT)

# COMMAND ----------

# display(df_SubAfiliadoresGT)

# COMMAND ----------

# display(df_InfluencersGT)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Create DF Source

# COMMAND ----------

# MAGIC %md
# MAGIC #### Source Tecnalis
# MAGIC <p> Se incorporan a la tabla Source todas las fuentes que no se encuentran tipificadas como Afiliador ni Influencer

# COMMAND ----------

def Get_Source(a, s, source):   
    total = a.join(s, (a["partner"] == s["partner"]) & (s["affiliate"] == a["id"]), "inner")\
            .select(
                     a["partner"].alias('partner_id'),
                     lit("Source").alias("tipo_tabla"),
                     a["id"].alias("affiliate_id"),
                     s["id"].alias("source_id"),
                     a["name"].alias("nombre"),
                     lit(None).cast(DateType()).alias("fecha_contrato"),
                     lit(0).alias("%_TAX"),
                     a["enabled"].alias('activo'),
                     lit(None).cast("string").alias("Tipo_Afiliador"),
                     lit(None).cast("string").alias("mail"),
                     a["name"].alias("nombre_affiliate"),
                     s["name"].alias("nombre_source"),
                     concat(a["partner"], 
                            format_string("%05d", a["id"]), 
                            format_string("%08d", s["id"])
                            ).alias("partner_affiliate_source_id"),
                     concat(a["partner"], 
                            format_string("%05d", a["id"])
                            ).alias("partner_affiliate_id")
              )
    
    rest = total.select("partner_affiliate_source_id").exceptAll(source.select("partner_affiliate_source_id"))

    result = source.unionByName(rest.join(total, "partner_affiliate_source_id")).withColumn("Jurisdiccion", \
                            when(col("partner_id") == '250', 'PY')\
                            .when(col("partner_id") == '251', 'SF')\
                            .when(col("partner_id") == '252', 'CABA')\
                            .when(col("partner_id") == '253', 'PBA')\
                            .when(col("partner_id") == '254', 'ER')\
                            .when(col("partner_id") == '255', 'COR')\
                            .when(col("partner_id") == '256', 'MZA')\
                            .otherwise(None)
                            )
    return result

# COMMAND ----------

# MAGIC %md
# MAGIC #### Source Proveedores
# MAGIC <p> Se incorporan a la tabla Source todas las fuentes que se encuentran tipificadas como Proveedor (accesos por jurisdicción según RLS tableros)

# COMMAND ----------

def Add_Proveedores(rls, table):                    # se quitó el partner_id 460 del filtro
    res = rls.filter(col("partner_id").isin(500)) \
        .select(
                col("partner_id"),
                col("Jurisdiccion"),
                lit('Proveedor').alias("tipo_tabla"),
                col("Affiliate_id").alias("affiliate_id"),
                lit(0).alias("source_id"),
                col("Afiliador").alias("nombre"),
                col("fecha_contrato").cast(DateType()).alias("fecha_contrato"),
                col("%_TAX"),
                col("Activo").alias("activo"),
                col("Tipo_Afiliador"),
                col('mail'),
                col("Afiliador").alias("nombre_affiliate"),
                col("Afiliador").alias("nombre_source"),
                concat(col("partner_affiliate_id"), lit('00000000')).alias("partner_affiliate_source_id"),
                col("partner_affiliate_id").alias("partner_affiliate_id"),
                lit(None).alias("sobreescritura_pas_id")
            )

    result = table.unionByName(res)
    
    return result

# COMMAND ----------

# MAGIC %md
# MAGIC #### Source GT Jujuy
# MAGIC <p> Se incorporan a la tabla Source todas las fuentes que no se encuentran tipificadas como Afiliador ni Influencer

# COMMAND ----------

def Get_Source_GT(rslls, source):   
    total = rslls.select(
                        rslls["partner_id"].alias("partner_id"),
                        lit("Source").alias("tipo_tabla"),
                        rslls["affiliate_id"].alias("affiliate_id"),
                        rslls["source_id"].alias("source_id"),
                        rslls["nombre_affiliate"].alias("nombre"),
                        lit(None).cast(DateType()).alias("fecha_contrato"),
                        lit(0).alias("%_TAX"),
                        rslls["active"].alias('activo'),
                        # lit(1).alias("activo"),
                        lit(None).cast("string").alias("Tipo_Afiliador"),
                        lit(None).cast("string").alias("mail"),
                        rslls["nombre_affiliate"].alias("nombre_affiliate"),
                        rslls["nombre_source"].alias("nombre_source"),
                        rslls["partner_affiliate_source_id"].alias("partner_affiliate_source_id"),
                        concat(rslls["partner_id"], 
                            format_string("%05d", rslls["affiliate_id"])
                            ).alias("partner_affiliate_id")
                        )
                        
    rest = total.select("partner_affiliate_source_id").exceptAll(source.select("partner_affiliate_source_id"))

    result = source.unionByName(rest.join(total, "partner_affiliate_source_id")).withColumn("Jurisdiccion", \
                            when(col("partner_id") == '130', 'PRN')\
                            .when(col("partner_id") == '460', 'JUJ')\
                            .when(col("partner_id") == '256', 'MZA')
                            .otherwise(None)
                            )
    
    return result


# COMMAND ----------

# Sobreescribo los valores de bplaybet en el listado de Tecnalis para evitar duplicados (tanto Afiliadores como Influencers).
# Agrego "dif" si sobreescribe un valor distinto de nombre y "ok" si la sobreescritura tiene el mismo nombre.
def Create_merge_T_bb(df_Tecnalis, df_bplaybet):

    # Realizar un FULL OUTER JOIN en el ID
    merged_df = df_Tecnalis.alias("dft").join(
        df_bplaybet.alias("dfb"), 
        on="partner_affiliate_source_id", 
        how="outer"
    )

    # Establecer columna de sobreescritura si el partner_affiliate_source_id existe en ambos DataFrames
    sobreescritura_pas_id = when(
        col("dft.nombre") != col("dfb.nombre"), 
        lit("dif")    # indica sobreescritura de bplaybet sobre tecnalis con nombres distintos
    ).when(col("dft.nombre") == col("dfb.nombre"), lit("ok")    # indica sobreescritura de bb sobre t correcta
    ).otherwise(lit(None))                                      # indica que no hubo sobreescritura

    # Mantener los valores originales de df1 para el resto de las columnas
    final_df = merged_df.select(
        col("partner_affiliate_source_id"),
        coalesce(col("dfb.partner_id"), col("dft.partner_id")).alias("partner_id"),
        coalesce(col("dfb.Jurisdiccion"), col("dft.Jurisdiccion")).alias("Jurisdiccion"),
        coalesce(col("dfb.tipo_tabla"), col("dft.tipo_tabla")).alias("tipo_tabla"),
        coalesce(col("dfb.affiliate_id"), col("dft.affiliate_id")).alias("affiliate_id"),
        coalesce(col("dfb.source_id"), col("dft.source_id")).alias("source_id"),
        coalesce(col("dfb.nombre"), col("dft.nombre")).alias("nombre"),
        coalesce(col("dfb.fecha_contrato"), col("dft.fecha_contrato")).alias("fecha_contrato"),
        coalesce(col("dfb.%_TAX"), col("dft.%_TAX")).alias("%_TAX"),
        coalesce(col("dfb.Activo"), col("dft.Activo")).alias("activo"),
        coalesce(col("dfb.Tipo_Afiliador"), col("dft.Tipo_Afiliador")).alias("Tipo_Afiliador"),
        coalesce(col("dfb.mail"), col("dft.mail")).alias("mail"),
        coalesce(col("dfb.nombre_affiliate"), col("dft.nombre_affiliate")).alias("nombre_affiliate"),
        coalesce(col("dfb.nombre_source"), col("dft.nombre_source")).alias("nombre_source"),
        coalesce(col("dfb.partner_affiliate_id"), col("dft.partner_affiliate_id")).alias("partner_affiliate_id"),
        sobreescritura_pas_id.alias("sobreescritura_pas_id")
    )

    return final_df

# COMMAND ----------

# MAGIC %md
# MAGIC ### Union Source

# COMMAND ----------

# Union Influencers + Afiliadores por DW
df_SourcePY = df_Influencers0.unionByName(df_Afiliadores0)
df_SourceSF = df_Influencers1.unionByName(df_Afiliadores1)
df_SourceCABA = df_Influencers2.unionByName(df_Afiliadores2)
df_SourceCBA = df_Influencers3.unionByName(df_Afiliadores3)

df_SourceGT = df_InfluencersGT.unionByName(df_AfiliadoresGT)

# Get Source completo Source + Alira Af & S
df_Source0  = Get_Source(df_af0, df_s0, df_SourcePY)
df_Source1  = Get_Source(df_af1, df_s1, df_SourceSF)
df_Source2  = Get_Source(df_af2, df_s2, df_SourceCABA)
df_Source3  = Get_Source(df_af3, df_s3, df_SourceCBA)

# df_SourceGT = Get_Source_GT(df_rsGT)
df_SourceGT = Get_Source_GT(df_resellers, df_SourceGT)

# COMMAND ----------

 # Carga de Source + conjunto completo 
df_Union_Source = (df_Source0.unionByName(df_Source1)).unionByName(df_Source2.unionByName(df_Source3)) \
                        .withColumn("sobreescritura_pas_id", lit(None))

#.unionByName(df_SourceGT)))
df_Union_Source = Create_merge_T_bb(df_Union_Source, df_SourceGT)

# COMMAND ----------

# Agregado de Proveedores
df_Source = Add_Proveedores(df_afiliadores, df_Union_Source) 
                #     .select("partner_id", "Jurisdiccion", "tipo_tabla", "affiliate_id", "source_id", "nombre", 
                #             "fecha_contrato", "%_TAX", "activo", "Tipo_Afiliador", "mail", "nombre_affiliate", "nombre_source", "partner_affiliate_source_id", "partner_affiliate_id", "sobreescritura_pas_id")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Upsert de Tabla Delta "Source"
# MAGIC <p> Obtenemos la tabla original para actualizar la Delta Table 'Source' con el dataframe df_Source recién creado.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Obtención de la Última Tabla Generada para Actualización
# MAGIC <p> Si ya existe la Delta Table de Eventos creada se obtendra la tabla y se actualizará con el DataFrame generado en esta ejecución.
# MAGIC <p> Si no existiera, se definirá la limitación las particiones en 8, y se generará un nuevo Delta Table de "Source".

# COMMAND ----------

print(delta_table_Source)
print(exist)

# COMMAND ----------

# # Verificar si la ruta contiene una tabla Delta
# if exist:
#     # Si la ruta contiene una tabla Delta, creamos el objeto DeltaTable
#     dt_origin = DeltaTable.forName(spark, delta_table_Source)

#     # Upsert del DataFrame de Source
#     (dt_origin.alias("dest").merge(df_Source.alias("update"), 'dest.partner_affiliate_source_id = update.partner_affiliate_source_id')\
#         .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())
# else:
#     # Limitar a 8 particiones el DF "Source"
#     df_Source = df_Source.coalesce(8)
#     # Guardar el DataFrame en formato Delta
#     df_Source.write.mode("overwrite").saveAsTable(delta_table_Source)

# COMMAND ----------

# Limitar a 8 particiones el DF "Source"
df_Source = df_Source.coalesce(8)
# Guardar el DataFrame en formato Delta
df_Source.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(delta_table_Source)