# Databricks notebook source
# MAGIC %md
# MAGIC # Caliendario_MDN-bplay_PySpark.ipynb
# MAGIC <h> by PySpark
# MAGIC #### Estado: **OK**
# MAGIC <p> Se calculara el calendario en distintos formatos segun la necesidad del negocio a partir del 01-01-2015 para generar un consumible en STG2 en formato Delta Table.
# MAGIC <p> Output: Delta Table actualizado de "calendario" y su carga en el Warehouse de Databricks.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Start Up
# MAGIC <p> Etapa de preparación para su ejecución

# COMMAND ----------

# MAGIC %md
# MAGIC ### Declaración de librerias utilizadas
# MAGIC <p> pyspark.sql.functions = Importa funciones de PySpark para manipular y trabajar con columnas y datos en los DataFrames.
# MAGIC <p> pyspark.sql.types = Importa tipos de datos, para definir la estructura de los DataFrames.
# MAGIC <p> import time / from datetime import datetime = Importa funciones de tipo date, para obtener y calcular días, meses y años.

# COMMAND ----------

import time
from pyspark.sql.functions import day, year, month, date_format, dayofmonth, date_trunc, last_day, when, dayofweek, quarter, substring, lit, lpad, concat, date_add, weekofyear, datediff, to_date, col, expr, udf
from pyspark.sql.types import IntegerType, DateType, StringType
from datetime import datetime

# Set the configuration property to restore the behavior before Spark 3.0
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Calendario

# COMMAND ----------

# MAGIC %md
# MAGIC ### Definición inicial del DF
# MAGIC <p> Se calculara la cantidad de días a partir del 01-01-2015 hasta el 31-12 del año corriente. Y se generara un dataframe inicial con las fechas desde 01-01-2015 hasta el 31-12 del año corriente

# COMMAND ----------

# Definir la cantidad de días desde 2015 hasta el final del año actual
numero_de_dias = (datetime.strptime("{}-12-31".format(datetime.today().year), "%Y-%m-%d").date() - (datetime.strptime('2014-12-31', '%Y-%m-%d')).date()).days

# Crear un rango de fechas desde 2015 hasta el final del año actual
fechas = spark.range(1, (numero_de_dias + 1 )).selectExpr("date_add(to_date('2015-01-01'), CAST(id AS INT)) as Fecha")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Definición del DF Calendario
# MAGIC <p> A partir del DF creado anteriormente se calculara las columnas necesarias para el negocio

# COMMAND ----------

# Añadir columnas con las transformaciones requeridas
# date_format(col("Fecha"), "EEEE, d 'de' MMMM 'de' yyyy")
df_Calendario = fechas \
    .withColumn("Dia", day(col("Fecha"))) \
    .withColumn("Anio", year(col("Fecha"))) \
    .withColumn("Mes", month(col("Fecha"))) \
    .withColumn("Nombre_del_mes", date_format(col("Fecha"), "MMMM")) \
    .withColumn("Dias_del_mes", dayofmonth(col("Fecha"))) \
    .withColumn("Dia_de_la_Semana", dayofweek(col("Fecha"))) \
    .withColumn("Inicio_del_mes", date_trunc("month", col("Fecha")).cast("date")) \
    .withColumn("Fin_del_mes", last_day(col("Fecha"))) \
    .withColumn("Anio_fiscal", when(month(col("Fecha")) >= 11, year(col("Fecha")) + 1).otherwise(year(col("Fecha")))) \
    .withColumn("Periodo_fiscal", when(month(col("Fecha")) >= 11, month(col("Fecha")) - 10).otherwise(month(col("Fecha")) + 2)) \
    .withColumn("Periodo_semanal", when((dayofweek(col("Fecha")) >= 1) & (dayofweek(col("Fecha")) <= 4 ), "LaJ").otherwise("VaD")) \
    .withColumn("Trimestre_calendario", quarter(col("Fecha"))) \
    .withColumn("Cuatrimestre_calendario", ((month(col("Fecha")) - 1) / 4 + 1).cast(IntegerType())) \
    .withColumn("Semestre_calendario", ((month(col("Fecha")) - 1) / 6 + 1).cast(IntegerType())) \
    .withColumn("Trimestre_fiscal", (col("Periodo_fiscal") - 1) % 4 + 1) \
    .withColumn("Cuatrimestre_fiscal", ((month(col("Fecha")) + 2) / 4 + 1).cast(IntegerType())) \
    .withColumn("Semestre_fiscal", ((month(col("Fecha")) + 2) / 6 + 1).cast(IntegerType())) \
    .withColumn("Semana_del_mes", date_format(col("Fecha"), "W").cast(IntegerType())) \
    .withColumn("Semana_del_anio", weekofyear(col("Fecha"))) \
    .withColumn("Nombre_del_dia", date_format(col("Fecha"), "EEEE")) \
    .withColumn("id_fecha", concat(year(col("Fecha")), lpad(month(col("Fecha")), 2, "0"), lpad(dayofmonth(col("Fecha")), 2, "0")).cast(IntegerType())) \
    .withColumn("Anio-Mes", concat(col("Anio"), lpad(col("Mes").cast(StringType()), 2, "0")).cast(IntegerType())) \
    .withColumn("Mes_corto_y_Anio", concat(substring(col("Nombre_del_mes"), 1, 3), lit("-"), col("Anio")).cast("string")) \
    .withColumn("Desde-Hasta_Semana", concat(date_trunc("week", col("Fecha")).cast(DateType()), lit(" al "), date_add(date_trunc("week", col("Fecha")), 6))) \
    .withColumn("id_fecha_numerico", col("id_fecha").cast(IntegerType())) \
    .withColumn("Ini_Semana", concat(date_trunc("week", col("Fecha"))).cast(DateType())) \
    .withColumn("Quincena", when(col('Dia') <= 15, lit("1ra Quincena")).otherwise(lit("2da Quincena"))) \
    .withColumn("Decena", when(col('Dia') <= 10, lit("1ra Decena")).when(col('Dia') <= 20, lit("2ra Decena")).otherwise(lit("3da Quincena")))



# COMMAND ----------

# MAGIC %md
# MAGIC ### Formato del DF Calendario
# MAGIC <p> Se dara formato en las columnas que se requieran

# COMMAND ----------

# Reordenar las columnas según el orden original
df_Calendario_en = df_Calendario.select(
    "Fecha",
    #date_format(col("Fecha"), "EEEE, d 'de' MMMM 'de' yyyy").alias("Fecha_es"), 
    "Dia",
    "Anio", 
    "Mes", 
    "Nombre_del_mes", 
    "Dia_de_la_Semana",
    "Dias_del_mes", 
    "Inicio_del_mes",
    #date_format(col("Inicio_del_mes"), "EEEE, d 'de' MMMM 'de' yyyy").alias("Inicio_del_mes_es"), 
    "Fin_del_mes",
    #date_format(col("Fin_del_mes"), "EEEE, d 'de' MMMM 'de' yyyy").alias("Fin_del_mes_es"),
    "Anio_fiscal", 
    "Periodo_fiscal", 
    "Periodo_semanal", 
    "Trimestre_calendario", 
    "Cuatrimestre_calendario",
    "Semestre_calendario", 
    "Trimestre_fiscal", 
    "Cuatrimestre_fiscal", 
    "Semestre_fiscal", 
    "Semana_del_mes",
    "Semana_del_anio", 
    "Nombre_del_dia", 
    "id_fecha", 
    "Anio-Mes", 
    "Mes_corto_y_Anio", 
    "Desde-Hasta_Semana",
    "id_fecha_numerico",
    "Ini_Semana",
    "Quincena",
    "Decena"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Formato Español

# COMMAND ----------

# MAGIC %md
# MAGIC ### Definicion Español
# MAGIC <p> Se declaran las colecciones de los nombres en Ingles y su traduccion a Español, de los días, meses y meses abreviados

# COMMAND ----------

# Diccionarios de nombres en inglés y español
dias_ingles_a_espanol = {
    'Monday': 'Lunes', 'Tuesday': 'Martes', 'Wednesday': 'Miércoles', 'Thursday': 'Jueves', 'Friday': 'Viernes', 'Saturday': 'Sábado', 'Sunday': 'Domingo'
}

meses_ingles_a_espanol = {
    'January': 'Enero', 'February': 'Febrero', 'March': 'Marzo', 'April': 'Abril', 'May': 'Mayo', 'June': 'Junio', 'July': 'Julio', 'August': 'Agosto', 'September': 'Septiembre', 'October': 'Octubre', 'November': 'Noviembre', 'December': 'Diciembre'
}

meses_ab_ingles_a_espanol = {
    'Jan':'Ene', 'Feb':'Feb', 'Mar':'Mar', 'Apr':'Abr', 'May':'May', 'Jun':'Jun', 'Jul':'Jul', 'Aug':'Ago', 'Sep':'Sep', 'Oct':'Oct', 'Nov':'Nov', 'Dec':'Dic' 
}

# COMMAND ----------

# MAGIC %md
# MAGIC ### Declaración de Funciones Auxiliares
# MAGIC <p> Tomara el texto y lo traducira a español segun corresponda día, mes o mes abreviado

# COMMAND ----------

# Función para reemplazar nombres de días
def reemplazar_dias(texto):
    for dia_ingles, dia_espanol in dias_ingles_a_espanol.items():
        texto = texto.replace(dia_ingles, dia_espanol)
    return texto

# Función para reemplazar nombres de meses
def reemplazar_meses(texto):
    for mes_ingles, mes_espanol in meses_ingles_a_espanol.items():
        texto = texto.replace(mes_ingles, mes_espanol)
    return texto

# Función para reemplazar nombres de mes cortos
def reemplazar_mes_ab(texto):
    for mes_ingles, mes_espanol in meses_ab_ingles_a_espanol.items():
        texto = texto.replace(mes_ingles, mes_espanol)
    return texto


# COMMAND ----------

# MAGIC %md
# MAGIC ### Declaración de Funcion utf_es
# MAGIC <p> Esta funcion ejecutara las funciones auxiliares y traducira las columnas que se requieran

# COMMAND ----------

def utf_es(cal):
    # Define UDFs for reemplazar_dias and reemplazar_meses functions
    reemplazar_dias_udf = udf(reemplazar_dias, StringType())
    reemplazar_meses_udf = udf(reemplazar_meses, StringType())
    reemplazar_mes_ab_udf = udf(reemplazar_mes_ab, StringType())

    # Apply UDFs to the "Fecha" column
    # cal = cal.withColumn("Fecha_es", reemplazar_dias_udf("Fecha_es"))
    # cal = cal.withColumn("Fecha_es", reemplazar_meses_udf("Fecha_es"))

    # # Apply UDFs to the "Inicio_del_mes" column
    # cal = cal.withColumn("Inicio_del_mes", reemplazar_dias_udf("Inicio_del_mes"))
    # cal = cal.withColumn("Inicio_del_mes", reemplazar_meses_udf("Inicio_del_mes"))

    # # Apply UDFs to the "Inicio_del_mes" column
    # cal = cal.withColumn("Fin_del_mes", reemplazar_dias_udf("Fin_del_mes"))
    # cal = cal.withColumn("Fin_del_mes", reemplazar_meses_udf("Fin_del_mes"))

    # Apply UDFs to the "Mes_corto_y_Año" column
    cal = cal.withColumn("Mes_corto_y_Anio", reemplazar_mes_ab_udf("Mes_corto_y_Anio"))

    return cal

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ejecución de funcion utf_es
# MAGIC <p> Se ejecuta la función para obtener el dataframe traducido a español

# COMMAND ----------

df_Calendario_es = reemplazar_meses(reemplazar_dias(utf_es(df_Calendario_en)))

# COMMAND ----------

# display(df_Calendario_es)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Upsert de Tabla Delta "Calendario"
# MAGIC <p> Obtenemos la tabla original para actualizar la Delta Table 'Calendario' con el dataframe df_Calendario_es recién creado.

# COMMAND ----------

# Limitar a 6 particiones el DF "df_Calendario"
df_Calendario_es = df_Calendario_es.coalesce(1)

# Ruta en el sistema de archivos donde se guardará el archivo Parquet
delta_table_Calendario = 'entprod_mdn.bplay.Calendario'

# Guardar el DataFrame en formato Delta
df_Calendario_es.write.mode("overwrite")\
    .option("overwriteSchema", "true")\
        .saveAsTable(delta_table_Calendario)

# COMMAND ----------

# MAGIC %md
# MAGIC