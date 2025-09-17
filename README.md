Análisis de jugadores en Databricks con PySpark

Este proyecto tiene como objetivo el análisis del comportamiento de jugadores a lo largo del tiempo y la aplicación de reglas de negocio específicas para la detección de casos de interés. El desarrollo se realiza en Databricks, utilizando PySpark como motor principal para el procesamiento de grandes volúmenes de datos.

📌 Funcionalidades principales

Evolución mensual del jugador

Se procesa un dataframe que contiene la evolución de cada jugador por partner_id, partner_player_id, #periodo, periodo y status.

Se generan nuevas columnas para identificar hitos en el ciclo de vida del jugador, como:

FTD (First Time Deposit)

Churn o Abandono

Recupero

Fidelización

Se incorpora una lógica para marcar con 5 los periodos en los que el jugador vuelve a tener actividad luego de más de 3 meses de inactividad.

Detección de contraseñas duplicadas

Se identifica si existen contraseñas repetidas entre jugadores.

Se crea una nueva columna llamada Caso8 con las siguientes reglas:

Si la contraseña está duplicada, se asigna el valor 3.

Si alguna de las cuentas con la misma contraseña tiene status_id = 7, entonces todas las cuentas con esa contraseña reciben el valor 5 en lugar de 3.

Procesamiento distribuido

Se aprovecha el poder de PySpark para manejar datasets grandes de manera eficiente.

Se utilizan transformaciones como groupBy, window functions, when/otherwise, y operaciones con reduce para la unión dinámica de dataframes.

⚙️ Tecnologías utilizadas

Databricks

Apache Spark
 (PySpark API)

Python 3.x
