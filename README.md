# dataEngineering_final_project_weather
Trabajo final para el curso de 'Data Engineering' en CoderHouse, consiste en crear un pipeline que extraiga datos de una API pública de forma constante combinándolos con información extraída de una base de datos
Colocar los datos extraídos en un Data Warehouse. 
Automatizar el proceso que extraerá, transformará y cargará datos cuantitativos (ejemplo estos son: valores de acciones de la bolsa, temperatura de ciudades seleccionadas, valor de una moneda comparado con el dólar, casos de covid). 
Automatizar el proceso para lanzar alertas (2 máximo) por e-mail en caso de que un valor sobrepase un límite configurado en el código.

# 1ra PreEntrega
Generar un script que funcione como prototipo (MVP) de un ETL para el proyecto final
El script debería extraer datos desde una API en formato JSON para ser manipulado como diccionario utilizando el lenguaje Python
Generar una tabla para ser almacenada en una base de datos a partir de información una API.

# Antes de ejecutar el codigo:
  1. Crear un archivo del tipo .env y completar los datos (AWS_HOST, AWS_DB, AWS_USER, AWS_PASSWORD, AWS_PORT, KEY)
  2. Instalar las librerias que se encuentran en el archivo 'requirements.txt'
