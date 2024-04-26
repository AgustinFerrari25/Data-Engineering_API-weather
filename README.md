# Data Engineering Project

![Badge finished](https://img.shields.io/badge/STATUS-FINISHED-green)

## Acerca de...
Este proyecto forma parte del trabajo final del curso de 'Data Engineering' realizado en Coderhouse. Consiste en extraer datos de distintas APIs públicas de manera constante y almacenarlos en una Data Warehouse. Luego, se utiliza Airflow para automatizar los procesos y poder monitorear el estado de los flujos de trabajo.

#### Requisitos
- Tener Docker Desktop instalado.
- Tener Postgres o AWS Redshift 

### APIs utilizadas
1. https://www.weatherapi.com/
2. https://www.visualcrossing.com/weather-api

## Cómo ejecutar el codigo?
> **Antes de ejecutar el código**:
>- Completa con tus datos las variables que se encuentran en el archivo `variables.json`
>- Genera una [App password](https://www.getmailbird.com/gmail-app-password/) con tu cuenta de google. Esa contraseña se usara en "EMAIL_PASSWORD"

1. En la terminal, navega hasta la carpeta principal y ejecuta `docker compose up airflow-init` y luego `docker compose up` 
2. Ingresa a `localhost:8080` en el navegador de tu preferencia.
3. Dirígete a 'Admin'> 'Variables' y ve agregando las variables de `variables.json` (foto_referencia_1)
4. Vuelve al menú 'DAGs' y ejecuta `dag_tasks.py`
5. Si algún valor no se encuentra dentro de los parámetros establecidos, recibirás un mail con la información correspondiente.

> **Tener en cuenta**: 
> - Las variables 'COLUMN_NAME y MAX_VALUE' son modificables a gusto. Tener en cuenta que el nombre de 'COLUMN_NAME' tiene que ser igual al de al menos una columna de las APIs
> - La API de 'weatherapi' solo puede devolver el historial de los ultimos 7 dias

## backfill 
>Que es? Backfill es la ejecución de tareas para fechas pasadas. Asegura que todas las ejecuciones anteriores se completen correctamente y que el DAG esté completamente actualizado hasta la fecha actual.

1. En la terminal, navega hasta la carpeta principal y ejecuta `Docker ps -a` (te muestra todos los contenedores de docker) y busca y copia uno que se llame `final_project_weather_api-airflow-scheduler-1` (foto_referencia_2)
2. luego en la terminar escribir `docker exec -it final_project_weather_api-airflow-scheduler-1 bash`
3. Para finalizar escribir `airflow dags backfill -s 2024-04-18 -e 2024-04-20 final_project` Primero va la fecha de inicio y la segunda fecha hace referencia hasta que dia queres hacer el backfill

## Ayuda
Variables recomendadas para una buena ejecución del código:

| KEY  | VALUE |
| ------------- |:-------------:|
| COLUMN_NAME   | temperature   |
| MAX_VALUE     | 20            |
    

### Keys que se pueden utilizar en las APIs
```KEY_FORECAST": "00a9c49c5b9645d3bb9174700241304```
```KEY_WEATHER": "3563PCR63ZQQPPYUWK8JY6GU6```
