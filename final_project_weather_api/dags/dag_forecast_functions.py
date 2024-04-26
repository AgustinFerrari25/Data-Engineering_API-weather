import requests
from airflow.models import Variable
import pandas as pd
from datetime import datetime
import psycopg2

host_instance = Variable.get("AWS_HOST")
data_base = Variable.get("AWS_DB")
user = Variable.get("AWS_USER")
port = Variable.get("AWS_PORT")
pwd = Variable.get("AWS_PASSWORD")
key_forecast = Variable.get("KEY_FORECAST")

def get_data_forecast(ti, dates):
    url= f'http://api.weatherapi.com/v1/history.json?key={key_forecast}&q=Buenos Aires&dt={dates}'
    #url = f'https://api.weatherapi.com/v1/forecast.json?key={key_forecast}&q=Buenos Aires&days=10&aqi=no&alerts=no'
    try:
        response = requests.get(url)
        response.raise_for_status()  
        data = response.json()
    except requests.RequestException as e:
        print("Error fetching data from Weather API:")
        print(e)
        exit()

    forecast_data_list = []

    for forecast_day in data.get('forecast', {}).get('forecastday', []):
        hourly_data = forecast_day.get('hour', [])
        date = forecast_day.get('date')
    
        for i in range(0, len(hourly_data), 1):
            hour_data = hourly_data[i]
            time = hour_data.get('time')
            forecast_data_list.append({
                'date_time': time,
                'temperature': hour_data.get('temp_c'),
                'condition': hour_data.get('condition', {}).get('text'),
                'wind_speed': hour_data.get('wind_kph'),
                'wind_direction': hour_data.get('wind_dir'),
                'pressure': hour_data.get('pressure_mb'),
                'humidity': hour_data.get('humidity'),
                'cloud_cover_percentage': hour_data.get('cloud'),
                'chance_of_rain_percentage': hour_data.get('chance_of_rain'),
                'precipitation': hour_data.get('precip_mm'),
                'uv_index': hour_data.get('uv'),
            })

    df = pd.DataFrame.from_dict(forecast_data_list)
    creation_date = datetime.now()
    
    df['creation_date'] = creation_date
    ti.xcom_push(key='df_forecast', value=df)
    return df

def get_db_connection_forecast(**kwargs):
    try:
        conn = psycopg2.connect(
            host=host_instance,
            dbname=data_base,
            user=user,
            password=pwd,
            port=port
        )
        print("successful connection to AWS Redshift")
        cur = conn.cursor()
        cur.execute("""
             CREATE TABLE IF NOT EXISTS forecastday_per_hour (
              date_time TIMESTAMP,         
              temperature FLOAT,
              condition VARCHAR(50),
              wind_speed FLOAT,
              wind_direction VARCHAR(50),
              pressure FLOAT,
              humidity INTEGER,
              cloud_cover_percentage INTEGER,
              chance_of_rain_percentage INTEGER,
              precipitation FLOAT,
              uv_index FLOAT,
              creation_date TIMESTAMP,
              PRIMARY KEY (date_time, creation_date)           
            )
        """)
        conn.commit()
        print("Table created successfully")
    except Exception as e:
        print("error connecting or creating table")
        print(e)