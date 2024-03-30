import requests
from config import AWS_HOST, AWS_DB, AWS_USER, AWS_PORT, AWS_PASSWORD, KEY
import pandas as pd
from datetime import datetime
import numpy as np
import psycopg2

#from airflow.models import Variable

host_instance = AWS_HOST
data_base = AWS_DB
user = AWS_USER
port = AWS_PORT
pwd = AWS_PASSWORD
key = KEY

url = f'https://api.weatherapi.com/v1/forecast.json?key={key}&q=Buenos Aires&days=7&aqi=no&alerts=no'

def get_data():
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
    
        for i in range(0, len(hourly_data), 2):
            hour_data = hourly_data[i]
            time = hour_data.get('time')
            forecast_data_list.append({
                'hour': time,
                'temperature_c': hour_data.get('temp_c'),
                'condition': hour_data.get('condition', {}).get('text'),
                'wind_speed_kph': hour_data.get('wind_kph'),
                'wind_direction': hour_data.get('wind_dir'),
                'pressure_mb': hour_data.get('pressure_mb'),
                'humidity_percentage': hour_data.get('humidity'),
                'cloud_cover_percentage': hour_data.get('cloud'),
                'chance_of_rain_percentage': hour_data.get('chance_of_rain'),
                'precipitation_mm': hour_data.get('precip_mm'),
                'uv_index': hour_data.get('uv'),
            })

    df = pd.DataFrame.from_dict(forecast_data_list)

    creation_date = datetime.now()
    df['creation_date'] = creation_date

    return df

def get_db_connection():
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
              hour TIMESTAMP,         
              temperature_c FLOAT,
              condition VARCHAR(50),
              wind_speed_kph FLOAT,
              wind_direction VARCHAR(50),
              pressure_mb FLOAT,
              humidity_percentage INTEGER,
              cloud_cover_percentage INTEGER,
              chance_of_rain_percentage INTEGER,
              precipitation_mm FLOAT,
              uv_index FLOAT,
              creation_date TIMESTAMP,
              PRIMARY KEY (hour, creation_date)           
            )
        """)
        conn.commit()
        print("Table created successfully")
        return conn
    except Exception as e:
        print("error connecting or creating table")
        print(e)


def load_data():
    try:
        conn=get_db_connection()
        df=get_data()
        cur = conn.cursor()
        for index, row in df.iterrows():
            values = [value for value in row]
            insert_query = f"INSERT INTO forecastday_per_hour VALUES ({', '.join(['%s'] * len(values))});"
            cur.execute(insert_query, values)

        conn.commit()
        print("Data inserted successfully")
    
        cur.execute("""
                    DELETE FROM forecastday_per_hour
                    WHERE (hour, creation_date) IN (
                    SELECT hour, MAX(creation_date) 
                    FROM forecastday_per_hour 
                    GROUP BY hour 
                    HAVING COUNT(*) > 1);
                    """)
        conn.commit()
        print("Query executed successfully")
    except Exception as e:
        print("Error creating or inserting data")
        print(e)