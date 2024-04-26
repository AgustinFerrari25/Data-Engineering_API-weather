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
key_weather = Variable.get("KEY_WEATHER")

def get_data_weather(ti, dates):

  url = f'https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/Argentina/{dates}?key={key_weather}'

  try:
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()
  except requests.RequestException as e:
    print("Error fetching data from Weather API:")
    print(e)
    exit()

  forecast_data_list = []

  for day_data in data.get('days', []):
    date = day_data.get('datetime')
    hourly_data = day_data.get('hours', [])

    for hour_data in hourly_data:
      time = hour_data.get('datetime')
      forecast_data_list.append({
        'date': date,
        'time': time,
        'temperature': hour_data.get('temp'),
        'feels_like': hour_data.get('feelslike'),
        'humidity': hour_data.get('humidity'),
        'dew_point': hour_data.get('dew'),
        'precipitation': hour_data.get('precip'),
        'precip_probability': hour_data.get('precipprob'),
        'wind_speed': hour_data.get('windspeed'),
        'wind_direction': hour_data.get('winddir'),
        'pressure': hour_data.get('pressure'),
        'cloud_cover_percentage': hour_data.get('cloudcover'),
        'visibility': hour_data.get('visibility'),
        'condition': hour_data.get('conditions'),
        })
  df=pd.DataFrame.from_dict(forecast_data_list)
  creation_date = datetime.now()

  df['creation_date'] = creation_date

  df['date_time'] = pd.to_datetime(df['date'] + ' ' + df['time'])

  df.drop(['date', 'time'], axis=1, inplace=True)

  df = df[['date_time'] + [col for col in df.columns if col != 'date_time']]
  ti.xcom_push(key='df_weather', value=df)
  return df

def get_db_connection_weather(**kwargs):
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
             CREATE TABLE IF NOT EXISTS weather_per_hour (
              date_time TIMESTAMP,
              temperature FLOAT,
              feels_like FLOAT,
              humidity FLOAT,
              dew_point FLOAT,
              precipitation FLOAT,
              precip_probability FLOAT,
              wind_speed FLOAT,
              wind_direction FLOAT,
              pressure FLOAT,
              cloud_cover_percentage FLOAT,
              visibility FLOAT,
              condition VARCHAR(50),                    
              creation_date TIMESTAMP,  
              PRIMARY KEY (date_time, creation_date)          
            )
        """)
        conn.commit()
        print("Table created successfully")
    except Exception as e:
        print("error connecting or creating table")
        print(e)