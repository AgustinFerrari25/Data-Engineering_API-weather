FROM python:3.12.0

LABEL author='Agustin'

RUN mkdir -p /home/app

WORKDIR /home/app

COPY requirements.txt .
COPY forecast_per_hour.py .
COPY config.py .
COPY sample-dot-env .env

RUN pip install -r requirements.txt


CMD ["python","-u", "forecast_per_hour.py"]