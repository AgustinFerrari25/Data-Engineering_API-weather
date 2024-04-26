FROM apache/airflow:2.8.3
LABEL author='Agustin'

USER airflow

COPY requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

ENV PATH="/opt/airflow/src:/opt/airflow/dags:${PATH}"
ENV PYTHONPATH="/opt/airflow/src:/opt/airflow/dags:${PYTHONPATH}"

ENTRYPOINT [ "/usr/in/dumb-init", "--", "/ENTRYPOINT" ]
CMD []