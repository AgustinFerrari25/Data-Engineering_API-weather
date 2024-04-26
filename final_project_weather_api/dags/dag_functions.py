from airflow.models import Variable
import psycopg2
import smtplib
from email.message import EmailMessage

host_instance = Variable.get("AWS_HOST")
data_base = Variable.get("AWS_DB")
user = Variable.get("AWS_USER")
port = Variable.get("AWS_PORT")
pwd = Variable.get("AWS_PASSWORD")

email_sender = Variable.get("EMAIL_SENDER")
email_password = Variable.get("EMAIL_PASSWORD")
receiver_email = Variable.get("EMAIL_RECEIVER")

smtp_server = 'smtp.gmail.com'

def load_data(table_name, xcom_keys, **kwargs):
    ti=kwargs['ti']
    df = None

    for key in xcom_keys:
        df = ti.xcom_pull(key=key, task_ids=None, include_prior_dates=True)
        if df is not None:
            break 

    if df is None:
        print("No se pudo encontrar ningún DataFrame en los XCom keys proporcionados.")
        return
    
    try:
        conn = psycopg2.connect(
            host=host_instance,
            dbname=data_base,
            user=user,
            password=pwd,
            port=port
        )
        cur = conn.cursor()
        for index, row in df.iterrows():
            values = [value for value in row]
            insert_query = f"INSERT INTO {table_name} VALUES ({', '.join(['%s'] * len(values))});"
            cur.execute(insert_query, values)

        conn.commit()
        print("Data inserted successfully")
    
        cur.execute(f"""
                    DELETE FROM {table_name}
                    WHERE (date_time, creation_date) IN (
                    SELECT date_time, MAX(creation_date) 
                    FROM {table_name} 
                    GROUP BY date_time 
                    HAVING COUNT(*) > 1);
                    """)
        conn.commit()
        print("Query executed successfully")
    except Exception as e:
        print("Error creating or inserting data")
        print(e)

def alert_mail(table_name, column_name, max_value, **kwargs):
    try:
        conn = psycopg2.connect(
            host=host_instance,
            dbname=data_base,
            user=user,
            password=pwd,
            port=port
        )
        cur = conn.cursor()
        column_name_str = str(column_name)
        if column_name_str is not None:
            if max_value is not None:
                cur.execute(f"SELECT * FROM {table_name} WHERE {column_name_str} > %s", (max_value,))
            else:
                return
            rows=cur.fetchall()
            print(cur.description)

            value_index = None
            for idx, desc in enumerate(cur.description):
                if desc.name == column_name:
                    value_index = idx
                    break

            if value_index is not None:
                x=smtplib.SMTP(smtp_server,587)
                x.starttls()
                x.login(email_sender,email_password) 
                subject=f'Alerta, Valor de la columna {column_name_str} excede el limite'
                body=f"Se han encontrado valores de la tabla {table_name} en la columna {column_name_str} que exceden los limites \n"
                for i in rows:
                    value = i[value_index]  
                    body += f"Valor: {value}\n"
                
                message=EmailMessage()
                message['From']=email_sender
                message['To']=receiver_email
                message['Subject']= subject
                message.set_content(body)

                x.sendmail(email_sender, receiver_email, message.as_string())
                print("Email sent successfully")
                x.quit()
            else:
                print(f"La columna {column_name} no existe en la tabla.")
    except Exception as exception:
        print(exception)
        print('Failure')