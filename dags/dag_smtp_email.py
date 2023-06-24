from datetime import datetime
from email import message
from airflow.models import DAG, Variable
from airflow.operators.python_operator import PythonOperator

import smtplib

def enviar():
    try:
        x=smtplib.SMTP('smtp.gmail.com',587)
        x.starttls()
        x.login('lucashrossi@gmail.com','sttnytugzvsrngmt')
        subject='API Air Quality'
        body_text='Datos Bajados!'
        message='Subject: {}\n\n{}'.format(subject,body_text)
        x.sendmail('lucashrossi@gmail.com','lucashrossi@gmail.com',message)
        print('Exito')
    except Exception as exception:
        print(exception)
        print('Failure')

default_args={
    'owner': 'Hernan',
    'start_date': datetime(2023,6,11)
}

with DAG(
    dag_id='dag_smtp_email_automatico',
    default_args=default_args,
    schedule_interval='@daily') as dag:

    tarea_1=PythonOperator(
        task_id='dag_envio',
        python_callable=enviar
    )

    tarea_1
