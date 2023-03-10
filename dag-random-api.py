from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

import pandas as pd
import requests as rq 
import pendulum
import json



def get_data(ti):
    
    lista = []


    for i in range(0,20): 
        response = rq.get('https://randomuser.me/api/')
    
        json =  response.json()

        json = json.get('results')

        json = json[0]
      
        lista.append(json)
        
        
    #df = pd.json_normalize(lista)
    
    return lista
   



def print_a_data(ti):
    
    lista = ti.xcom_pull(task_ids="ext_data")
    

    df = pd.json_normalize(lista)

    print(df.iloc[0,1])
   



    
   


    
with DAG(
         "random_users",
         start_date=pendulum.today('UTC').add(days=-1),
         schedule='0 0 * * 1', # executar toda segunda feira
 ) as dag:


    ext = PythonOperator(
         task_id = 'ext_data',
         python_callable  = get_data,)


    print_data = PythonOperator(
        task_id = 'print_data',
        python_callable= print_a_data,)


ext >> print_data