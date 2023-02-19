from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


import pendulum


from sqlalchemy import create_engine
from sqlalchemy.engine import Connection


def conectar_banco() -> Connection:
    """Executa a conexão com banco de dados ou retorna erro.
    Returns
    -------
    Connection
        Conexão com o banco de dados.
    """
    try:
        connection = sql_connection()
        print("Conexao realizada com sucesso!")
        return connection
    except Exception as error:
        error = str(error)
        print("Conexao nao realizada! " + error)



def sql_connection() -> Connection:
    """
    Função para conectar a um banco de dados PostgreSQL usando o SQLAlchemy.
    """
    user = "postgres"
    password = "senha"
    host = "172.174.139.52"
    port = "5433"
    database = "postgres"
    
    # Cria a string de conexão com o banco de dados.
    url = f"postgresql://{user}:{password}@{host}:{port}/{database}"
    
    # Cria uma engine e conecta ao banco de dados.
    engine = create_engine(url)
    conn = engine.connect()
    
    
    return conn




def consulta():
    
    engine = conectar_banco()
    
    
    import pandas as pd 
    
    query = 'select * from testecru'

    df = pd.read_sql(query,engine)

    print(df[0])




with DAG(
        "conectar_postgres",
        start_date=pendulum.today('UTC').add(days=-1),
        schedule='0 0 * * 1', # executar toda segunda feira
) as dag:
     
    # t1 = PythonOperator(
    #      task_id = 'conecta_banco',
    #      python_callable  = conectar_banco,)


    t2 = PythonOperator(
        task_id = 'consulta',
        python_callable= consulta,)


