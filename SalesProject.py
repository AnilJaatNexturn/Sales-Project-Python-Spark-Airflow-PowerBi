from datetime import timedelta,datetime
from airflow import DAG

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from pyspark.sql import SparkSession
from airflow.operators.email_operator import EmailOperator


default_args={
        'owner':'aniljat',
        'start_date':datetime(2023,7,13),
        'retries':3,
        'retry_delay':timedelta(minutes=5)
        }

dags=DAG(
        'SalesProject',
        default_args=default_args,
        description='analysis sales data with saprk airflow',
        schedule_interval='* * * * *',
        catchup=False,
        tags=['analysis Sales Data']
        )

def fun1():
    spark=SparkSession.builder.appName("Spark Dataframe1").getOrCreate()
    df = spark.read.csv("/root/airflow/inputfiles/Sales_data_final.csv",header=True)
    df.createOrReplaceTempView("table1")
    #total sales data
    df1=spark.sql("SELECT * FROM table1")
    df1.write.csv("/root/airflow/OutputSales/AllData.csv", header=True, mode="overwrite")
def fun2():
    spark=SparkSession.builder.appName("Spark Dataframe2").getOrCreate()
    df = spark.read.csv("/root/airflow/inputfiles/Sales_data_final.csv",header=True)
    df.createOrReplaceTempView("table2")
    #iphone data
    df1=spark.sql("SELECT *  FROM table2 where Product='iPhone'")
    df1.write.csv("/root/airflow/OutputSales/iPhoneData.csv", header=True, mode="overwrite")



send_email = EmailOperator(
        task_id='send_email',
        to='anilkumarjat06@gmail.com',
        subject='ingestion complete',
        html_content="task done vrify email",
        dag=dags)

start_task=DummyOperator(task_id='start_task',dag=dags)
end_task=DummyOperator(task_id='end_task',dag=dags)
spark_task1=PythonOperator(task_id='AllData_task1',python_callable=fun1,dag=dags)
spark_task2=PythonOperator(task_id='iPhone_task2',python_callable=fun2,dag=dags)
start_task>>[spark_task1,spark_task2]>>end_task>>send_email

