from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils import timezone


def _world():
    return "World"


with DAG(
    dag_id="everyday",                  #  <------  เวลาสร้าง id ชื่อ id ควรเหมือนกับชื่อไฟล์
    schedule="@daily",                  #  <------  Every DAY
    start_date=timezone.datetime(2024, 3, 10),
    catchup=False,
    tags=["DEB", "Skooldio"],
):

    hello = BashOperator(
        task_id="hello",
        bash_command="echo 'Hello'",     #  <------  Batch operator
    )

    world = PythonOperator(
        task_id="world",
        python_callable=_world,          #  <------  Python operator
    )

    hello >> world