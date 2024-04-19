from time import sleep
from airflow.decorators import dag,task
from datetime import datetime


@dag(
        dag_id="bootcamp_dag",
        description="dag feita no bootcamp",
        schedule="5 0 * 8 *",
        start_date=datetime(2024,4,18),
        catchup=False
)
def pipeline():
    @task
    def primeira_atividade():
        print("Primeira atividade")
        sleep(2)
    @task
    def segunda_atividade():
        print("Primeira atividade")
        sleep(2)
    @task
    def terceira_atividade():
        print("Primeira atividade")
        sleep(2)
    @task
    def quarta_atividade():
        print("Primeira atividade")
        sleep(2)

    t1 = primeira_atividade()
    t2 = segunda_atividade()
    t3 = terceira_atividade()
    t4 = quarta_atividade()

    t1 >> t2 >> t3 >> t4
pipeline()