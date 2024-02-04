from airflow.decorators import dag,task
from datetime import datetime
from astro import sql as aql
from astro.table import Table
from astro.sql import transform
from astro.sql.operators import append, merge,raw_sql


sql_get_max_id = """
select max(id) from customer_purchase ;
"""
@task
def get_max_id(customer_purchase_maxid):
        max_id = int(customer_purchase_maxid[0][0])
        return max_id

@dag(
    dag_id="demo_checkpointing",
    schedule="@daily",
    start_date=datetime(2024,1,1),
    catchup=False
)


def demo_checkpointing():


    customer_purchase_maxid = aql.get_value_list(
        sql= sql_get_max_id,
        conn_id='postgres'
    )

    print(get_max_id(customer_purchase_maxid=customer_purchase_maxid))

    


demo_checkpointing()