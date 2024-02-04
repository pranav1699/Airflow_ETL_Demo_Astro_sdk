from airflow.decorators import dag , task
from datetime import datetime
from astro import sql as aql
from astro.sql.table import Table
from astro.sql.operators import raw_sql ,append
from astro.files import File
from astro.sql import transform


@transform
def select_top_user(table1 :Table):
    return """
        select * from {{table1}} order by id desc limit 1 ;
        """


@dag(start_date=datetime(2024,1,1),
     schedule='@daily',
     catchup=False,
     )

def demo():

    load_user_table = aql.Table(
        name='users',
        conn_id='postgres',
        temp=False
    )

    get_top_user = select_top_user(
        table1 = load_user_table,
        output_table = Table("top_user",temp=False)
        )


demo()
