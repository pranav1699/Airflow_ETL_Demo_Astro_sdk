from airflow.decorators import dag, task
from datetime import datetime
from astro import sql as aql
from astro.table import Table
from astro.sql import transform
from astro.sql.operators import append, merge,raw_sql


@transform
def enriched_customer_purchase(region_table : Table,products_table : Table,customer_purchase_table : Table):
    return """
    select c.id, c.name, r.name as region, p.product, c.qty 
    from {{customer_purchase_table}} c 
    join {{region_table}} r on r.id = c.region 
    join {{products_table}} p on p.id = c.product ;
    """

@transform
def region_purchase(enriched_customer_purchase : Table):
    return """
    select region , count(*) as orders 
    from {{enriched_customer_purchase}}
    group by region ;
    """

@transform
def product_purchase(enriched_customer_purchase : Table):
    return """
    select product , count(*) as orders 
    from {{enriched_customer_purchase}}
    group by product ;
    """


@dag(
    dag_id='customer_purchase_demo',
    schedule='@daily',
    start_date=datetime(2024,1,1),
    catchup=False,
)


def customer_purchase_demo():
    region_table = aql.Table(
        name='region',
        conn_id='postgres',
        temp=False
    )
    products_table = aql.Table(
        name='products',
        conn_id='postgres',
        temp=False
    )
    customer_purchase_table = aql.Table(
        name='customer_purchase',
        conn_id='postgres',
        temp=False
    )

    enriched_customer_purchase_table = enriched_customer_purchase(
       region_table = region_table,
       products_table =  products_table,
       customer_purchase_table =  customer_purchase_table,
       output_table = Table(
           name='enriched_customer_purchase',
           temp=False
           )

    )

    region_purchase_table = region_purchase(
        enriched_customer_purchase = enriched_customer_purchase_table,
         output_table = Table(
             name="region_purchase",
             temp=False
         ) 
    )

    product_purchase_table = product_purchase(
        enriched_customer_purchase = enriched_customer_purchase_table,
         output_table = Table(
             name="product_purchase",
             temp=False
         ) 
    )



customer_purchase_demo()