from airflow.decorators import dag, task
from datetime import datetime
from astro import sql as aql
from astro.table import Table
from astro.sql import transform
from astro.sql.operators import append, merge,raw_sql

sql_get_max_id = """
select id from max_id_checkpoint ;
"""

@task
def get_max_id(customer_purchase_maxid):
        max_id = customer_purchase_maxid[0]
        return max_id


@transform
def enriched_customer_purchase(region_table : Table,
                               products_table : Table,
                               customer_purchase_table : Table,
                               max_id : Table):
    return """
    select c.id, c.name, r.name as region, p.product, c.qty 
    from {{customer_purchase_table}} c 
    join {{region_table}} r on r.id = c.region 
    join {{products_table}} p on p.id = c.product
    where c.id > (select id from {{max_id}})
    ;
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

@transform
def max_id_checkpoint(enriched_customer_purchase : Table):
    return """
    select max(id) as id from {{enriched_customer_purchase}} ;
    """

@dag(
    dag_id='customer_purchase_demo_v2',
    schedule='@daily',
    start_date=datetime(2024,1,1),
    catchup=False,
)


def customer_purchase_demo_v2():
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

    max_id_checkpoint_table = aql.Table(
        name='max_id_checkpoint',
        conn_id='postgres',
        temp=False
    )

    enriched_customer_purchase_table_db = aql.Table(
        name='enriched_customer_purchase',
        conn_id='postgres',
        temp=False
    )

    # customer_purchase_maxid = aql.get_value_list(
    #     sql= sql_get_max_id,
    #     conn_id='postgres'
    # )

    # max_id = get_max_id(customer_purchase_maxid=customer_purchase_maxid)


    enriched_customer_purchase_table = enriched_customer_purchase(
       region_table = region_table,
       products_table =  products_table,
       customer_purchase_table =  customer_purchase_table,
       max_id = max_id_checkpoint_table
    )

    enriched_customer_purchase_table_append = aql.append(
        source_table=enriched_customer_purchase_table,
        target_table=enriched_customer_purchase_table_db
    )

    region_purchase_table = region_purchase(
        enriched_customer_purchase = enriched_customer_purchase_table_append,
         output_table = Table(
             name="region_purchase",
             temp=False
         ) 
    )

    product_purchase_table = product_purchase(
        enriched_customer_purchase = enriched_customer_purchase_table_append,
         output_table = Table(
             name="product_purchase",
             temp=False
         ) 
    )

    max_id_checkpoint_table = max_id_checkpoint(
        enriched_customer_purchase = enriched_customer_purchase_table_append,
         output_table = Table(
             name="max_id_checkpoint",
             temp=False
         ) 
    )

    aql.cleanup()



customer_purchase_demo_v2()