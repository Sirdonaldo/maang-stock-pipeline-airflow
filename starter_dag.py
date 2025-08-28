from airflow.decorators import dag
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import datetime, timedelta
from include.eczachly.trino_queries import execute_trino_query, run_trino_query_dq_check
from airflow.models import Variable
tabular_credential = Variable.get("TABULAR_CREDENTIAL")
polygon_api_key = Variable.get('POLYGON_CREDENTIALS', deserialize_json=True)['AWS_SECRET_ACCESS_KEY']
# TODO make sure to rename this if you're testing this dag out!
schema = 'sirdonaldo'
def create_prices_table_sql(schema: str) -> str:
    return f"""
    CREATE TABLE IF NOT EXISTS academy.{schema}.maang_prices (
    symbol  VARCHAR,
    ts      TIMESTAMP(3) WITH TIME ZONE,
    open         DOUBLE,
    high         DOUBLE,
    low          DOUBLE,
    close        DOUBLE,
    volume       BIGINT,
    vwap         DOUBLE,
    trade_count  BIGINT
    )
    WITH (
        format = 'PARQUET',
        partitioning = ARRAY ['day(ts)']
        )
    """

def create_staging_table_sql(schema: str) -> str:
    return f"""
    CREATE TABLE IF NOT EXISTS {staging_table} (
    symbol  VARCHAR,
    ts      TIMESTAMP(3) WITH TIME ZONE,
    open    DOUBLE,
    high    DOUBLE,
    low     DOUBLE,
    close   DOUBLE,
    volume   BIGINT,
    vwap     DOUBLE,
    trade_count  BIGINT
    )
    WITH (
    format = 'PARQUET',
    partitioning = ARRAY ['day(ts)']
    )
    """
@dag(
    description="A dag for your homework, it takes polygon data in and cumulates it",
    default_args={
        "owner": schema,
        "start_date": datetime(2025, 1, 9),
        "retries": 0,
        "execution_timeout": timedelta(hours=1),
    },
    start_date=datetime(2025, 1, 9),
    max_active_runs=1,
    schedule_interval="@daily",
    catchup=True,
    tags=["community"],
)
def starter_dag():
    maang_stocks = ['AAPL', 'AMZN', 'NFLX', 'GOOGL', 'META']
    production_table = f'{schema}.user_web_events_cumulated'
    staging_table = production_table + '_stg_{{ ds_nodash }}'
    cumulative_table = f'{schema}.your_table_name'
    yesterday_ds = '{{ yesterday_ds }}'
    ds = '{{ ds }}'

    # todo figure out how to load data from polygon into Iceberg
    import requests
    from datetime import datetime as dt

    def load_data_from_polygon(table):
        # List of MAANG stocks
        symbols = ["AAPL", "AMZN", "NFLX", "GOOGL", "META"]
        today = dt.utcnow().date()

        for symbol in symbols:
            url = (
                f"https://api.polygon.io/v2/aggs/ticker/{symbol}/prev"
                f"?adjusted=true&apiKey={polygon_api_key}"
            )
            response = requests.get(url)
            response = raise_for_status()
            data = response.json().get("results", [])

            # Skip if no data
            if not data:
                continue
            # Prepare INSERT statement
            for record in data:
                query = f"""
                INSERT INTO {table} VALUES (
                 '{symbol}',
                 TIMESTAMP '{dt.utcfromtimestamp(record['t'] / 1000).isoformat()}',
                 {record['o']},
                 {record['h']},
                 {record['l']},
                 {record['c']},
                 {record['v']},
                 {record['vw']},
                 {record['n']}
                )
                """
                execute_trino_query(query)

    # TODO create schema for daily stock price summary table
    create_daily_step = PythonOperator(
        task_id="create_daily_step",
        python_callable=execute_trino_query,
        op_kwargs={
            'query': create_prices_table_sql(schema),
        }
    )

    # TODO create the schema for your staging table
    create_staging_step = PythonOperator(
        task_id="create_staging_step",
        python_callable=execute_trino_query,
        op_kwargs={
            'query': create_staging_table_sql(staging_table),
        }
    )


    # todo make sure you load into the staging table not production
    load_to_staging_step = PythonOperator(
        task_id="load_to_staging_step",
        python_callable=load_data_from_polygon,
        op_kwargs={
            'table': staging_table
        }
    )

    # TODO figure out some nice data quality checks
    run_dq_check = PythonOperator(
        task_id="run_dq_check",
        python_callable=run_trino_query_dq_check,
        op_kwargs={
            'query': f"""
                SELECT
                    COUNT(*) AS row_count
                    COUNT_IF(symbol IS NULL) AS null_symbols,
                    COUNT_IF(ts IS NULL) AS null_timestamps
                    FROM {staging_table}
                """
        }
    )


    # todo make sure you clear out production to make things idempotent
    clear_step = PythonOperator(
        task_id="clear_step",
        depends_on_past=True,
        python_callable=execute_trino_query,
        op_kwargs={
            'query': f"""
            DELETE FROM {production_table}
            WHERE ds = DATE('{{{{ds}}}}')
        """
        }
    )

    exchange_data_from_staging = PythonOperator(
        task_id="exchange_data_from_staging",
        python_callable=execute_trino_query,
        op_kwargs={
            'query': """
                          INSERT INTO {production_table}
                          SELECT * FROM {staging_table} 
                          WHERE ds = DATE('{ds}')
                      """.format(production_table=production_table,
                                 staging_table=staging_table,
                                 ds='{{ ds }}')
        }
    )

    # TODO do not forget to clean up
    drop_staging_table = PythonOperator(
        task_id="drop_staging_table",
        python_callable=execute_trino_query,
        op_kwargs={
            'query': f"DROP TABLE IF EXISTS {staging_table}"
        }
    )

    # TODO create the schema for your cumulative table
    def create_cumulative_table_sql(cumulative_table: str) -> str:
        return f"""
        CREATE TABLE IF NOT EXISTS {cumulative_table} (
          symbol        VARCHAR,
          ts            TIMESTAMP(3) WITH TIME ZONE,
          open          DOUBLE,
          high          DOUBLE,
          low           DOUBLE,
          close         DOUBLE,
          volume        BIGINT,
          vwap          DOUBLE,
          trade_count   BIGINT
        )
        WITH (
          format = 'PARQUET',
          partitioning = ARRAY['day(ts)']
        )
        """

    create_cumulative_step = PythonOperator(
        task_id="create_cumulative_step",
        python_callable=execute_trino_query,
        op_kwargs={
            'query': create_cumulative_table_sql(cumulative_table)
        }
    )

    clear_cumulative_step = PythonOperator(
        task_id="clear_cumulative_step",
        depends_on_past=True,
        python_callable=execute_trino_query,
        op_kwargs={
            'query': f"""
                DELETE FROM {cumulative_table}
                WHERE ts >= DATE('{{{{ ds }}}}') - INTERVAL '6' DAY
                  AND ts <= DATE('{{{{ ds }}}}')
            """
        }
    )

    # TODO make sure you create array metrics for the last 7 days of stock prices
    cumulate_step = PythonOperator(
        task_id="cumulate_step",
        depends_on_past=True,
        python_callable=execute_trino_query,
        op_kwargs={
            'query': f"""
                INSERT INTO {cumulative_table}
                SELECT * 
                FROM {production_table} p
                FULL OUTER JOIN {other_table}o ON p.symbol =  o.symbol AND p.ts = o.ts
                WHERE p.ts >= DATE('{{{{ds}}}}') - INTERVAL '6' DAY
                AND p.ts <= DATE('{{{{ ds }}}}')
            """
        }
    )

    # TODO figure out the right dependency chain

starter_dag()


create_daily_step >> create_staging_step >> load_to_staging_step >> run_dq_check >> exchange_data_from_staging >> drop_staging_table >> cumulate_step
