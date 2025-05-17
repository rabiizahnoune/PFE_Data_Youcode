from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=60),
}

# Define the DAG
with DAG(
    'gold_news_pipeline',
    default_args=default_args,
    description='Pipeline to ingest gold market news with Kafka, Spark, and Cassandra',
    schedule_interval=None,  # Run every 10 minutes
    start_date=days_ago(1),
    catchup=False,
) as dag:

    # Task 1: Run the Kafka producer in the Kafka container
    run_producer = BashOperator(
        task_id='run_producer',
        bash_command='docker exec gold_price_project-kafka-1 python3 /scripts/news_producer.py',

        dag=dag
    )

    # Task 2: Run the Spark consumer in the Spark container
    run_consumer = BashOperator(
        task_id='run_consumer',
        bash_command='docker exec gold_price_project-spark-1 bash -c "export PYSPARK_PYTHON=/usr/bin/python3.8 && spark-submit --conf spark.pyspark.python=/usr/bin/python3.8 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,com.datastax.spark:spark-cassandra-connector_2.12:3.5.0 /scripts/spark_news.py"',
        retries=3,  # Ajout de tentatives en cas d'échec
        retry_delay=timedelta(minutes=5),  # Délai entre les tentatives
        dag=dag
)

    # Task 3: Check data in Cassandra
    def check_cassandra_data(**kwargs):
        # Connect to Cassandra
        cluster = Cluster(['cassandra'], port=9042)
        session = cluster.connect('gold_news')
        
        # Query to count recent articles
        query = "SELECT COUNT(*) FROM articles WHERE ingestion_time >= %s ALLOW FILTERING"
        ingestion_time = (kwargs['execution_date'] - timedelta(minutes=15)).isoformat()
        statement = SimpleStatement(query)
        result = session.execute(statement, [ingestion_time])
        count = result.one()[0]
        
        # Check if data was inserted
        if count > 0:
            print(f"Success: Found {count} articles in Cassandra since {ingestion_time}")
        else:
            raise ValueError(f"Failure: No articles found in Cassandra since {ingestion_time}")
        
        # Close connection
        cluster.shutdown()

    # check_cassandra = PythonOperator(
    #     task_id='check_cassandra',
    #     python_callable=check_cassandra_data,
    #     provide_context=True,
    #     dag=dag,
    # )

    # Define task dependencies
    run_producer >> run_consumer 