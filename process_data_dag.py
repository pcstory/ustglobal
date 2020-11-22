from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from google.cloud import bigquery

args = {
    'owner': 'airflow',
}


client = bigquery.Client()
project = 'ustg-296220'
dataset_id = project + '.' + 'movie'


def create_dataset():
    dataset = bigquery.Dataset(dataset_id)
    dataset = client.create_dataset(dataset, timeout=30)


def load_dimension_year():
    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("year_id", "INT64"),
            bigquery.SchemaField("value", "INT64")
        ],
        skip_leading_rows=1,
        source_format=bigquery.SourceFormat.CSV

    )
    uri = "gs://ust-data/processed/d_year.csv"
    table_id = dataset_id + '.' + 'd_year'
    client.delete_table(table_id, not_found_ok=True)
    load_job = client.load_table_from_uri(
        uri, table_id, job_config=job_config
    )
    load_job.result()

    destination_table = client.get_table(table_id)
    print("Loaded {} rows.".format(destination_table.num_rows))



def load_dimension_month():
    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("month_id", "INT64"),
            bigquery.SchemaField("value", "INT64")
        ],
        skip_leading_rows=1,
        source_format=bigquery.SourceFormat.CSV

    )
    uri = "gs://ust-data/processed/d_month.csv"
    table_id = dataset_id + '.' + 'd_month'
    client.delete_table(table_id, not_found_ok=True)
    load_job = client.load_table_from_uri(
        uri, table_id, job_config=job_config
    )
    load_job.result()

    destination_table = client.get_table(table_id)
    print("Loaded {} rows.".format(destination_table.num_rows))


def load_fact():
    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("original_title", "STRING"),
            bigquery.SchemaField("popularity", "STRING"),
            bigquery.SchemaField("release_date", "DATE"),
            bigquery.SchemaField("revenue", "FLOAT64"),
            bigquery.SchemaField("runtime", "FLOAT64"),
            bigquery.SchemaField("title", "STRING"),
            bigquery.SchemaField("vote_average", "FLOAT64"),
            bigquery.SchemaField("vote_count", "FLOAT64"),
            bigquery.SchemaField("genres_list", "STRING"),
            bigquery.SchemaField("year_id", "INT64"),
            bigquery.SchemaField("month_id", "INT64")
        ],
        skip_leading_rows=1,
        source_format=bigquery.SourceFormat.CSV

    )
    uri = "gs://ust-data/processed/f_movie.csv"
    table_id = dataset_id + '.' + 'f_movie'
    client.delete_table(table_id, not_found_ok=True)
    load_job = client.load_table_from_uri(
        uri, table_id, job_config=job_config
    )
    load_job.result()  # Waits for the job to complete.

    destination_table = client.get_table(table_id)  # Make an API request.
    print("Loaded {} rows.".format(destination_table.num_rows))



with DAG(
    dag_id='movie_elt',
    default_args=args,
    schedule_interval=None,
    start_date=days_ago(1)
) as dag:

    op_process_spark = BashOperator(
        task_id='process_spark',
        bash_command='gcloud dataproc jobs submit pyspark gs://asia-northeast1-ust-airflow-41d9f5de-bucket/dags/process.py --cluster ustg-dataproc --region asia-southeast1',
        dag=dag,
    )

    op_load_dimension_year = PythonOperator(
        task_id='load_dimension_year',
        python_callable=load_dimension_year,
        dag=dag,
    )

    op_load_dimension_month = PythonOperator(
        task_id='load_dimension_month',
        python_callable=load_dimension_month,
        dag=dag,
    )

    op_load_fact = PythonOperator(
        task_id='load_fact',
        python_callable=load_fact,
        dag=dag,
    )

    op_process_spark >> [op_load_dimension_year,op_load_dimension_month] >> op_load_fact
