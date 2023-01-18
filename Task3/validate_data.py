import airflow
from airflow import models
from airflow.contrib.operators import gcs_to_bq
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator


dwh_dataset = 'DWH'
project_id = 'verdant-petal-374217'
staging_dataset = 'DWH_STAGING'
gs_bucket = 'edu_task4'

args = {
    'owner': 'Airflow',
    'retries': 1,
    'start_date': airflow.utils.dates.days_ago(2),
    'schedule_interval': "0 */3 * * *"
}

dag = models.DAG(dag_id='load_raw_data', default_args=args)


start_pipeline = DummyOperator(
    task_id='start_pipeline',
    dag=dag
)


create_d_users = BigQueryOperator(
    task_id='create_d_users',
    use_legacy_sql=False,
    params={
        'project_id': project_id,
        'staging_dataset': staging_dataset,
        'dwh_dataset': dwh_dataset
    },
    sql='./data/D_Users.sql'
)

create_d_videos = BigQueryOperator(
    task_id='create_d_videos',
    use_legacy_sql=False,
    params={
        'project_id': project_id,
        'staging_dataset': staging_dataset,
        'dwh_dataset': dwh_dataset
    },
    sql='./data/Videos.sql'
)

start_pipeline >> [create_d_users, create_d_videos]
