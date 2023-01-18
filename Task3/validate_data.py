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

dag = models.DAG(dag_id='validate_data', default_args=args)


start_pipeline = DummyOperator(
    task_id='start_pipeline',
    dag=dag
)

validate_users = BigQueryOperator(
    task_id='validate_users',
    use_legacy_sql=False,
    params={
        'project_id': project_id,
        'staging_dataset': staging_dataset,
        'dwh_dataset': dwh_dataset
    },
    sql='../data/D_Users_Validate.sql',
    dag=dag
)

merge_users = BigQueryOperator(
    task_id='merge_users',
    use_legacy_sql=False,
    params={
        'project_id': project_id,
        'staging_dataset': staging_dataset,
        'dwh_dataset': dwh_dataset
    },
    sql='../data/D_Users_Merge.sql',
    dag=dag
)

insert_d_videos = BigQueryOperator(
    task_id='insert_d_videos',
    use_legacy_sql=False,
    params={
        'project_id': project_id,
        'staging_dataset': staging_dataset,
        'dwh_dataset': dwh_dataset
    },
    sql='./data/D_Videos_Insert.sql',
    dag=dag
)

loaded_data_to_core = DummyOperator(
    task_id='loaded_data_to_staging',
    dag=dag
)

start_pipeline >> [insert_d_videos, validate_users]

insert_d_videos
validate_users >> merge_users

[insert_d_videos, merge_users] >> loaded_data_to_core
