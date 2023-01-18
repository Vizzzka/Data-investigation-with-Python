import airflow
from airflow import models
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.bigquery_check_operator import BigQueryCheckOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators import gcs_to_bq


staging_dataset = 'DWH_STAGING'
gs_bucket = 'edu_task4'
dwh_dataset = 'DWH'
project_id = 'verdant-petal-374217'

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

load_users = gcs_to_bq.GoogleCloudStorageToBigQueryOperator(
    task_id='load_users',
    bucket=f'{gs_bucket}',
    source_objects=['raw_data/users.csv'],
    source_format='CSV',
    schema_object='../schemas/users_schema.json',
    skip_leading_rows=1,
    destination_project_dataset_table=f'{staging_dataset}.Users',
    write_disposition='WRITE_TRUNCATE',
    dag=dag)


load_videos = gcs_to_bq.GoogleCloudStorageToBigQueryOperator(
    task_id='load_videos',
    bucket=f'{gs_bucket}',
    source_objects=['raw_data/videos.csv'],
    source_format='CSV',
    skip_leading_rows=1,
    destination_project_dataset_table=f'{staging_dataset}.Videos',
    write_disposition='WRITE_TRUNCATE',
    dag=dag)


load_events = gcs_to_bq.GoogleCloudStorageToBigQueryOperator(
    task_id='load_events',
    bucket=f'{gs_bucket}',
    source_objects=['raw_data/events.jsonl'],
    source_format="NEWLINE_DELIMITED_JSON",
    destination_project_dataset_table=f'{staging_dataset}.Events',
    create_disposition='CREATE_IF_NEEDED',
    schema_fields=[
        {'name': 'user_id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'video_id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'event', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'timestamp', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'tags', 'type': 'STRING', 'mode': 'REPEATED'},
        {'name': 'comment', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'events', 'type': 'JSON', 'mode': 'NULLABLE'},
    ],
    write_disposition='WRITE_TRUNCATE',
    dag=dag)

loaded_data_to_staging = DummyOperator(
    task_id='loaded_data_to_staging',
    dag=dag
)

check_users = BigQueryCheckOperator(
    task_id='check_users',
    use_legacy_sql=False,
    sql=f'SELECT count(*) FROM `{staging_dataset}.Users`',
    dag=dag

)

check_videos = BigQueryCheckOperator(
    task_id='check_videos',
    use_legacy_sql=False,
    sql=f'SELECT count(*) FROM `{staging_dataset}.Videos`',
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

start_pipeline >> [load_users, load_videos, load_events]

load_users >> check_users
load_videos >> check_videos

[check_users, check_videos] >> loaded_data_to_staging

loaded_data_to_staging >> [create_d_users, create_d_videos]