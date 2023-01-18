import airflow
from airflow import models
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.contrib.operators.bigquery_check_operator import BigQueryCheckOperator
from airflow.contrib.operators import gcs_to_bq


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

load_users = gcs_to_bq.GoogleCloudStorageToBigQueryOperator(
    task_id='load_users',
    bucket=f'{gs_bucket}',
    source_objects=['raw_data/users.csv'],
    source_format='CSV',
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

move_users_to_archive = GCSToGCSOperator(
    task_id="move_users_file",
    source_bucket=f'{gs_bucket}',
    source_object="raw_data/users.csv",
    destination_bucket=f'{gs_bucket}',
    destination_object="archive/users/users.csv",
    move_object=True,
    dag=dag
)

move_videos_to_archive = GCSToGCSOperator(
    task_id="move_videos_file",
    source_bucket=f'{gs_bucket}',
    source_object="raw_data/videos.csv",
    destination_bucket=f'{gs_bucket}',
    destination_object="archive/videos/videos.csv",
    move_object=True,
    dag=dag
)

move_events_to_archive = GCSToGCSOperator(
    task_id="move_events_file",
    source_bucket=f'{gs_bucket}',
    source_object="raw_data/events.jsonl",
    destination_bucket=f'{gs_bucket}',
    destination_object="archive/events/events.jsonl",
    move_object=True,
    dag=dag
)

start_pipeline >> [load_users, load_videos, load_events]

load_users >> check_users >> move_users_to_archive
load_videos >> check_videos >> move_videos_to_archive
load_events >> move_events_to_archive

[move_users_to_archive, move_videos_to_archive, move_events_to_archive] >> loaded_data_to_staging