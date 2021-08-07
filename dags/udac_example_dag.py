from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from operators.stage_redshift import StageToRedshiftOperator 

from airflow.operators import LoadFactOperator
from airflow.operators import LoadDimensionOperator
from airflow.operators import DataQualityOperator

from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args= {
    'owner': 'udacity',
    'start_date': datetime(2021, 8, 7),
    # Depenndencies on past runs: none
    'depend_on_past':False,
    # On failure, the task are retried 3 times:
    'retries': 3,
    # Retries happen every 5 minutes:
    'retry_delay': timedelta(minutes=5),
    # Catchup is turned off
    'catchup': False,
    # Do not email on retry:
    'email_on_retry':False
}

#log_on_s3 = "log-data/2018/11"
log_on_s3 = "log_data"
#log_on_s3 = "log_json_path.json"
song_on_s3 = "song_data/A/A/A"
s3_bucket ="udacity-dend"


dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          # schedule_interval='0 * * * *'
          schedule_interval='@hourly'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)


stage_events_to_redshift = StageToRedshiftOperator(
    task_id="Load_log_file_to_Redshift",
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_events",
    s3_bucket=s3_bucket,
    s3_key=log_on_s3,
    #file_format='log_json_path.json',
    #json_path = 's3://udacity-dend/log_json_path.json',
    #region = "us-west-2",
    file_format = "JSON 'auto'",
    #file_format = "JSON 's3://udacity-dend/log_json_path.json'",
    #file_format="JSON",
    #json_path="auto",
    #json_path="s3://udacity-dend/log_json_path.json",  
    provide_context=True
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id="Load_songs_to_Redshift",
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_songs",
    s3_bucket= s3_bucket,
    s3_key= song_on_s3,
    #file_format='/log_json_path.json',
    file_format = "JSON 'auto'",
    #file_format="json",
    #json_path="auto",
    provide_context  = True
    
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="songplays",
    sql=SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_users_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="users",
    sql=SqlQueries.user_table_insert,
    loding_mode = "delete-load"
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_songs_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="songs",
    sql=SqlQueries.song_table_insert,
    loding_mode = "delete-load"
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artists_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="artists",
    sql=SqlQueries.artist_table_insert,
    loding_mode = "delete-load"
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="time",
    sql=SqlQueries.time_table_insert,
    loding_mode = "delete-load"
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    table="songplays"
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# subsequence of tasks:

start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift

## Loading:

stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table
## Creating dimentional tables
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table
## Checking data:
load_song_dimension_table >> run_quality_checks
load_user_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks
run_quality_checks >> end_operator