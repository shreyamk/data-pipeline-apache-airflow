from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

# AWS_KEY= os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

# Default arguments
default_args = {
    'owner': 'shreyak',
    'start_date': datetime(2020, 12, 1),
    'end_date': datetime(2020, 12, 1),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False,
    
}

# Defining DAG
dag = DAG('udac_sparkify_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
        )

# Starting operator

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

# Operators to create Staging tables on Redshift

stage_events_to_redshift = StageToRedshiftOperator(
    redshift_id = "redshift",
    aws_id = "aws_credentials",
    schema = "staging_events",
    s3_path="udacity-dend",
    s3_key = "log_data/",
    query_end = "format as json 's3://udacity-dend/log_json_path.json'",
    task_id='Stage_events',
    dag=dag,
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    redshift_id = "redshift",
    aws_id = "aws_credentials",
    schema = "staging_songs",
    s3_path="udacity-dend",
    s3_key = "song_data",
    query_end = "json 'auto' compupdate off region 'us-west-2'",
    dag=dag
)

# Operator to load fact table

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    redshift_id = "redshift",
    schema = "songplays",
    query = "songplay_table_insert",
    dag=dag,
    append_only = False
)

# Operators to load dimension tables

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table', 
    redshift_id = "redshift",
    schema = "users",
    query = "user_table_insert",
    dag=dag,
    append_only = False
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    redshift_id = "redshift",
    schema = "song",
    query = "song_table_insert",
    dag=dag,
    append_only = False
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    redshift_id = "redshift",
    schema = "artist",
    query = "artist_table_insert",
    dag=dag,
    append_only = False
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    redshift_id = "redshift",
    schema = "time",
    query = "time_table_insert",
    dag=dag,
    append_only = False
)

# Operator for quality checks 

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    redshift_id = "redshift",
    tables = ["songplay", "users", "song", "artist", "time"],
    dag=dag
)

# Ending operator

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# Defining dependencies

start_operator >> stage_events_to_redshift >> load_songplays_table
start_operator >> stage_songs_to_redshift >> load_songplays_table

load_songplays_table >> load_song_dimension_table >> run_quality_checks
load_songplays_table >> load_user_dimension_table >> run_quality_checks
load_songplays_table >> load_artist_dimension_table >> run_quality_checks
load_songplays_table >> load_time_dimension_table >> run_quality_checks

run_quality_checks >> end_operator