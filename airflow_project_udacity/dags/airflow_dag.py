from datetime import datetime, timedelta, date
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries as sql

# Today's date for start date.
today = datetime.today()

# Dag dictionary
default_args = {
    'owner': 'udacity',
    'start_date': today,
    'depends_on_past': '',
    'retries': '5',
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False,
}

# Variables for s3, redshift, and the SQL queries.
redshift_conn_id="redshift"
aws_credentials_id="aws_credentials"
schema = "udacity_airflow_project"
region='us-west-2'
songs_staging_table = f"{schema}.staging_songs"
events_staging_table = f"{schema}.staging_events"
aws_credentials_id="aws_credentials"
s3_bucket = "udacity-dend"
s3_key = {"log_data":"log_data", "song_data":"song_data"}
sql_tables = list(map(lambda str: schema + '.' + str, sql.tables))

# Action variables. These control what opertions are run in the dates.
# Turn on info level logging.
verbose_logging = True
# Run CREATE SQL
create = True
# Run DELETE SQL
delete = False
# Run INSERT SQL
append=True

# The Dag. Takes in the default_args dictionaryfor settings.
dag = DAG('udacity_airflow_project',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@hourly'
        )

# Operators and tasks
start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

# S3 to Redshift Operators
stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id=redshift_conn_id,
    aws_credentials_id=aws_credentials_id,
    table=events_staging_table,
    s3_bucket=s3_bucket,
    s3_key=s3_key["log_data"],
    region=region,
    verbose=verbose_logging,
    create=create,
    delete=delete,
    append=append,
    sql=sql.create_staging_events_table.format(schema)
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id=redshift_conn_id,
    aws_credentials_id=aws_credentials_id,
    table=songs_staging_table,
    s3_bucket=s3_bucket,
    s3_key=s3_key["song_data"],
    region=region,
    verbose=verbose_logging,
    create=create,
    delete=delete,
    append=append,
    sql=sql.create_staging_songs_table.format(schema)
)

# Load Redshift tables Operators
load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id=redshift_conn_id,
    table="",
    verbose_logging=verbose_logging,
    create=create,
    delete=delete,
    append=append,
    sql=[sql.create_songplays_table.format(schema),
     sql.songplay_table_insert.format(schema, schema, schema)]
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id=redshift_conn_id,
    table="",
    verbose_logging=verbose_logging,
    create=create,
    delete=delete,
    append=append,
    sql=[sql.create_users_table.format(schema),
        sql.user_table_insert.format(schema, schema)]
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id=redshift_conn_id,
    table="",
    verbose_logging=verbose_logging,
    create=create,
    delete=delete,
    append=append,
    sql=[sql.create_song_table.format(schema),
        sql.song_table_insert.format(schema, schema)]
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id=redshift_conn_id,
    table="",
    verbose_logging=verbose_logging,
    create=create,
    delete=delete,
    append=append,
    sql=[sql.create_artist_table.format(schema),
        sql.artist_table_insert.format(schema, schema)]
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id=redshift_conn_id,
    table="",
    verbose_logging=verbose_logging,
    create=create,
    delete=delete,
    append=append,
    sql=[sql.create_time_table.format(schema),
        sql.time_table_insert.format(schema, schema)]
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id=redshift_conn_id,
    table=sql_tables,
    verbose_logging=verbose_logging,
    sql=sql_tables,
)

# End operator. Notes the end of execution.
end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# DAG workflow
start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift
stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table
load_song_dimension_table >> run_quality_checks
load_user_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks
run_quality_checks >> end_operator
