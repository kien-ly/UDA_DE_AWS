from datetime import timedelta
import pendulum
import configparser
from airflow.decorators import dag
from airflow.operators.dummy_operator import DummyOperator
from operators.stage_redshift import StageToRedshiftOperator
from operators.load_fact import LoadFactOperator
from operators.load_dimension import LoadDimensionOperator
from operators.data_quality import DataQualityOperator
from operators.create_table import CreateTableOperator
from helpers.songplays_queries import SongplaysQueries

default_args = {
    'owner': 'kienly',
    'depends_on_past': False,
    'start_date': pendulum.now(),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
}

# Get config
config = configparser.ConfigParser()
config.read('dags/dwh.cfg')

log_data = config.get("S3","log_data")
log_jsonpath = config.get("S3", "log_jsonpath")
song_data = config.get("S3", "song_data")


@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='0 * * * *'
)
def songplays():
    start_operator = DummyOperator(task_id='Begin_execution')

    create_table_redshift = CreateTableOperator(
        task_id='Create_tables',
        query=SongplaysQueries.create_tables,
        redshift_conn_id='redshift'
    )
    
    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        table='staging_events',
        s3_path=log_data,
        format_log=log_jsonpath,
        aws_credentials_id='aws_credentials',
        redshift_conn_id='redshift'
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        table='staging_songs',
        s3_path=song_data,
        format_log='auto',
        aws_credentials_id='aws_credentials',
        redshift_conn_id='redshift'
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        query=SongplaysQueries.song_table_insert,
        redshift_conn_id='redshift',
        table="songs",
        truncate=False
    )

    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        query=SongplaysQueries.songplay_table_insert,
        redshift_conn_id='redshift'
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        query=SongplaysQueries.user_table_insert,
        redshift_conn_id='redshift',
        table="users",
        truncate=False
    )
    
    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        query=SongplaysQueries.time_table_insert,
        redshift_conn_id='redshift',
        table="time",
        truncate=False
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        query=SongplaysQueries.artist_table_insert,
        redshift_conn_id='redshift',
        table="artists",
        truncate=False
    )

    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        tables=['staging_events', 'staging_songs', 'songplays', 'users', 'songs', 'artists', 'time'],
        redshift_conn_id='redshift'
    )
    
    finish_operator = DummyOperator(task_id='End_execution')
    
    start_operator  >> create_table_redshift >> [
        stage_events_to_redshift,
        stage_songs_to_redshift
    ] >> load_songplays_table >> [
        load_song_dimension_table,
        load_artist_dimension_table,
        load_time_dimension_table,
        load_user_dimension_table
    ] >> run_quality_checks >> finish_operator


songplays_dag = songplays()