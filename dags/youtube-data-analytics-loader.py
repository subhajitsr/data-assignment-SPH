# This is a the dag file for loading youtube analytics data to Snowflake
# Author: Subhajit Maji
# subhajitsr@gmail.com
# +65-98302027
# Date: 2024-08-08

import logging
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow.hooks.base import BaseHook
import os
import json
import logging
import pandas as pd
import datetime
from .utils import YoutubeChannel, SnowflakeLoader
import pyarrow as pa
import pyarrow.parquet as pq
import boto3
import io
import time
import snowflake.connector


# Fetch Snowflake connection details
connection = BaseHook.get_connection('yt-analytics-sf')

# Configurations
service_account_info = json.load(open('Secrets/youtube-app-secret.json'))
channel_list = ['straitstimesonline', 'BeritaHarianSG1957', 'Tamil_Murasu', 'TheBusinessTimes', 'zaobaodotsg']
s3_bucket_name = 'youtube-stats-001'
s3_path = "dump/parquet"
sf_username = connection.login
sf_password = connection.password
sf_account = connection.host
sf_warehouse = 'COMPUTE_WH'
sf_database = 'TESTDB'
sf_schema = 'CORE'

# Initialize SF and S3 connections
dbcon = snowflake.connector.connect(
    user=sf_username,
    password=sf_password,
    account=sf_account,
    warehouse=sf_warehouse,
    database=sf_database,
    schema=sf_schema
)

s3 = boto3.resource('s3')


dag = DAG('youtube-data-analytics-loader-v1',
          description='Extracts Youtube stats data and lods to Snowflake',
          schedule_interval='0 * * * *',
          start_date=datetime(2024, 8, 8),
          catchup=False,
          maxmax_active_runs=1)


def fn_extract_load_s3(**context):
    # Define the dataframes
    df_channel = pd.DataFrame(columns=['channel_name','channel_id','title','customUrl','publishedAt','country','viewCount','subscriberCount','videoCount','rptg_dt','etl_ts'])
    df_video = pd.DataFrame(columns=['id','title','url','views','likes','dislikes','comments','publishedAt','channel_name','channel_id','rptg_dt','etl_ts'])

    _now_ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    _today_dt = datetime.datetime.now().strftime("%Y-%m-%d")
    logging.info(f"_now_ts: {_now_ts}")
    logging.info(f"_today_dt: {_today_dt}")

    # Fetch all the channles configured for collecting data
    for channel_name in channel_list:
        logging.info(f"Fetching data for Channel: {channel_name}")
        channelObj = YoutubeChannel(service_account_info=service_account_info, channel_name=channel_name)    
        df_channel = pd.concat([df_channel, pd.DataFrame([{
            'channel_name': channelObj.channel_name,
            'channel_id': channelObj.channel_id,
            'title': channelObj.title,
            'customUrl': channelObj.customUrl,
            'publishedAt': channelObj.publishedAt,
            'country': channelObj.country,
            'viewCount': channelObj.viewCount,
            'subscriberCount': channelObj.subscriberCount,
            'videoCount': channelObj.videoCount,
            'rptg_dt': _today_dt,
            'etl_ts': _now_ts
        }])], ignore_index=True)
        df_video_temp = pd.DataFrame(channelObj.get_video_data())
        df_video_temp['channel_name'] = channelObj.channel_name
        df_video_temp['channel_id'] = channelObj.channel_id
        df_video_temp['rptg_dt'] = _today_dt
        df_video_temp['etl_ts'] = _now_ts
        df_video = pd.concat([df_video, df_video_temp], ignore_index=True)
    logging.info("Data fetching from Youtube finished.")

    logging.info("Splitting data and preparing for S3 load.")
    df_video_md = df_video[['id','channel_id','title','url','publishedAt','etl_ts']]
    df_video = df_video[['id','channel_id','rptg_dt','views','likes','dislikes','comments','etl_ts']]
    df_channel_md = df_channel[['channel_name','channel_id','title','customUrl','publishedAt','country','etl_ts']]
    df_channel = df_channel[['channel_id','rptg_dt','viewCount','subscriberCount','videoCount','etl_ts']]

    # Dropping duplicates based on respective Key columns
    df_channel = df_channel.drop_duplicates(subset=['channel_id','rptg_dt'])
    df_channel_md = df_channel_md.drop_duplicates(subset=['channel_id'])
    df_video_md = df_video_md.drop_duplicates(subset=['id'])
    df_video = df_video.drop_duplicates(subset=['id','rptg_dt'])

    logging.info("Data prepared. Starting S3 load.")

    # Upload the Channel MD Parquet file to S3
    try:
        parquet_buffer = io.BytesIO()
        df_channel_md.to_parquet(parquet_buffer, index=False)
        chnl_md_file_name = f"channel_md_data_{str(int(round(time.time())))}.parquet"
        s3.Object(s3_bucket_name, f"{s3_path}/channel_md/{chnl_md_file_name}").put(Body=parquet_buffer.getvalue())
        logging.info(f"File {chnl_md_file_name} has been uploaded to s3://{s3_bucket_name}/{s3_path}/channel_md")
    except Exception as e:
        raise Exception(f"Channel MD S3 Upload failed. {e}")
    
    logging.info("Channel MD file uploaded to S3.")

        
    # Upload the Channel Parquet file to S3
    try:
        parquet_buffer = io.BytesIO()
        df_channel.to_parquet(parquet_buffer, index=False)
        chnl_file_name = f"channel_data_{str(int(round(time.time())))}.parquet"
        s3.Object(s3_bucket_name, f"{s3_path}/channel/{chnl_file_name}").put(Body=parquet_buffer.getvalue())
        logging.info(f"File {chnl_file_name} has been uploaded to s3://{s3_bucket_name}/{s3_path}/channel")
    except Exception as e:
        raise Exception(f"Channel Stats S3 Upload failed. {e}")
    
    logging.info("Channel stats file uploaded to S3.")

    # Upload the Video MD Parquet file to S3
    try:
        parquet_buffer = io.BytesIO()
        df_video_md.to_parquet(parquet_buffer, index=False)
        video_md_file_name = f"video_md_data_{str(int(round(time.time())))}.parquet"
        s3.Object(s3_bucket_name, f"{s3_path}/video_md/{video_md_file_name}").put(Body=parquet_buffer.getvalue())
        logging.info(f"File {video_md_file_name} has been uploaded to s3://{s3_bucket_name}/{s3_path}/video_md")
    except Exception as e:
        raise Exception(f"Video MD S3 Upload failed. {e}")
    
    logging.info("Video MD file uploaded to S3.")

    # Upload the Video Parquet file to S3
    try:
        parquet_buffer = io.BytesIO()
        df_video.to_parquet(parquet_buffer, index=False)
        video_file_name = f"video_data_{str(int(round(time.time())))}.parquet"
        s3.Object(s3_bucket_name, f"{s3_path}/video/{video_file_name}").put(Body=parquet_buffer.getvalue())
        logging.info(f"File {video_file_name} has been uploaded to s3://{s3_bucket_name}/{s3_path}/video")
    except Exception as e:
        raise Exception(f"Video Stats S3 Upload failed. {e}")
    
    logging.info("Video stats file uploaded to S3.")


def fn_load_channel_md_to_sf(**context):
    logging.info("Start: Loading Channel MD.")
    # Load Channel MD
    sf_ldr = SnowflakeLoader(
        conn = dbcon,
        schema = sf_schema,
        s3_stage_name = 'stg_yt_channel_md',
        stage_table_name = 'tbl_stg_yt_channel_md',
        core_table_name = 'tbl_yt_channel_md',
        s3_col_map = {
                        'channel_name': 'channel_name',
                        'channel_id': 'channel_id',
                        'title': 'title',
                        'customUrl': 'custom_url',
                        'publishedAt': 'published_at',
                        'country': 'country',
                        'etl_ts': 'etl_ts'
                    })

    # Run the S3 loader and core loader
    sf_ldr.s3_to_stg()
    sf_ldr.stg_to_core()
    logging.info("Complete: Loading Channel MD.")


def fn_load_channel_stats_to_sf(**context):
    logging.info("Start: Loading Channel Stats.")
    # Load Channel Stats
    sf_ldr = SnowflakeLoader(
        conn = dbcon,
        schema = sf_schema,
        s3_stage_name = 'stg_yt_channel_stats',
        stage_table_name = 'tbl_stg_yt_channel_stats',
        core_table_name = 'tbl_yt_channel_stats',
        s3_col_map = {
                        'channel_id': 'channel_id',
                        'rptg_dt': 'rptg_dt',
                        'viewCount': 'view_count',
                        'subscriberCount': 'subscriber_count',
                        'videoCount': 'video_count',
                        'etl_ts': 'etl_ts'
                    },
        load_type = 'MERGE',
        merge_on_col = ['channel_id','rptg_dt']
            )

    # Run the S3 loader and core loader
    sf_ldr.s3_to_stg()
    sf_ldr.stg_to_core()
    logging.info("Complete: Loading Channel Stats.")


def fn_load_video_md_to_sf(**context):
    logging.info("Start: Loading Video MD.")
    # Load Video MD
    sf_ldr = SnowflakeLoader(
        conn = dbcon,
        schema = sf_schema,
        s3_stage_name = 'stg_yt_video_md',
        stage_table_name = 'tbl_stg_yt_video_md',
        core_table_name = 'tbl_yt_video_md',
        s3_col_map = {
                        'id': 'id',
                        'channel_id': 'channel_id',
                        'title': 'title',
                        'url': 'url',
                        'publishedAt': 'published_at',
                        'etl_ts': 'etl_ts'
                    },
        load_type = 'MERGE',
        merge_on_col = ['id']
            )

    # Run the S3 loader and core loader
    sf_ldr.s3_to_stg()
    sf_ldr.stg_to_core()
    logging.info("Complete: Loading Video MD.")


def fn_load_video_stats_to_sf(**context):
    logging.info("Start: Loading Video Stats.")
    # Load Video stats
    sf_ldr = SnowflakeLoader(
        conn = dbcon,
        schema = sf_schema,
        s3_stage_name = 'stg_yt_video_stats',
        stage_table_name = 'tbl_stg_yt_video_stats',
        core_table_name = 'tbl_yt_video_stats',
        s3_col_map = {
                        'id': 'id',
                        'channel_id': 'channel_id',
                        'rptg_dt': 'rptg_dt',
                        'views': 'view_count',
                        'likes': 'like_count',
                        'dislikes': 'dislike_count',
                        'comments': 'comment_count',
                        'etl_ts': 'etl_ts'
                    },
        load_type = 'MERGE',
        merge_on_col = ['id','rptg_dt']
    )

    # Run the S3 loader and core loader
    sf_ldr.s3_to_stg()
    sf_ldr.stg_to_core()
    logging.info("Complete: Loading Video Stats.")


# Define tasks
task_load_from_yt_to_s3 = PythonOperator(
    task_id='task_load_from_yt_to_s3',
    python_callable=fn_extract_load_s3,
    provide_context=True,
    dag=dag,
)

task_load_channel_md_to_sf = PythonOperator(
    task_id='task_load_channel_md_to_sf',
    python_callable=fn_load_channel_md_to_sf,
    provide_context=True,
    dag=dag,
)

task_load_channel_stats_to_sf = PythonOperator(
    task_id='task_load_channel_stats_to_sf',
    python_callable=fn_load_channel_stats_to_sf,
    provide_context=True,
    dag=dag,
)

task_load_video_md_to_sf = PythonOperator(
    task_id='task_load_video_md_to_sf',
    python_callable=fn_load_video_md_to_sf,
    provide_context=True,
    dag=dag,
)

task_load_video_stats_to_sf = PythonOperator(
    task_id='task_load_video_stats_to_sf',
    python_callable=fn_load_video_stats_to_sf,
    provide_context=True,
    dag=dag,
)



# Define task dependency
task_load_from_yt_to_s3 >> [task_load_channel_md_to_sf,
                            task_load_channel_stats_to_sf,
                            task_load_video_md_to_sf,
                            task_load_video_stats_to_sf]