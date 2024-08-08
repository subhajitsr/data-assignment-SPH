create database TESTDB;
use database TESTDB;
create schema CORE;
create schema SEMANTIC;

-- Create the S3 stage
CREATE OR REPLACE STAGE stg_yt_channel_stats
  URL = 's3://youtube-stats-001/dump/parquet/channel/'
  CREDENTIALS = (
    AWS_KEY_ID = 'AWS_ACCESS_KEY_ID'
    AWS_SECRET_KEY = '<AWS_SECRET_ACCESS_KEY>'
  );

CREATE OR REPLACE STAGE stg_yt_channel_md
  URL = 's3://youtube-stats-001/dump/parquet/channel_md/'
  CREDENTIALS = (
    AWS_KEY_ID = 'AWS_ACCESS_KEY_ID'
    AWS_SECRET_KEY = '<AWS_SECRET_ACCESS_KEY>'
  );

CREATE OR REPLACE STAGE stg_yt_video_md
  URL = 's3://youtube-stats-001/dump/parquet/video_md/'
  CREDENTIALS = (
    AWS_KEY_ID = 'AWS_ACCESS_KEY_ID'
    AWS_SECRET_KEY = '<AWS_SECRET_ACCESS_KEY>'
  );

CREATE OR REPLACE STAGE stg_yt_video_stats
  URL = 's3://youtube-stats-001/dump/parquet/video/'
  CREDENTIALS = (
    AWS_KEY_ID = 'AWS_ACCESS_KEY_ID'
    AWS_SECRET_KEY = '<AWS_SECRET_ACCESS_KEY>'
  );




-- Create the stage table
create or replace TABLE TESTDB.CORE.tbl_stg_yt_channel_md (
channel_name VARCHAR(300),
channel_id VARCHAR(200),
title VARCHAR(300),
custom_url VARCHAR(100),
published_at timestamp_ntz(0),
country VARCHAR(50),
etl_ts  timestamp_ntz(0)
);

create or replace TABLE TESTDB.CORE.tbl_stg_yt_channel_stats (
channel_id VARCHAR(200),
rptg_dt date,
view_count bigint,
subscriber_count bigint,
video_count integer,
etl_ts  timestamp_ntz(0)
);


create or replace TABLE TESTDB.CORE.tbl_stg_yt_video_md (
id VARCHAR(100),
channel_id VARCHAR(200),
title VARCHAR(500),
url VARCHAR(200),
published_at timestamp_ntz(0),
etl_ts  timestamp_ntz(0)
);

create or replace TABLE TESTDB.CORE.tbl_stg_yt_video_stats (
id VARCHAR(100),
channel_id VARCHAR(200),
rptg_dt date,
view_count bigint,
like_count bigint,
dislike_count bigint,
comment_count bigint,
etl_ts  timestamp_ntz(0)
);




-- Create the core table
create or replace TABLE TESTDB.CORE.tbl_yt_channel_md (
channel_name VARCHAR(300),
channel_id VARCHAR(200),
title VARCHAR(300),
custom_url VARCHAR(100),
published_at timestamp_ntz(0),
country VARCHAR(50),
etl_ts  timestamp_ntz(0)
);

create or replace TABLE TESTDB.CORE.tbl_yt_channel_stats (
channel_id VARCHAR(200),
rptg_dt date,
view_count bigint,
subscriber_count bigint,
video_count integer,
etl_ts  timestamp_ntz(0)
);


create or replace TABLE TESTDB.CORE.tbl_yt_video_md (
id VARCHAR(100),
channel_id VARCHAR(200),
title VARCHAR(500),
url VARCHAR(200),
published_at timestamp_ntz(0),
etl_ts  timestamp_ntz(0)
);


create or replace TABLE TESTDB.CORE.tbl_yt_video_stats (
id VARCHAR(100),
channel_id VARCHAR(200),
rptg_dt date,
view_count bigint,
like_count bigint,
dislike_count bigint,
comment_count bigint,
etl_ts timestamp_ntz(0)
);


