## Youtube-data-analysis

This Solution uses gcloud sdk and Youtube Data API to fetch some public data related to few listed channels from Youtube, store it to Datalake and eventually loading it to Snowflake data watehouse to perform data analysis and produce insights.



## Data Pipeline deployment notes:
1. The solution is built to be run in an Airflow scheduler. the `youtube-data-analytics-loader.py` file in the 'dags' folder needs to be placed in the `airflow/dags` directory of the instance.
2. Install the gcloud sdk which is a prerequisite. Please follow the instruction here - https://cloud.google.com/sdk/docs/install
3. All the necessary Pypi packages are mentioned in the `requirements.txt`. Same needs to be installed in the system where the Airflow instance is running from.
4. Need to set the Snowflake connectivity details in the Airflow variable as below,
   - `SF_USERNAME`: Snowflake DB username
   - `SF_PASSWORD`: Password for the username
   - `SF_ACCOUNT`: Snowflake account name
5. Set the AWS Secret in the Environmental variable as below.
   - For Linax/MacOS:
      - `export AWS_ACCESS_KEY_ID=your_access_key_id`
      - `export AWS_SECRET_ACCESS_KEY=your_secret_access_key`
   - For Windows:
      - `set AWS_ACCESS_KEY_ID=your_access_key_id`
      - `set AWS_SECRET_ACCESS_KEY=your_secret_access_key`
7. Execute the DDLs (`database/object_definition.ddl`) in the Snowflake database where the data is intended to be landed.
8. Create the S3 buckets and subsequent folders as below,
   - Bucket Name: `youtube-stats-001`
   - Path:
        - `dump/parquet/channel`
        - `dump/parquet/channel_md`
        - `dump/parquet/video_md`
        - `dump/parquet/video`
