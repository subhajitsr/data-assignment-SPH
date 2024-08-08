## Data Description

SPH Media deals with primarily audience data and these data may come from various social media sources such as facebook, instagram, youtube, etc.
SPH Media has many news publications under it’s portfolio. Management would like to understand how well the publications are engaging with it’s audience and the general performance of the videos that are published there. The publications Youtube Channels that need to be reported on are:
Strait Times (   The Straits Times ) Business Times (   The Business Times ) ZaoBao (   zaobaosg )
Tamil Marusu (   Tamil Murasu )
Berita Harian (   Berita Harian Singapura )

## Data Pipeline deployment notes:
1. The solution is built to be run in an Airflow scheduler. the `loan-application-core-loader.py` file in the 'dags' folder needs to be placed in the `airflow/dags` directory of the instance.
2. All the necessary Pypi packages are mentioned in the `requirements.txt`. Same needs to be installed in the AIrflow instance before the dag deployment.
3. Need to set the AWS/Snowflake connectivity details in the Airflow variable as below,
   - `SF_USERNAME`: Snowflake DB username
   - `SF_PASSWORD`: Password for the username
   - `SF_ACCOUNT`: Snowflake account name
   - `AWS_ACCESS_KEY`: Access key for AWS connectivity for S3 access
   - `AWS_SECRET_KEY`: Secret key for the access key
   - `S3_BUCKET_NAME`: S3 bucket name where the data will be staged
4. Execute the DDLs (`database/object_definition.ddl`) in the Snowflake database where the data is intended to be landed.
5. Create the `/inbox/loan-data` (Folder to contain the csv files shared frequently) and `/archive/loan-data`(The files already processed to datalake will be pushed to this Archive) directory in the Airflow local folder.
6. Create the bucket named `loan-test00XX` in AWS S3 datalake. Configure the same name in the Airflow variable. 
7. Under the above bucket create the path, `loan-data/dump`. Since there is a requirement of keeping the processed data in the datalake for the other data analysts to use, there is no archival mechanism implemented here.
