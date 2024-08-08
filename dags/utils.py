# This util file has some defined Class which help pull YouTube API data and ETL Loader
# Author: Subhajit Maji
# subhajitsr@gmail.com
# +65-98302027
# Date: 2024-08-08

from google.oauth2 import service_account
from googleapiclient.discovery import build
from typing import Any, Optional, Dict, List
import json
import datetime
import logging
import snowflake.connector


class CredentialError(Exception):
    """This exception is raised when there is error in creating credential."""

class YoutubeDataError(Exception):
    """This exception is raised when there is error in creating Youtube data object."""

class InsufficientInputError(Exception):
    """This exception is raised when insufficint info is provided to create channel object."""

class ChannelNotFoundError(Exception):
    """This exception is raised when a channel is not found."""

class YoutubeChannel():
    def __init__(self,
                 service_account_info: json,
                 scopes: Optional[list] = ['https://www.googleapis.com/auth/youtube.readonly'],
                 channel_name: Optional[str] = None,
                channel_id: Optional[str] = None) -> None:
        # Trying to create the credential object
        try:
            credentials = service_account.Credentials.from_service_account_info(
            service_account_info,
            scopes=scopes
            )
        except Exception as e:
            raise CredentialError(e)
        
        # Trying to build the youtube object
        try:
            youtube = build('youtube', 'v3', credentials=credentials)
        except Exception as e:
            raise YoutubeDataError(e)
            
        # Fetching Channel id by name
        if channel_name is None and channel_id is None:
            raise InsufficientInputError("Either channel_name or channel_id needs to be provided")
        
        if channel_id is None:
            # Fetch channel ID by name
            response = youtube.search().list(q=channel_name, type='channel', part='id', maxResults=1).execute()
            if not response.get('items'):
                raise ChannelNotFoundError(f"No channel found for username: {channel_name}")
            channel_id = response['items'][0]['id']['channelId']
        else:
            # Verify the channel id passed is correct
            if not youtube.channels().list(id=channel_id,part='id').execute().get('items'):
                raise ChannelNotFoundError(f"No channel found for channel_id: {channel_id}")
        
        # Get channel attributes
        response = youtube.channels().list(
            id=channel_id,
            part='snippet,statistics'
        ).execute()
        
        # Set Channel attribute for the object
        self._credentials = credentials
        self._youtube = youtube
        self.channel_name = channel_name
        self.channel_id = channel_id
        self.title = response.get('items')[0].get('snippet').get('title')
        self.description = response.get('items')[0].get('snippet').get('description')
        self.customUrl = response.get('items')[0].get('snippet').get('customUrl')
        self.publishedAt = response.get('items')[0].get('snippet').get('publishedAt')
        self.country = response.get('items')[0].get('snippet').get('country')
        self.viewCount = response.get('items')[0].get('statistics').get('viewCount')
        self.subscriberCount = response.get('items')[0].get('statistics').get('subscriberCount')
        self.videoCount = response.get('items')[0].get('statistics').get('videoCount')
    
    @staticmethod
    def get_video_statistics(youtube: Any, video_ids: list) -> list:
        video_stats = []

        # Fetch statistics for the videos
        video_response = youtube.videos().list(
            part='snippet,statistics',
            id=','.join(video_ids)
        ).execute()

        for item in video_response['items']:
            video_stats.append({
                'id': item['id'],
                'title': item['snippet']['title'],
                'url': f"https://www.youtube.com/watch?v={item['id']}",
                'views': item['statistics'].get('viewCount',0),
                'likes': item['statistics'].get('likeCount',0),
                'dislikes': item['statistics'].get('dislikeCount',0),
                'comments': item['statistics'].get('commentCount', 0),
                'publishedAt': item['snippet']['publishedAt']
            })
    
        return video_stats

    
    def get_video_data(self, chunk_size: Optional[int] = 50, days_count: Optional[int] = 365):
        video_data = []
        
        # Calculating published_after based on days_count
        t_ago = datetime.datetime.now() - datetime.timedelta(days=days_count)
        published_after = t_ago.isoformat("T") + "Z"

        request = self._youtube.search().list(
                    part='id',
                    channelId=self.channel_id,
                    publishedAfter=published_after,
                    maxResults=chunk_size,
                    type='video'
                )

        while request:
            response = request.execute()
            video_ids = [item['id']['videoId'] for item in response['items']]
            
            # Getting video statistics
            video_data = video_data + self.get_video_statistics(self._youtube, video_ids)
            
            # Creating request for the next chunk fetch
            request = self._youtube.search().list_next(request, response)        
                
        return video_data
        



class SnowflakeLoader():
    def __init__(self,
                 conn: Any,
                 schema: str,
                 s3_stage_name: str,
                 stage_table_name: str,
                 core_table_name: str,
                 s3_col_map: Dict,
                 load_type: Optional[str] = 'FULL',
                 merge_on_col: Optional[List[str]] = []
                ):
        try:
            curs = conn.cursor()
        except Exception as e:
            raise Exception(f"Error while opening the cursor. {e}")
        
        # Check when load type is Merge, the merge_on_col need to be supplied
        if load_type.upper() == 'MERGE' and len(merge_on_col) == 0:
            raise Exception(f"merge_on_col arg is mandatory for Loader type: {load_type}")
        
        # Fecthing stage table columns
        curs.execute(f"""
        SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{stage_table_name.upper()}' AND TABLE_SCHEMA = '{schema.upper()}';
        """)
        self.stg_cols = [row[0] for row in curs.fetchall()]
        
        # Fecthing main table columns
        curs.execute(f"""
        SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{core_table_name.upper()}' AND TABLE_SCHEMA = '{schema.upper()}';
        """)
        self.main_cols = [row[0] for row in curs.fetchall()]
        
        # Checking if stage and main table columns are in sync
        if not set(self.main_cols) == set(self.stg_cols):
            raise Exception('Stage and Main table column mismatch. ')
        
        # Converting merge columns to upper
        if  len(merge_on_col) != 0:
            self.merge_on_col = [col.upper() for col in merge_on_col]
        else:
            self.merge_on_col = merge_on_col
            
        # Checking if merge col existing in stage and main table
        if load_type.upper() == 'MERGE':
            if len([col for col in self.merge_on_col if col in self.main_cols and col in self.stg_cols]) != len(self.merge_on_col):
                raise Exception('All columns in merge_on_col must be present in both stage and core table.')
        
            
        self.schema = schema.upper()
        self.stage_table_name = stage_table_name.upper()
        self.core_table_name = core_table_name.upper()
        self.load_type = load_type.upper()
        self.conn = conn
        self.s3_col_map = s3_col_map
        self.s3_stage_name = s3_stage_name
        
        curs.close()
    
    def curs_handler(func):
        def wrapper(self, *args, **kwargs):
            try:
                curs = self.conn.cursor()
            except Exception as e:
                raise Exception(f"from db_conn_check: Error while opening the cursor. {e}")
            func(self, curs, *args, **kwargs)
            curs.close()
        return wrapper

    
    @curs_handler
    def stg_to_core(self, curs, *args, **kwargs) -> None:
            
        if self.load_type == 'MERGE':
            sql_text = [f"""
                MERGE INTO {self.schema}.{self.core_table_name} as t
                USING {self.schema}.{self.stage_table_name} as d
                ON 
                {'AND '.join(f"d.{col} = t.{col} " for col in self.merge_on_col)}
                WHEN MATCHED THEN
                UPDATE SET
                {', '.join(f"{col} = d.{col} " for col in [col for col in self.main_cols if col not in self.merge_on_col])}
                WHEN NOT MATCHED THEN
                INSERT
                ({','.join([col for col in self.main_cols])})
                VALUES
                ({','.join([f"d.{col}" for col in self.main_cols])})
                ;
                """]
        else:
            sql_text = [f"delete from {self.schema}.{self.core_table_name};",
            f"""insert into {self.schema}.{self.core_table_name}
            ({','.join([col for col in self.main_cols])})
            select
            {','.join([col for col in self.main_cols])}
            from
            {self.schema}.{self.stage_table_name};
            """]
        
        for sql in sql_text:
            curs.execute(sql)
            logging.info(sql)

    @curs_handler  
    def s3_to_stg(self, curs, *args, **kwargs) -> None:
        
        # Validate S3_col_map dictionary if available
        for item in self.s3_col_map.items():
            if item[1].upper() not in self.stg_cols:
                raise Exception(f"s3_to_stg: Column {item[1]} not present in the table {self.schema}.{self.stage_table_name}")
                    
        
        # Cleanup stage table first
        curs.execute(f"""delete from {self.schema}.{self.stage_table_name};""")
        
        # Prepare the COPY INTO statement for S3 load
        sql_text = f"""
        COPY INTO {self.schema}.{self.stage_table_name} ({','.join([col for col in [item[1] for item in self.s3_col_map.items()]])})
        FROM (
            SELECT
            {','.join([f"$1:{item[0]}::VARIANT AS {item[1]}" for item in self.s3_col_map.items()])}
            FROM @{self.s3_stage_name}
        )
        FILE_FORMAT = (TYPE = 'PARQUET');
        """
        
        logging.info(sql_text)
        curs.execute(sql_text)
