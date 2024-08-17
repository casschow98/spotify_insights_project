#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import os
import requests
import json
import requests
from datetime import datetime, timezone
import pytz
from auth_token import get_token
from google.cloud import bigquery
from airflow.exceptions import AirflowSkipException
import re



class get_recent_tracks:
    def __init__(self):
        self.REDIRECT_URI= "http://localhost:8888"
        self.SP_GET_USER_RP_URL = 'https://api.spotify.com/v1/me/player/recently-played'
        self.SP_GET_AUDIO_FT_URL = 'https://api.spotify.com/v1/audio-features'
        self.DATASET = os.environ.get("BQ_DATASET")
        self.PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
        self.TABLE = os.environ.get("BQ_TABLE")
        self.AUTH_TOKEN = ""
        self.CC_TOKEN = ""


    def api_request_recently_played(self):
        timestamp = self.get_timestamp()

        headers = {
            'Authorization': f'Bearer {self.AUTH_TOKEN}',
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        }
        params = {
            'limit': 50,
            'after': f"{timestamp}"
        }
        response = requests.get(
            self.SP_GET_USER_RP_URL,
            headers=headers,
            params=params
        )
        recent_tracks_json = response.json()

        if (not recent_tracks_json.get('items') or 
        not isinstance(recent_tracks_json, dict) or 
        len(recent_tracks_json.get('items', [])) <= 1):
            raise AirflowSkipException("No new tracks played recently on Spotify. Stopping DAG execution.")

        return recent_tracks_json


    def api_request_audio_features(self, track_ids):
        headers = {
            'Authorization': f'Bearer {self.CC_TOKEN}'
        }
        params = {
            'ids': ','.join(track_ids)
        }
        response = requests.get(self.SP_GET_AUDIO_FT_URL, headers=headers, params=params)
        return response.json()


    def get_timestamp(self):
        # Initialize a BigQuery client
        client = bigquery.Client(project=self.PROJECT_ID)
        
        # Query to get the latest timestamp from the table
        query = f"""
        SELECT
            MAX(played_at) AS latest_datetime
        FROM
            `{self.PROJECT_ID}.{self.DATASET}.{self.TABLE}`
        """
        
        query_job = client.query(query)
        result = query_job.result()

        row = next(result, None)
        if row is None or row.latest_datetime is None:
            print("No rows returned from the query. Using default August 1, 2024 timestamp for API request.")
            unix_timestamp = 1722556800010 # unix_timestamp for August 1, 2024 00:00:00 PT
        else:
            latest_datetime = row.latest_datetime
            print(f"Latest datetime found is {latest_datetime}. Using this as parameter to request songs from API played after this time.")
            unix_timestamp = int(latest_datetime.timestamp() * 1000)

        return unix_timestamp


    def ms_reformat(self, milliseconds):
        total_seconds = milliseconds / 1000
        minutes = int(total_seconds // 60)
        seconds = int(total_seconds % 60)

        return f"{minutes}:{seconds:02}"
    

    def utc_to_pt(self, utc_ts):
        pacific_tz = pytz.timezone('America/Los_Angeles')
        # utc_dt = datetime.strptime(utc_ts, "%Y-%m-%dT%H:%M:%S.%fZ")
        # utc_dt = utc_dt.replace(tzinfo=pytz.utc)
        pt_dt = utc_ts.astimezone(pacific_tz)
        pt_fmt = pt_dt.strftime("%Y-%m-%d %H:%M:%S")

        return pt_fmt


    def songs_to_csv(self):
        recent_tracks_json = self.api_request_recently_played()

        dt = datetime.now(timezone.utc) 
        utc_time = dt.replace(tzinfo=timezone.utc) 
        now_timestamp = utc_time.timestamp()

        rt_rows = []
        for item in recent_tracks_json['items']:
            track_id = item['track']['id']
            track_name = item['track']['name']
            artists_names = ", ".join([artist['name'] for artist in item['track']['artists']])
            played_at = item['played_at']
            duration_ms = item['track']['duration_ms']
            track_duration = self.ms_reformat(duration_ms)
            spotify_url = item['track']['external_urls']['spotify']

            ts_clean = re.sub(r'\W+', '', played_at)
            
            # Create unique identifier
            unique_id = f"{track_id}{ts_clean}"

            rt_rows.append({
                'track_id': track_id,
                'track_name': track_name,
                'artists': artists_names,
                'played_at': played_at,
                'duration_ms': duration_ms,
                'track_duration': track_duration,
                'spotify_url': spotify_url,
                'upload_timestamp': now_timestamp,
                'unique_id': unique_id
            })

        rt_df = pd.DataFrame(rt_rows)
        rt_df = rt_df.sort_values(by='played_at', ascending=True)
        rt_df = rt_df.iloc[1:,:]
        size = len(rt_df)
        print(f"**Dataframe has total of {size} records**")


        track_ids_list = rt_df['track_id'].drop_duplicates().to_list()
        audio_features_json = self.api_request_audio_features(track_ids_list)

        af_rows = []
        for item in audio_features_json['audio_features']:
            track_id = item['id']
            danceability = item['danceability']
            energy = item['energy']
            key = item['key']
            loudness = item['loudness']
            mode = item['mode']
            speechiness = item['speechiness']
            acousticness = item['acousticness']
            instrumentalness = item['instrumentalness']
            liveness = item['liveness']
            valence = item['valence']
            tempo = item['tempo']
            time_signature = item['time_signature']

            af_rows.append({
                'track_id': track_id,
                'danceability': danceability,
                'energy': energy,
                'key': key,
                'loudness': loudness,
                'mode': mode,
                'speechiness': speechiness,
                'acousticness': acousticness,
                'instrumentalness': instrumentalness,
                'liveness': liveness,
                'valence': valence,
                'tempo': tempo,
                'time_signature': time_signature
            })

        af_df = pd.DataFrame(af_rows)

        df = pd.merge(rt_df, af_df, on='track_id', how='left')
        df = df.sort_values(by='played_at', ascending=False)

        df['track_id'] = df['track_id'].astype(str)
        df['track_name'] = df['track_name'].astype(str)
        df['artists'] = df['artists'].astype(str)
        df['played_at'] = pd.to_datetime(df['played_at'])
        df['duration_ms'] = df['duration_ms'].astype(int)
        df['track_duration'] = df['track_duration'].astype(str)
        df['spotify_url'] = df['spotify_url'].astype(str)
        df['upload_timestamp'] = pd.to_datetime(df['upload_timestamp'])
        df['unique_id'] = df['unique_id'].astype(str)
        
        # Audio features
        df['danceability'] = df['danceability'].astype(float)
        df['energy'] = df['energy'].astype(float)
        df['key'] = df['key'].astype(int)
        df['loudness'] = df['loudness'].astype(float)
        df['mode'] = df['mode'].astype(int)
        df['speechiness'] = df['speechiness'].astype(float)
        df['acousticness'] = df['acousticness'].astype(float)
        df['instrumentalness'] = df['instrumentalness'].astype(float)
        df['liveness'] = df['liveness'].astype(float)
        df['valence'] = df['valence'].astype(float)
        df['tempo'] = df['tempo'].astype(float)
        df['time_signature'] = df['time_signature'].astype(int)

        # Find the latest 'played_at' value in the DataFrame
        latest_played_at = df['played_at'].max()

        # Convert the latest 'played_at' timestamp to a format suitable for filenames
        # Remove timezone info and replace colons with dashes for filename safety
        latest_played_at = self.utc_to_pt(latest_played_at)
        latest_dt = datetime.strptime(latest_played_at, "%Y-%m-%d %H:%M:%S")
        formatted_dt = latest_dt.strftime('%Y-%m-%d_%H-%M')

        os.makedirs(f"/opt/airflow/tmp/", exist_ok=True)
        df.to_csv(f"/opt/airflow/tmp/spotify_tracks_{formatted_dt}.csv", index=False)


    def retrieve_songs(self):
        instance = get_token()
        self.AUTH_TOKEN = instance.refresh()
        self.CC_TOKEN = instance.get_cc_access_token()
        self.songs_to_csv()

    


