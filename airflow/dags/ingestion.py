#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import os
import requests
import json
import requests
from datetime import datetime, timezone
import pytz
from token import get_token
from google.cloud import bigquery
from exceptions import EmptyResponseError



class get_recent_tracks:
    def __init__(self):
        self.REDIRECT_URI= "http://localhost:8888"
        self.SP_GET_USER_RP_URL = 'https://api.spotify.com/v1/me/player/recently-played'
        self.SP_GET_AUDIO_FT_URL = 'https://api.spotify.com/v1/audio-features'
        self.DATASET = os.environ.get("BQ_DATASET")
        self.PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
        self.TABLE = os.environ.get("BQ_TABLE")


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

        if not recent_tracks_json.get('items'):
            raise EmptyResponseError("No new tracks played recently on Spotify.")

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
        
        # Define the query to get the latest timestamp from the table
        query = f"""
        SELECT
            MAX(played_at_PT) AS latest_pacific_datetime
        FROM
            `{self.PROJECT_ID}.{self.DATASET}.{self.TABLE}`
        """
        
        query_job = client.query(query)
        result = query_job.result()
        row = result.next()

        if row.played_at_PT is None:
            unix_timestamp = 1727811200000 # unix_timestamp for August 1, 2024 00:00:00 PT
        else:
            latest_pacific_datetime = row.played_at_PT
            pacific_tz = pytz.timezone('America/Los_Angeles')
            latest_pacific_datetime = datetime.strptime(latest_pacific_datetime,'%Y-%m-%d %H:%M:%S %Z')
            # Localize the datetime object to Pacific Time
            latest_pacific_datetime = pacific_tz.localize(latest_pacific_datetime)
            latest_datetime_utc = latest_pacific_datetime.astimezone(pytz.utc)
            unix_timestamp = int(latest_datetime_utc.timestamp())
        
        return unix_timestamp


    def ms_reformat(self, milliseconds):
        total_seconds = milliseconds / 1000
        minutes = int(total_seconds // 60)
        seconds = int(total_seconds % 60)

        return f"{minutes}:{seconds:02}"
    

    def utc_to_pt(self, utc_ts):
        pacific_tz = pytz.timezone('America/Los_Angeles')
        utc_dt = datetime.strptime(utc_ts, "%Y-%m-%dT%H:%M:%S.%fZ")
        utc_dt = utc_dt.replace(tzinfo=pytz.utc)
        pt_dt = utc_dt.astimezone(pacific_tz)
        pt_fmt = pt_dt.strftime("%Y-%m-%d %H:%M:%S %Z")
        return pt_fmt


    def songs_to_csv(self):
        recent_tracks_json = self.api_request_recently_played()

        rt_rows = []
        for item in recent_tracks_json['items']:
            track_id = item['track']['id']
            track_name = item['track']['name']
            artists_names = ", ".join([artist['name'] for artist in item['track']['artists']])
            utc_timestamp = item['played_at']
            played_at = self.utc_to_pt(utc_timestamp)
            duration_ms = item['track']['duration_ms']
            track_duration = self.ms_reformat(duration_ms)
            spotify_url = item['track']['external_urls']['spotify']

            rt_rows.append({'track_id': track_id, 'name': track_name, 'artists': artists_names, 'played_at': played_at, 'track_duration': track_duration, 'spotify_url': spotify_url})

        rt_df = pd.DataFrame(rt_rows)
        rt_df = df.sort_values(by='played_at', ascending=True)
        rt_df = rt_df.iloc[1:,:]


        track_ids_list = rt_df['track_id'].to_list()
        audio_features_json = self.api_request_audio_features(track_ids_list)

        af_rows = []
        for item in audio_features_json['audio_features']:
            track_id = item['id']
            danceability = item['danceability'],
            energy = item['energy'],
            key = item['key'],
            loudness = item['loudness'],
            mode = item['mode'],
            speechiness = item['speechiness'],
            acousticness = item['acousticness'],
            instrumentalness = item['instrumentalness'],
            liveness = item['liveness'],
            valence = item['valence'],
            tempo = item['temp']

            af_rows.append({'track_id': track_id, 'danceability': danceability, 'energy': energy, 'key': key, 'loudness': loudness, 'mode': mode, 'speechiness': speechiness, 'acousticness': acousticness, 'instrumentalness': instrumentalness, 'liveness': liveness, 'valence': valence, 'tempo': tempo})

        af_df = pd.DataFrame(af_rows)

        df = pd.merge(rt_df, af_df, on='track_id', how='left')
        df = df.sort_values(by='played_at', ascending=False)

        # Find the earliest 'played_at' value in the DataFrame
        latest_played_at = df['played_at'].max()

        # Convert the earliest 'played_at' timestamp to a format suitable for filenames
        # Remove timezone info and replace colons with dashes for filename safety
        latest_dt = datetime.strptime(latest_played_at, "%Y-%m-%d %H:%M:%S %Z")
        formatted_dt = latest_dt.strftime('%Y-%m-%d_%H-%M-%S')

        os.makedirs(f"/opt/airflow/tmp/", exist_ok=True)
        df.to_csv(f"/opt/airflow/tmp/spotify_tracks_{formatted_dt}.csv", index=False)


    def retrieve_songs(self):
        self.AUTH_TOKEN = get_token.refresh()
        self.CC_TOKEN = get_token.get_cc_access_token()
        self.songs_to_csv()

    


