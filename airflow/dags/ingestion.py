#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import os
import requests
import json
import requests
from datetime import datetime
import pytz
from token import get_token



class get_recent_tracks:
    def __init__(self):
        self.REDIRECT_URI= "http://localhost:8888"
        self.SPOTIFY_GET_USER_RP_URL = 'https://api.spotify.com/v1/me/player/recently-played'


    def api_request_recently_played(self):
        headers = {
            'Authorization': f'Bearer {self.ACCESS_TOKEN}',
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        }
        params = {
            'limit': 50
        }
        response = requests.get(self.SPOTIFY_GET_USER_RP_URL, headers=headers, params=params)
        recent_tracks = response.json()

        return recent_tracks


    def ms_reformat(self, milliseconds):
        total_seconds = milliseconds / 1000
        minutes = int(total_seconds // 60)
        seconds = int(total_seconds % 60)

        return f"{minutes}:{seconds:02}"
    

    def utc_to_pt_ts(self, utc_ts):
        pacific_tz = pytz.timezone('America/Los_Angeles')
        utc_dt = datetime.strptime(utc_ts, "%Y-%m-%dT%H:%M:%S.%fZ")
        utc_dt = utc_dt.replace(tzinfo=pytz.utc)
        pt_dt = utc_dt.astimezone(pacific_tz)
        pt_fmt = pt_dt.strftime("%Y-%m-%d %H:%M:%S %Z")
        return pt_fmt


    def songs_to_csv(self):
        recent_tracks = self.api_request_recently_played()

        rows = []
        for item in recent_tracks['items']:
            track_id = item['track']['id']
            track_name = item['track']['name']
            artists_names = ", ".join([artist['name'] for artist in item['track']['artists']])
            utc_timestamp = item['played_at']
            played_at = self.utc_to_pt_ts(utc_timestamp)
            duration_ms = item['track']['duration_ms']
            track_duration = self.ms_reformat(duration_ms)

            # Append row data to the list
            rows.append({'track_id': track_id, 'name': track_name, 'artists': artists_names, 'played_at': played_at, 'track_duration': track_duration})

        df = pd.DataFrame(rows)
        df = df.sort_values(by='played_at', ascending=False)

        # Find the earliest 'played_at' value in the DataFrame
        latest_played_at = df['played_at'].max()

        # Convert the earliest 'played_at' timestamp to a format suitable for filenames
        # Remove timezone info and replace colons with dashes for filename safety
        earliest_dt = datetime.strptime(latest_played_at, "%Y-%m-%d %H:%M:%S %Z")
        formatted_dt = earliest_dt.strftime('%Y-%m-%d_%H-%M-%S')

        os.makedirs(f"/opt/airflow/tmp/", exist_ok=True)
        df.to_csv(f"/opt/airflow/tmp/spotify_tracks_{formatted_dt}.csv", index=False)


    def retrieve_songs(self):
        self.ACCESS_TOKEN = get_token.refresh()
        self.api_request_recently_played()
        self.songs_to_csv()

    


