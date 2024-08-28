import pandas as pd
import os
import requests
import json
import requests
from datetime import datetime, timezone
import pytz
from operators.auth_token import get_token
from google.cloud import bigquery
from airflow.exceptions import AirflowSkipException
import re



class get_recent_tracks:
    def __init__(self):
        self.REDIRECT_URI= os.environ.get("REDIRECT_URI")
        self.SP_GET_USER_RP_URL = 'https://api.spotify.com/v1/me/player/recently-played'
        self.SP_GET_AUDIO_FT_URL = 'https://api.spotify.com/v1/audio-features'
        self.DATASET = os.environ.get("BQ_DATASET")
        self.PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
        self.TABLE = os.environ.get("BQ_TABLE")
        self.AUTH_TOKEN = ""
        self.CC_TOKEN = ""

    # Submit a request to Spotify Web API to obtain json of user's recently played tracks
    def api_request_recently_played(self):
        timestamp = self.get_timestamp()

        headers = {
            'Authorization': f'Bearer {self.AUTH_TOKEN}',
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        }
        params = {
            'limit': 50, # This is the maximum items retrievable from the API using this scope
            'after': f"{timestamp}" # Response will include recently played songs after this time
        }
        response = requests.get(
            self.SP_GET_USER_RP_URL,
            headers=headers,
            params=params
        )
        recent_tracks_json = response.json()

        if (not recent_tracks_json.get('items') or 
        not isinstance(recent_tracks_json, dict) or 
        len(recent_tracks_json.get('items', [])) < 1):
            # Stops DAG if there are no items returned from the API request
            raise AirflowSkipException("No new tracks played recently on Spotify. Stopping DAG execution.")

        return recent_tracks_json

    # Submit a request to Spotify Web API to obtain the audio features of the listed track_id's
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
        
        # Query to get the latest "played_at" timestamp from the table
        query = f"""
        SELECT
            MAX(played_at) AS latest_datetime
        FROM
            `{self.PROJECT_ID}.{self.DATASET}.{self.TABLE}`
        """
        
        query_job = client.query(query)
        result = query_job.result()

        row = next(result, None)
        # Defines the reference unix timestamp for the API request for recently played tracks
        if row is None or row.latest_datetime is None:
            print("No rows returned from the query. Using default August 1, 2024 timestamp for API request.")
            unix_timestamp = 1722556800010 # unix_timestamp for August 1, 2024 00:00:00 PT
        else:
            latest_datetime = row.latest_datetime
            print(f"Latest datetime found is {latest_datetime}. Using this as parameter to request songs from API played after this time.")
            unix_timestamp = int(latest_datetime.timestamp() * 1000)

        return unix_timestamp

    # Converts milliseconds to a formatted string to show minutes:seconds
    def ms_reformat(self, milliseconds):
        total_seconds = milliseconds / 1000
        minutes = int(total_seconds // 60)
        seconds = int(total_seconds % 60)

        return f"{minutes}:{seconds:02}"
    
    # Converts timestamps from UTC to Pacific Time in a formatted string
    def utc_to_pt(self, utc_ts):
        pacific_tz = pytz.timezone('America/Los_Angeles')
        pt_dt = utc_ts.astimezone(pacific_tz)
        pt_fmt = pt_dt.strftime("%Y-%m-%d %H:%M:%S")

        return pt_fmt

    # Submits two API requests, aggregates data into one dataframe, then saves to local .csv
    def songs_to_csv(self):
        # Sends API request to retrieve recently played tracks
        recent_tracks_json = self.api_request_recently_played()

        # Creates timestamp value for "upload_timestamp" column
        dt = datetime.now(timezone.utc) 
        print(f"Current timestamp is {dt}..using this for upload_timestamp field.")

        rt_rows = []
        # Loops through items in the response json and extracts relevant fields
        for item in recent_tracks_json['items']:
            track_id = item['track']['id']
            track_name = item['track']['name']
            # Creates a comma-separated string of all artist names
            artists_names = ", ".join([artist['name'] for artist in item['track']['artists']])
            played_at = item['played_at']
            duration_ms = item['track']['duration_ms']
            # Creates reformatted field of the track duration
            track_duration = self.ms_reformat(duration_ms)
            spotify_url = item['track']['external_urls']['spotify']

            # Extracts alphanumeric characters from the played_at timestamp
            ts_clean = re.sub(r'\W+', '', played_at)
            # Create unique identifier
            unique_id = f"{track_id}{ts_clean}"

            # Creates a dictionary of key:value pairs and appends it to the list as a row
            rt_rows.append({
                'track_id': track_id,
                'track_name': track_name,
                'artists': artists_names,
                'played_at': played_at,
                'duration_ms': duration_ms,
                'track_duration': track_duration,
                'spotify_url': spotify_url,
                'upload_timestamp': dt,
                'unique_id': unique_id
            })
        
        # Creates dataframe using list of rows
        rt_df = pd.DataFrame(rt_rows)
        rt_df = rt_df.sort_values(by='played_at', ascending=True)
        size = len(rt_df)
        print(f"**Dataframe has total of {size} records**")

        # Creates list of all unique track_ids
        track_ids_list = rt_df['track_id'].drop_duplicates().to_list()
        # Sends API request to retrieve audio features for each track_id
        audio_features_json = self.api_request_audio_features(track_ids_list)

        af_rows = []
        # Loops through response json and extracts relevant fields
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

            # Creates a dictionary of key:value pairs and appends it to the list as a row
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

        # Creates dataframe using list of rows
        af_df = pd.DataFrame(af_rows)

        # Performs one to many left join on track_id's to add respective audio features to recently played tracks
        df = pd.merge(rt_df, af_df, on='track_id', how='left')
        df = df.sort_values(by='played_at', ascending=False)

        # Type-cast
        df['track_id'] = df['track_id'].astype(str)
        df['track_name'] = df['track_name'].astype(str)
        df['artists'] = df['artists'].astype(str)
        df['played_at'] = pd.to_datetime(df['played_at'],format='ISO8601')
        df['duration_ms'] = df['duration_ms'].astype(int)
        df['track_duration'] = df['track_duration'].astype(str)
        df['spotify_url'] = df['spotify_url'].astype(str)
        df['upload_timestamp'] = pd.to_datetime(df['upload_timestamp'])
        df['unique_id'] = df['unique_id'].astype(str)
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
        # Remove timezone and replace colons with dashes for filename safety
        latest_played_at = self.utc_to_pt(latest_played_at)
        latest_dt = datetime.strptime(latest_played_at, "%Y-%m-%d %H:%M:%S")
        formatted_dt = latest_dt.strftime('%Y-%m-%d_%H-%M')
        # Makes a temporary directory and saves dataframe to .csv to it
        os.makedirs(f"/opt/airflow/working/", exist_ok=True)
        df.to_csv(f"/opt/airflow/working/spotify_tracks_{formatted_dt}.csv", index=False)


    # Calls on functions to retrieve access tokens and execute main songs_to_csv() function
    def retrieve_songs(self):
        gt = get_token()
        self.AUTH_TOKEN = gt.refresh()
        self.CC_TOKEN = gt.get_cc_access_token()
        self.songs_to_csv()

    


