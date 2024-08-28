import base64
import requests
import os


class get_token:
    def __init__(self):
        self.AUTH_BASE64= os.getenv('AUTH_BASE64')
        self.TOKEN_URL = 'https://accounts.spotify.com/api/token'
        self.REFRESH_TOKEN= os.getenv('REFRESH_TOKEN')


    # Get access token using authorization code flow grant type by using refresh token from initial authoriation
    def refresh(self):
        data={
            "grant_type": "refresh_token",
            "refresh_token": self.REFRESH_TOKEN
        }
        headers={
            "Authorization": "Basic " + self.AUTH_BASE64
        }
        response = requests.post(
            self.TOKEN_URL,
            data=data,
            headers=headers,
        )

        response_json = response.json()
        access_token = response_json['access_token']

        return access_token
    

    # Get access token using client credentials grant type
    def get_cc_access_token(self):
        headers = {
            'Authorization': f'Basic {self.AUTH_BASE64}',
            'Content-Type': 'application/x-www-form-urlencoded'
        }
        data = {
            'grant_type': 'client_credentials'
        }
        response = requests.post(
            self.TOKEN_URL,
            headers=headers,
            data=data
        )
        response_json = response.json()
        cc_access_token = response_json['access_token']

        return cc_access_token