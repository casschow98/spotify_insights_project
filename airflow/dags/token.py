import base64
import requests
import os


class get_token:
    def __init__(self):
        # self.CLIENT_ID= os.getenv('CLIENT_ID')
        # self.CLIENT_SECRET= os.getenv('CLIENT_SECRET')
        self.AUTH_BASE64= os.getenv('AUTH_BASE64')
        self.TOKEN_URL = 'https://accounts.spotify.com/api/token'
        self.REFRESH_TOKEN= os.getenv('REFRESH_TOKEN')


    def refresh(self):
        # auth_string = self.CLIENT_ID + ":" + self.CLIENT_SECRET
        # auth_bytes = auth_string.encode("utf-8")
        # auth_base64 = str(base64.b64encode(auth_bytes), "utf-8")

        response = requests.post(
            self.TOKEN_URL,
            data={"grant_type": "refresh_token", "refresh_token": self.REFRESH_TOKEN},
            headers={"Authorization": "Basic " + self.AUTH_BASE64},
        )

        response_json = response.json()
        access_token = response_json['access_token']

        return access_token