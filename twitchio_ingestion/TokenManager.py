import time
import requests

#https://id.twitch.tv/oauth2/authorize?client_id=id&redirect_uri=https://localhost&response_type=token&scope=chat:read

class TokenManager:
    def __init__(self, client_id, client_secret,authorization_code,redirect_uri):
        self.client_id = client_id
        self.client_secret = client_secret
        self.authorization_code = authorization_code
        self.redirect_uri = redirect_uri
        self.access_token = None
        self.expires_at = 0
        self.refresh_token = ""

        self.get_init_token()

    def get_init_token(self):
        """Request a new OAuth token from Twitch and store it."""
        url = "https://id.twitch.tv/oauth2/token"
        params = {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "code": self.authorization_code,
            "grant_type": "authorization_code",
            "redirect_uri":self.redirect_uri
        }
        response = requests.post(url, params=params)
        
        response_data = response.json()

        
        if response.status_code >=400:
            raise Exception(f"Error occurred retrieving new twitch auth token: {response_data}")
   

        self.refresh_token = response_data["refresh_token"]
        expires_in = response_data["expires_in"]
        self.expires_at = time.time() + expires_in

        self.access_token = response_data["access_token"]

        print(f"Initial token retrieved, expires in {expires_in} seconds.")

    def get_token(self):
        """Get the current OAuth token, refreshing if necessary."""
        # check if the token is expiring within the next 300 seconds (5 minutes)
        if time.time() > self.expires_at - 300:
            print("Token is expiring soon, refreshing...")
            self.get_new_token()

        return self.access_token

    def refresh_access_token(self):
        """Refresh the OAuth access token using the refresh token."""
        url = "https://id.twitch.tv/oauth2/token"
        params = {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "grant_type": "refresh_token",
            "refresh_token": self.refresh_token  
        }
        response = requests.post(url, params=params)
        response_data = response.json()

        if response.status_code >= 400:
            raise Exception(f"Error occurred refreshing twicth auth token: {response_data}")

        self.access_token = response_data["access_token"]
        self.refresh_token = response_data.get("refresh_token", self.refresh_token) 
        self.expires_at = time.time() + response_data["expires_in"]

        print(f"Token refreshed, new token expires in {response_data['expires_in']} seconds.")
