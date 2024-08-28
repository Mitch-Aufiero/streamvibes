import time
import requests


class TokenManager:
    def __init__(self, client_id, client_secret):
        self.client_id = client_id
        self.client_secret = client_secret
        self.access_token = None
        self.expires_at = 0

    def get_new_token(self):
        """Request a new OAuth token from Twitch and store it."""
        url = "https://id.twitch.tv/oauth2/token"
        params = {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "grant_type": "client_credentials",
        }
        response = requests.post(url, params=params)
        response_data = response.json()

        expires_in = response_data["expires_in"]
        self.expires_at = time.time() + expires_in

        self.access_token = response_data["access_token"]

        print(f"New token retrieved, expires in {expires_in} seconds.")

    def get_token(self):
        """Get the current OAuth token, refreshing if necessary."""
        # check if the token is expiring within the next 300 seconds (5 minutes)
        if time.time() > self.expires_at - 300:
            print("Token is expiring soon, refreshing...")
            self.get_new_token()

        return self.access_token
