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

    def get_new_token(self):
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
