import webbrowser
import json
import os

config_path = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "config.json"
)

with open(config_path) as config_file:
    config = json.load(config_file)



client_id = config["client_id"]
redirect_uri = 'https://localhost' 
scope = 'chat:read'

auth_url = (
    f"https://id.twitch.tv/oauth2/authorize?client_id={client_id}"
    f"&redirect_uri={redirect_uri}&response_type=code&scope={scope}"
)

webbrowser.open(auth_url)
