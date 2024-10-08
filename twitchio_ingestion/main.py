from TokenManager import TokenManager
from TwitchChatClient import TwitchChatClient

import json
import os

# Usage example
if __name__ == "__main__":
    print("starting up...")
    config_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "config.json"
    )

    print("retrieving configs...")
    with open(config_path) as config_file:
        config = json.load(config_file)

    client_id = config["client_id"]
    client_secret = config["client_secret"]
    authorization_code = config["authorization_code"]
    redirect_uri = config["redirect_uri"]
    bot_name = "streamvibesbot"
    channels = config["channels"]

    print("setting up token manager...")
    # Instantiate the TokenManager
    token_manager = TokenManager(client_id, client_secret,authorization_code, redirect_uri)

    print(f"OAuth Token: oauth:{token_manager.get_token()}")
    print(f"Bot Name: {bot_name}")

    print("setting up twitch chat client...")
    # Instantiate the TwitchChatClient with the TokenManager
    chat_client = TwitchChatClient(token_manager, bot_name, channels)

    print("running the bot...")
    # Run the bot
    chat_client.run()
