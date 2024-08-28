from TokenManager import TokenManager
from TwitchChatClient import TwitchChatClient

import json
import os

# Usage example
if __name__ == "__main__":

    config_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "config.json"
    )

    with open(config_path) as config_file:
        config = json.load(config_file)

    client_id = config["client_id"]
    client_secret = config["client_secret"]
    bot_name = "StreamVibesBot"
    channels = config["channels"]

    # Instantiate the TokenManager
    token_manager = TokenManager(client_id, client_secret)

    # Instantiate the TwitchChatClient with the TokenManager
    chat_client = TwitchChatClient(token_manager, bot_name, channels)

    # Run the bot
    chat_client.run()
