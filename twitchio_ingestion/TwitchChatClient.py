import twitchio
from twitchio.ext import commands
from kafka import KafkaProducer

import json

producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

class TwitchChatClient(commands.Bot):
    def __init__(self, token_manager, bot_name, channels, prefix="!"):
        self.token_manager = token_manager
        self.bot_name = bot_name
        self.channels = channels
        self.prefix = prefix

        super().__init__(
            token=f"oauth:{self.token_manager.get_token()}",
            client_id=self.token_manager.client_id,
            nick=self.bot_name,
            prefix=self.prefix,
            initial_channels=self.channels,
        )

    async def event_ready(self):
        print(f"Bot {self.nick} is ready to receive messages!")

    async def event_message(self, message):
        producer.send('twitch_chat', {'channel': message.channel.name, 'message': message.content})

