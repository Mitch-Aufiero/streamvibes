import twitchio
from twitchio.ext import commands


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
        print(message.content)

        # send message to kafka topic
