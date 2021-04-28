import asyncio
import aioredis

try:
    import orjson
    import orjson as json
except ImportError:
    print("orjson not installed")
    orjson = None
    import json

from workerbase import WorkerBase

# https://stackoverflow.com/questions/287871/how-to-print-colored-text-to-the-terminal
class CLIColors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

class DisplayClient(WorkerBase):
    def __init__(self):
        super().__init__()

        self.messages = []

    async def start(self):
        # self.tasks.append(asyncio.create_task(self.stats_loop()))
        await self.connect_redis()

    async def connect_redis(self):
        mpsc = aioredis.pubsub.Receiver()
        await self.redis.subscribe(mpsc.channel('filebeat'))
        async for _channel, msg in mpsc.iter():
            try:
                await self.on_message(msg)
            except KeyError as e:
                print("missing key", e)

    async def stats_loop(self):
        while self.running:
            await asyncio.sleep(1)
            if self.messages:
                print(len(self.messages))
                self.messages.clear()

    async def on_message(self, message):
        message = json.loads(message)
        self.messages.append(message)
        # print(json.dumps(message).decode("utf8"))
        # print("-"*30)
        container_name = message.get("container", {}).get("name", None)
        print_name = message["host"]["name"]

        if container_name:
            print_name = print_name + ", " + container_name
        print(f'{print_name} - {message["message"]}')

if __name__ == "__main__":
    client = DisplayClient()
    asyncio.get_event_loop().run_until_complete(client.main())