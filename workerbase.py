import os
import aioredis
import uvloop

class WorkerBase:
    def __init__(self):
        self._redis = None
        self.tasks = []
        self.running = True
    
    @property
    def redis(self) -> aioredis.Redis:
        return self._redis
    
    async def main(self):
        self._redis = await aioredis.create_redis_pool(
            (os.environ["REDIS_HOST"], os.environ.get("REDIS_PORT", 6379)),
            password=os.environ["REDIS_PASSWORD"]
        )
        
        try:
            await self.start()
        except KeyboardInterrupt:
            self.running = False

        # Close Redis
        self.redis.close()
        await self.redis.wait_closed()

        # Kill all tasks.
        for task in self.tasks:
            await task

uvloop.install()
