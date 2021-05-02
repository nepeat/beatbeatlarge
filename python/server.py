import asyncio
import os
import aioredis

import orjson as json

import socketio
from sanic import Sanic
from sanic.response import text

sio = socketio.AsyncServer(async_mode="sanic", cors_allowed_origins=[])
app = Sanic("logapp")
sio.attach(app)

# Handle setting up and cleaning up Redis.

async def beat_listener(mpsc):
    print("listener launched")
    async for _channel, msg in mpsc.iter():
        # print("message", flush=True)
        await sio.emit("message", json.loads(msg))

@app.listener("before_server_start")
async def init_redis(app, loop):
    app.ctx.mpsc = aioredis.pubsub.Receiver(loop=loop)
    app.ctx.redis = await aioredis.create_redis_pool(
        (os.environ["REDIS_HOST"], os.environ.get("REDIS_PORT", 6379)),
        password=os.environ["REDIS_PASSWORD"]
    )

    await app.ctx.redis.subscribe(app.ctx.mpsc.channel("beatbeatlarge"))
    app.ctx.beat_listener = loop.create_task(beat_listener(app.ctx.mpsc))

@app.listener("after_server_stop")
async def cleanup_redis(app, loop):
    print("redis stopping")
    app.ctx.mpsc.stop()
    app.ctx.redis.close()
    await app.ctx.redis.wait_closed() 

# Basic HTTP response to /
@app.get("/")
async def hello_world(request):
    return text("Hello, world.")

# socket.io handlers
@sio.event
def connect(sid, environ, auth):
    print('connect ', sid)

@sio.event
def disconnect(sid):
    print('disconnect ', sid)