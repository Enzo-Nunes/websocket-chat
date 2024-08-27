import asyncio
import logging
import os
import time

from aiohttp import ClientSession
from jwt import encode
from redis import asyncio as aioredis
from requests import post
from websockets import ConnectionClosed, serve

APP_HOST = os.getenv("APP_HOST", "localhost")
APP_PORT = os.getenv("APP_PORT", 8300)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

app_url = f"http://{APP_HOST}:{APP_PORT}"
stream_name = "webchat"
headers = {
    "Content-Type": "application/json",
    "Authorization": encode({"account_id": "GG_Websocket"}, "", algorithm="none"),
}
stream = post(
    f"{app_url}/streams",
    headers=headers,
    json={"stream_name": stream_name},
).json()
stream_id = stream["stream_id"]
clients = {}  # {websocket: (username, password)}

REDIS_HOST = stream["connection"]["host"]
REDIS_PORT = stream["connection"]["port"]

consumer = post(
    f"{app_url}/consumers",
    headers=headers,
    json={
        "consumer_name": "webchat",
        "stream_name": stream_name,
    },
).json()


async def send_heartbeats():
    while True:
        redis = await aioredis.from_url(
            f"redis://{REDIS_HOST}:{REDIS_PORT}",
            username=consumer["login"]["username"],
            password=consumer["login"]["password"],
        )
        await redis.zadd(f"{stream_id}:heartbeats", {consumer["login"]["username"]: int(time.time())})
        await redis.aclose()

        for user in clients.values():
            redis = await aioredis.from_url(f"redis://{REDIS_HOST}:{REDIS_PORT}", username=user[0], password=user[1])
            await redis.zadd(f"{stream_id}:heartbeats", {user[0]: int(time.time())})
            await redis.zadd(f"{stream_id}:heartbeats", {user[0]: int(time.time())})
            await redis.aclose()
        await asyncio.sleep(30)


async def read_messages():
    redis = await aioredis.from_url(
        f"redis://{REDIS_HOST}:{REDIS_PORT}",
        username=consumer["login"]["username"],
        password=consumer["login"]["password"],
        decode_responses=True,
    )
    message_id = "0"
    while True:
        messages = await redis.xread({stream_id: message_id})
        if messages:
            for message in messages[0][1]:
                message_id = message[0]
                content = message[1].get("message", "")
                for client in clients:
                    await client.send(content)


async def client_handler(websocket, path):
    print("New client", websocket)
    print(f" ({len(clients)} existing clients)")

    # The first line from the client is the name
    name = await websocket.recv()
    await websocket.send(f"Welcome to the Gossip_Girl chat, {name}")
    await websocket.send(
        f"There are {len(clients)} other users connected: {list(user[0] for user in clients.values())}"
    )
    for client in clients:
        await client.send(f"[System]: {name} has joined the chat")

    # Create a new producer for this client
    async with ClientSession() as session:
        async with session.post(
            f"{app_url}/producers",
            headers=headers,
            json={"producer_name": name, "stream_name": stream_name},
        ) as response:
            producer = await response.json()
    clients[websocket] = (producer["login"]["username"], producer["login"]["password"])

    redis = await aioredis.from_url(
        f"redis://{REDIS_HOST}:{REDIS_PORT}",
        username=producer["login"]["username"],
        password=producer["login"]["password"],
    )
    # Handle messages from this client
    try:
        while True:
            message = await websocket.recv()

            # Send message to the stream
            await redis.xadd(stream_id, {"message": f"[{name}]: {message}"})
    except ConnectionClosed:
        del clients[websocket]
        print("Client closed connection", websocket)
        for client in clients:
            if client != websocket:
                await client.send(name + " has left the chat")


start_server = serve(client_handler, "192.168.1.105", 8765)

asyncio.ensure_future(read_messages())
asyncio.ensure_future(send_heartbeats())
asyncio.get_event_loop().run_until_complete(start_server)
asyncio.get_event_loop().run_forever()
