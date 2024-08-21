import asyncio

import websockets

clients = {}  # {websocket: name}


async def client_handler(websocket, path):
    print("New client", websocket)
    print(" ({} existing clients)".format(len(clients)))

    # The first line from the client is the name
    name = await websocket.recv()
    await websocket.send("Welcome to websocket-chat, {}".format(name))
    await websocket.send("There are {} other users connected: {}".format(len(clients), list(clients.values())))
    clients[websocket] = name
    for client in clients:
        if client != websocket:
            await client.send(name + " has joined the chat")

    # Handle messages from this client
    try:
        while True:
            message = await websocket.recv()

            # Send message to all clients
            for client in clients:
                await client.send("{}: {}".format(name, message))
    except websockets.exceptions.ConnectionClosed:
        del clients[websocket]
        print("Client closed connection", websocket)
        for client in clients:
            if client != websocket:
                await client.send(name + " has left the chat")


start_server = websockets.serve(client_handler, "192.168.1.105", 8765)

asyncio.get_event_loop().run_until_complete(start_server)
asyncio.get_event_loop().run_forever()
