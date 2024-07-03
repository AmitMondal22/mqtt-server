import asyncio
import logging

logging.basicConfig(level=logging.INFO)

class MQTTBroker:
    def __init__(self):
        self.clients = {}  # Dictionary to store connected clients and their subscriptions

    async def handle_client(self, reader, writer):
        client_id = None
        try:
            while True:
                data = await reader.readline()
                if not data:
                    break
                message = data.decode().strip()
                logging.info(f"Received message from client {client_id}: {message}")

                if message.startswith("CONNECT"):
                    client_id = message.split(" ")[1]
                    self.clients[client_id] = writer
                    logging.info(f"Client {client_id} connected")

                elif message.startswith("PUBLISH"):
                    _, topic, payload = message.split(" ", 2)
                    await self.handle_publish(topic, payload)

                elif message.startswith("SUBSCRIBE"):
                    _, topic = message.split(" ", 1)
                    await self.handle_subscribe(client_id, topic)

                elif message.startswith("DISCONNECT"):
                    await self.handle_disconnect(client_id)
                    break

        except asyncio.CancelledError:
            logging.info(f"Connection with client {client_id} closed abruptly")

        finally:
            await self.handle_disconnect(client_id)

    async def handle_publish(self, topic, payload):
        logging.info(f"Received PUBLISH to topic '{topic}': {payload}")
        # Implement logic to handle publishing to subscribed clients

    async def handle_subscribe(self, client_id, topic):
        logging.info(f"Client {client_id} subscribed to topic '{topic}'")
        # Implement logic to manage client subscriptions

    async def handle_disconnect(self, client_id):
        if client_id in self.clients:
            del self.clients[client_id]
            logging.info(f"Client {client_id} disconnected")

    async def start(self, host='localhost', port=1883):
        server = await asyncio.start_server(self.handle_client, host, port)
        logging.info(f"MQTT Broker started on {host}:{port}")

        async with server:
            await server.serve_forever()

if __name__ == "__main__":
    broker = MQTTBroker()
    asyncio.run(broker.start())
