import asyncio
import logging
import json
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
import uvicorn
from FeedDistributor import FeedDistributor

logger = logging.getLogger(__name__)

app = FastAPI()


class WebSocketServer:
    def __init__(self, host="0.0.0.0", port=8000):
        self.host = host
        self.port = port
        self.feed_distributor = FeedDistributor()
        self.active_connections = set()

    async def handle_connection(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.add(websocket)
        logger.info("Client connected: %s", websocket.client)

        # Create per-connection asyncio queue and register with FeedDistributor
        queue = asyncio.Queue(maxsize=1000)
        loop = asyncio.get_running_loop()
        self.feed_distributor.register(websocket, loop, queue)

        async def push_loop():
            """Reads from the queue and forwards ticks to the WebSocket client."""
            while True:
                try:
                    data = await queue.get()
                    await websocket.send_text(data)
                except Exception:
                    break

        push_task = asyncio.create_task(push_loop())
        try:
            while True:
                msg = await websocket.receive_text()
                data = json.loads(msg)

                action = data.get("action")
                instruments = data.get("instruments", [])

                if action == "subscribe":
                    self.feed_distributor.subscribe(websocket, instruments)
                elif action == "unsubscribe":
                    self.feed_distributor.unsubscribe(websocket, instruments)
                else:
                    await websocket.send_text(json.dumps({"error": "Invalid action"}))
        except WebSocketDisconnect:
            logger.info("Client disconnected: %s", websocket.client)
        except Exception as e:
            logger.error("Unexpected error in handle_connection: %s", e, exc_info=True)
        finally:
            push_task.cancel()
            self.cleanup(websocket)

    def cleanup(self, websocket: WebSocket):
        self.feed_distributor.disconnect(websocket)
        self.active_connections.discard(websocket)

    def start(self):
        @app.websocket("/ws")
        async def websocket_endpoint(websocket: WebSocket):
            await self.handle_connection(websocket)

        logger.info("Starting on %s:%d", self.host, self.port)
        uvicorn.run(app, host=self.host, port=self.port)
