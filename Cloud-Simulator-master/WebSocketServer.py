import asyncio
import json
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
import uvicorn
from dotenv import load_dotenv
from FeedDistributor import FeedDistributor

load_dotenv()

app = FastAPI()


class WebSocketServer:
    def __init__(self, host="0.0.0.0", port=8000):
        self.host = host
        self.port = port
        self.feed_distributor = FeedDistributor(poll_interval=0.5)  # poll every 500ms
        self.active_connections: set[WebSocket] = set()

    async def handle_connection(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.add(websocket)
        print(f"[WebSocketServer] Client connected: {websocket.client}")

        try:
            while True:
                msg = await websocket.receive_text()
                data = json.loads(msg)

                action = data.get("action")
                instruments = data.get("instruments", [])

                if action == "subscribe":
                    self.feed_distributor.subscribe(websocket, instruments)
                    await websocket.send_text(json.dumps({
                        "status": "subscribed",
                        "instruments": instruments
                    }))

                elif action == "unsubscribe":
                    self.feed_distributor.unsubscribe(websocket, instruments)
                    await websocket.send_text(json.dumps({
                        "status": "unsubscribed",
                        "instruments": instruments
                    }))

                else:
                    await websocket.send_text(json.dumps({"error": "Invalid action"}))

        except WebSocketDisconnect:
            print(f"[WebSocketServer] Client disconnected: {websocket.client}")
            self._cleanup(websocket)
        except Exception as e:
            print(f"[WebSocketServer] Error: {e}")
            self._cleanup(websocket)

    def _cleanup(self, websocket: WebSocket):
        self.feed_distributor.disconnect(websocket)
        self.active_connections.discard(websocket)

    def start(self):
        @app.websocket("/ws")
        async def websocket_endpoint(websocket: WebSocket):
            await self.handle_connection(websocket)

        print(f"[WebSocketServer] Starting on {self.host}:{self.port}")
        uvicorn.run(app, host=self.host, port=self.port)


if __name__ == "__main__":
    server = WebSocketServer(host="0.0.0.0", port=8000)
    server.start()
    