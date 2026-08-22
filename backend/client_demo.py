#!/usr/bin/env python3
"""
Interactive / Resilient WebSocket Test Client.

Demonstrates:
  - Persistent WebSocket connection to the gateway
  - Automatic reconnection with Exponential Backoff
  - Heartbeat / ping-pong handling
  - Receiving real-time events published anywhere across the cluster

Usage:
  python client_demo.py --client-id alice --port 8000
  python client_demo.py --client-id bob --port 8001
"""

import argparse
import asyncio
import json
import logging
import sys
import websockets

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("client")


async def run_client(client_id: str, host: str = "localhost", port: int = 8000):
    url = f"ws://{host}:{port}/ws/{client_id}"
    reconnect_delay = 1.0
    max_reconnect_delay = 30.0

    print("=" * 65)
    print(f"🚀 Real-Time Streaming Gateway Client Demo")
    print(f"👤 Client ID: {client_id}")
    print(f"🌐 Target:    {url}")
    print("=" * 65)

    while True:
        try:
            logger.info("Connecting to %s...", url)
            async with websockets.connect(url) as ws:
                logger.info(" Connected successfully to Gateway!")
                reconnect_delay = 1.0  # Reset delay on successful connection

                # Task 1: Listen for incoming messages & events
                async def receive_loop():
                    async for raw_msg in ws:
                        try:
                            msg = json.loads(raw_msg)
                            msg_type = msg.get("type", "unknown")
                            if msg_type == "ping":
                                logger.info(" Received heartbeat ping from server -> sending pong")
                                await ws.send(json.dumps({"type": "pong"}))
                            elif msg_type == "event":
                                logger.info(
                                    " [EVENT RECEIVED] Type: %s | Source: %s | Instance: %s | Payload: %s",
                                    msg.get("event_type"),
                                    msg.get("source_client_id"),
                                    msg.get("published_by_instance"),
                                    msg.get("payload"),
                                )
                            elif msg_type == "ack":
                                logger.info(" [SERVER ACK] %s", msg)
                            else:
                                logger.info(" [MESSAGE] %s", msg)
                        except json.JSONDecodeError:
                            logger.info(" [RAW] %s", raw_msg)

                # Task 2: Periodic local client event publisher (demo)
                async def publish_demo_loop():
                    counter = 1
                    while True:
                        await asyncio.sleep(15)
                        demo_event = {
                            "type": "publish",
                            "event_type": "chat.message",
                            "payload": {
                                "sender": client_id,
                                "text": f"Hello from {client_id}! (Sequence #{counter})",
                            }
                        }
                        logger.info("📤 Publishing WebSocket event: %s", demo_event["payload"]["text"])
                        await ws.send(json.dumps(demo_event))
                        counter += 1

                await asyncio.gather(receive_loop(), publish_demo_loop())

        except (websockets.ConnectionClosedError, websockets.ConnectionClosedOK) as exc:
            logger.warning("Connection closed: %s. Reconnecting in %.1fs...", exc, reconnect_delay)
        except Exception as exc:
            logger.error("Connection failed (%s: %s). Reconnecting in %.1fs...", type(exc).__name__, exc, reconnect_delay)

        await asyncio.sleep(reconnect_delay)
        reconnect_delay = min(reconnect_delay * 2, max_reconnect_delay)


def main():
    parser = argparse.ArgumentParser(description="Real-Time Gateway WebSocket Client")
    parser.add_argument("--client-id", type=str, default="client_1", help="Unique client identifier")
    parser.add_argument("--host", type=str, default="localhost", help="Gateway host")
    parser.add_argument("--port", type=int, default=8000, help="Gateway port")
    args = parser.parse_args()

    try:
        asyncio.run(run_client(client_id=args.client_id, host=args.host, port=args.port))
    except KeyboardInterrupt:
        logger.info("Client stopped by user.")
        sys.exit(0)


if __name__ == "__main__":
    main()
