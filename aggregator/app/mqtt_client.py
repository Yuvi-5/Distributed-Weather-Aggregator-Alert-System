import asyncio
import asyncio
import json
import logging
import time
from typing import Tuple

import paho.mqtt.client as mqtt

from .config import Settings

logger = logging.getLogger(__name__)


def start_mqtt_listener(
    settings: Settings, loop: asyncio.AbstractEventLoop, queue: asyncio.Queue
) -> mqtt.Client:
    """
    Start a paho-mqtt client in its own network loop thread and push messages onto
    an asyncio queue for ingestion.
    Includes simple retry on initial connect so we don't crash if broker isn't ready yet.
    """
    client = mqtt.Client()
    if settings.mqtt_username and settings.mqtt_password:
        client.username_pw_set(settings.mqtt_username, settings.mqtt_password)

    def on_connect(client: mqtt.Client, userdata, flags, rc):
        if rc == 0:
            client.subscribe("city/+/observations")
            logger.info("Connected to MQTT broker and subscribed to city/+/observations")
        else:
            logger.error("MQTT connection failed with code %s", rc)

    def on_message(client: mqtt.Client, userdata, msg: mqtt.MQTTMessage):
        try:
            payload = json.loads(msg.payload.decode("utf-8"))
            asyncio.run_coroutine_threadsafe(queue.put((msg.topic, payload)), loop)
        except Exception as exc:
            logger.warning("Failed to handle MQTT message on %s: %s", msg.topic, exc)

    client.on_connect = on_connect
    client.on_message = on_message

    # Retry connect a few times to avoid crashing during broker startup.
    for attempt in range(10):
        try:
            client.connect(settings.mqtt_host, settings.mqtt_port, keepalive=60)
            break
        except Exception as exc:
            logger.warning(
                "MQTT connect failed (attempt %s/10): %s", attempt + 1, exc
            )
            time.sleep(2)
    else:
        raise RuntimeError("MQTT broker not reachable after retries")

    client.loop_start()
    return client


def stop_mqtt_listener(client: mqtt.Client) -> None:
    try:
        client.loop_stop()
    finally:
        client.disconnect()
