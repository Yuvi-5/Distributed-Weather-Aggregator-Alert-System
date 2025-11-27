import argparse
import json
import random
import time
from datetime import datetime, timezone

import paho.mqtt.client as mqtt


def build_payload(city_id: str, source: str) -> dict:
    return {
        "city_id": city_id,
        "source": source,
        "observed_at": datetime.now(timezone.utc).isoformat(),
        "temp_c": round(random.uniform(18, 32), 2),
        "humidity": round(random.uniform(0.35, 0.85), 2),
        "wind_kph": round(random.uniform(0, 25), 2),
        "pressure_hpa": round(random.uniform(1005, 1025), 2),
        "rain_mm": round(random.uniform(0, 5), 2),
    }


def main():
    parser = argparse.ArgumentParser(description="Edge node simulator for weather observations.")
    parser.add_argument("--host", default="localhost", help="MQTT broker host")
    parser.add_argument("--port", type=int, default=1883, help="MQTT broker port")
    parser.add_argument("--city", default="nyc", help="City ID")
    parser.add_argument("--source", default="edge-sim-1", help="Source ID")
    parser.add_argument("--interval", type=float, default=5.0, help="Seconds between publishes")
    args = parser.parse_args()

    client = mqtt.Client()
    client.connect(args.host, args.port, keepalive=60)
    client.loop_start()

    topic = f"city/{args.city}/observations"
    print(f"Publishing to {topic} every {args.interval}s (Ctrl+C to stop)")

    try:
        while True:
            payload = build_payload(args.city, args.source)
            client.publish(topic, json.dumps(payload))
            print(f"Published: {payload}")
            time.sleep(args.interval)
    except KeyboardInterrupt:
        print("Stopping simulator.")
    finally:
        client.loop_stop()
        client.disconnect()


if __name__ == "__main__":
    main()
