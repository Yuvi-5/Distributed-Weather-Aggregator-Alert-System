import argparse
import json
import queue
import threading
import tkinter as tk
from datetime import datetime

try:
    import paho.mqtt.client as mqtt
except ImportError as exc:
    raise SystemExit(
        "paho-mqtt is required. Install with: pip install paho-mqtt"
    ) from exc


def fmt_alert(alert: dict) -> str:
    ts = alert.get("triggered_at")
    try:
        ts = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        ts_s = ts.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        ts_s = ts or ""
    return f"[{alert.get('level', '').upper()}] {alert.get('city_id', '')}: {alert.get('message', '')} @ {ts_s}"


def run_mqtt(host: str, port: int, topic: str, q: queue.Queue, status_cb):
    client = mqtt.Client()

    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            status_cb(f"Connected to MQTT {host}:{port}, subscribing {topic}")
            client.subscribe(topic)
        else:
            status_cb(f"MQTT connect failed: {rc}")

    def on_message(client, userdata, msg):
        try:
            payload = json.loads(msg.payload.decode("utf-8"))
            q.put(payload)
        except Exception:
            status_cb(f"Bad message on {msg.topic}")

    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(host, port, keepalive=60)
    client.loop_forever()


def main():
    parser = argparse.ArgumentParser(description="Live alert client")
    parser.add_argument("--host", default="localhost", help="MQTT host")
    parser.add_argument("--port", type=int, default=1883, help="MQTT port")
    parser.add_argument("--topic", default="alerts/#", help="MQTT topic to subscribe")
    args = parser.parse_args()

    q: queue.Queue = queue.Queue()

    root = tk.Tk()
    root.title("Live Alerts")
    root.geometry("480x320")

    status_var = tk.StringVar(value="Connecting...")
    tk.Label(root, textvariable=status_var).pack(anchor="w", padx=10, pady=5)

    listbox = tk.Listbox(root, height=12, width=70)
    listbox.pack(padx=10, pady=5, fill="both", expand=True)

    def status_cb(msg: str):
        status_var.set(msg)

    def drain_queue():
        updated = False
        while True:
            try:
                alert = q.get_nowait()
            except queue.Empty:
                break
            listbox.insert(0, fmt_alert(alert))
            updated = True
        if updated:
            listbox.yview_moveto(0)
        root.after(500, drain_queue)

    drain_queue()

    thread = threading.Thread(
        target=run_mqtt, args=(args.host, args.port, args.topic, q, status_cb), daemon=True
    )
    thread.start()

    root.mainloop()


if __name__ == "__main__":
    main()
