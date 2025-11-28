import json
import subprocess
import sys
import time
import webbrowser
from datetime import datetime, timezone, timedelta
from pathlib import Path
from urllib.error import URLError, HTTPError
from urllib.request import Request, urlopen
import http.client
import shutil
import threading

try:
    import paho.mqtt.client as mqtt  # for local publishing of demo alerts
except ImportError:
    mqtt = None


ROOT = Path(__file__).parent.resolve()
COMPOSE_FILE = ROOT / "infra" / "docker-compose.yml"
FRONTEND_DIR = ROOT / "frontend"
ALERT_CLIENT = ROOT / "alert_client.py"

API_BASE = "http://localhost:8080"
FRONTEND_URL = "http://localhost:3000"
API_KEY = "devkey"  # keep in sync with infra/docker-compose.yml


def run_cmd(cmd, cwd=None):
    result = subprocess.run(
        cmd,
        cwd=cwd,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    stdout = result.stdout or ""
    stderr = result.stderr or ""
    if result.returncode != 0:
        raise RuntimeError(
            f"Command failed ({' '.join(cmd)}): {stderr.strip() or stdout.strip()}"
        )
    return stdout.strip()


def ensure_docker():
    try:
        run_cmd(["docker", "--version"])
    except Exception as exc:
        sys.exit(f"[start] Docker not available: {exc}")


def compose_up():
    cmd = ["docker", "compose", "-f", str(COMPOSE_FILE), "up", "-d", "--build"]
    print("[start] Bringing up services via docker compose...")
    run_cmd(cmd)
    print("[start] Services starting...")


def wait_ready(timeout=120):
    url = f"{API_BASE}/ready"
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            req = Request(url)
            with urlopen(req) as resp:
                if resp.status == 200:
                    print("[start] Backend ready.")
                    return
        except HTTPError as exc:
            # 503 until ready
            if exc.code not in (503, 404):
                print(f"[start] Waiting for ready... {exc}")
        except (URLError, http.client.RemoteDisconnected) as exc:
            # Service not listening yet
            print(f"[start] Waiting for ready... {exc}")
        time.sleep(3)
    print("[start] Ready check timed out; continuing anyway.")


def seed_sample_events():
    """
    Publish a few synthetic observations via mosquitto_pub inside the broker container.
    """
    print("[start] Seeding sample MQTT observations...")
    now = datetime.now(timezone.utc)
    samples = []
    base = [
        ("toronto", 21.4, 0.55, 12.0, 1013, 0.2),
        ("brampton", 19.8, 0.62, 8.5, 1015, 0.0),
        ("mississauga", 20.5, 0.58, 10.2, 1014, 0.1),
    ]
    for i in range(3):
        for city, temp, hum, wind, pressure, rain in base:
            samples.append(
                {
                    "city_id": city,
                    "source": "start-seed",
                    "observed_at": (now - timedelta(minutes=5 * i)).isoformat(),
                    "temp_c": round(temp + (i * 0.3), 2),
                    "humidity": hum,
                    "wind_kph": wind,
                    "pressure_hpa": pressure,
                    "rain_mm": rain,
                }
            )

    for payload in samples:
        topic = f"city/{payload['city_id']}/observations"
        message = json.dumps(payload)
        cmd = [
            "docker",
            "compose",
            "-f",
            str(COMPOSE_FILE),
            "exec",
            "-T",
            "mosquitto",
            "mosquitto_pub",
            "-h",
            "mosquitto",
            "-p",
            "1883",
            "-t",
            topic,
            "-m",
            message,
        ]
        try:
            run_cmd(cmd)
        except Exception as exc:
            print(f"[start] Failed to seed message to {topic}: {exc}")
            break
    print("[start] Seed complete (if mosquitto_pub is available in the container).")


def seed_alert_triggers():
    """
    Publish extreme observations to trigger alerts (temp >35C, wind >40kph, humidity >0.85).
    """
    print("[start] Seeding alert-triggering observations...")
    now = datetime.now(timezone.utc)
    samples = [
        # Toronto: hot + windy
        {"city_id": "toronto", "temp_c": 37.5, "humidity": 0.58, "wind_kph": 45.0, "pressure_hpa": 1010, "rain_mm": 0.0},
        {"city_id": "toronto", "temp_c": 38.2, "humidity": 0.60, "wind_kph": 42.0, "pressure_hpa": 1010, "rain_mm": 0.0},
        {"city_id": "toronto", "temp_c": 36.8, "humidity": 0.57, "wind_kph": 46.5, "pressure_hpa": 1011, "rain_mm": 0.1},
        # Toronto: humid spike to trigger 15m humidity rule
        {"city_id": "toronto", "temp_c": 30.0, "humidity": 0.90, "wind_kph": 5.0, "pressure_hpa": 1012, "rain_mm": 0.0},
        {"city_id": "toronto", "temp_c": 29.5, "humidity": 0.88, "wind_kph": 6.0, "pressure_hpa": 1012, "rain_mm": 0.0},
    ]
    for idx, payload in enumerate(samples):
        payload["source"] = "alert-demo"
        payload["observed_at"] = (now - timedelta(minutes=idx)).isoformat()
        topic = f"city/{payload['city_id']}/observations"
        message = json.dumps(payload)
        cmd = [
            "docker",
            "compose",
            "-f",
            str(COMPOSE_FILE),
            "exec",
            "-T",
            "mosquitto",
            "mosquitto_pub",
            "-h",
            "mosquitto",
            "-p",
            "1883",
            "-t",
            topic,
            "-m",
            message,
        ]
        try:
            run_cmd(cmd)
        except Exception as exc:
            print(f"[start] Failed to send alert-triggering message to {topic}: {exc}")
            break
    print("[start] Alert trigger seeding done.")


def wait_for_alerts(city_id="toronto", timeout=120, poll=10):
    print(f"[start] Waiting up to {timeout}s for alerts for {city_id} ...")
    end = time.time() + timeout
    last_error = None
    while time.time() < end:
        try:
            url = f"{API_BASE}/cities/{city_id}/alerts?limit=5"
            req = Request(url, headers={"X-API-Key": API_KEY})
            with urlopen(req) as resp:
                body = resp.read().decode("utf-8")
                alerts = json.loads(body)
                if alerts:
                    print(f"[start] Alerts received:\n{json.dumps(alerts, indent=2)}")
                    return alerts
        except Exception as exc:
            last_error = exc
        time.sleep(poll)
    if last_error:
        print(f"[start] No alerts after waiting ({last_error})")
    else:
        print("[start] No alerts received within timeout.")
    return []


def start_alert_spammer(city_id="toronto", count=6, interval=5):
    """
    Publish demo alerts directly to MQTT alerts/{city} every interval seconds.
    This drives the alert client window without waiting for rule evaluation.
    """
    def publish_local(alert_payload: dict):
        if mqtt is None:
            raise RuntimeError("paho-mqtt not installed")
        client = mqtt.Client()
        client.connect("localhost", 1883, keepalive=30)
        client.loop_start()
        client.publish(f"alerts/{city_id}", json.dumps(alert_payload))
        client.loop_stop()
        client.disconnect()

    def _spam():
        for i in range(count):
            payload = {
                "city_id": city_id,
                "level": "warning",
                "rule": "demo",
                "message": f"Demo alert #{i+1}",
                "triggered_at": datetime.now(timezone.utc).isoformat(),
            }
            try:
                # Prefer local publish via paho; fallback to container mosquitto_pub.
                if mqtt:
                    publish_local(payload)
                    print(f"[start] Published demo alert {i+1}/{count} (local paho) to alerts/{city_id}")
                else:
                    cmd = [
                        "docker",
                        "compose",
                        "-f",
                        str(COMPOSE_FILE),
                        "exec",
                        "-T",
                        "mosquitto",
                        "mosquitto_pub",
                        "-h",
                        "mosquitto",
                        "-p",
                        "1883",
                        "-t",
                        f"alerts/{city_id}",
                        "-m",
                        json.dumps(payload),
                    ]
                    run_cmd(cmd)
                    print(f"[start] Published demo alert {i+1}/{count} (container) to alerts/{city_id}")
            except Exception as exc:
                print(f"[start] Failed to publish demo alert: {exc}")
                break
            time.sleep(interval)

    threading.Thread(target=_spam, daemon=True).start()


def start_frontend_server():
    print("[start] Launching frontend at http://localhost:3000 ...")
    return subprocess.Popen(
        [sys.executable, "-m", "http.server", "3000"], cwd=FRONTEND_DIR
    )


def open_browser():
    try:
        webbrowser.open(FRONTEND_URL)
    except Exception:
        pass


def demo_calls():
    print("[start] Demonstrating API calls...")
    endpoints = [
        f"/cities/toronto/observations?limit=3",
        f"/cities/toronto/aggregates?window=15%20minutes&limit=3",
        f"/cities/toronto/alerts?limit=3",
    ]
    for ep in endpoints:
        url = f"{API_BASE}{ep}"
        try:
            req = Request(url, headers={"X-API-Key": API_KEY})
            with urlopen(req) as resp:
                body = resp.read().decode("utf-8")
                print(f"\nGET {ep}\n{body}")
        except Exception as exc:
            print(f"[start] Failed GET {ep}: {exc}")


def main():
    if not COMPOSE_FILE.exists():
        sys.exit(f"[start] Missing compose file at {COMPOSE_FILE}")
    ensure_docker()
    compose_up()
    wait_ready()
    seed_sample_events()
    seed_alert_triggers()
    frontend_proc = start_frontend_server()
    alert_proc = None
    if ALERT_CLIENT.exists():
        print("[start] Launching alert client window...")
        alert_proc = subprocess.Popen([sys.executable, str(ALERT_CLIENT)])
    else:
        print("[start] Alert client script not found; skipping.")
    open_browser()
    # Start demo alert spammer for UI showcase.
    start_alert_spammer(city_id="toronto", count=6, interval=5)
    # Give the aggregator a minute to compute aggregates/alerts, then poll.
    wait_for_alerts(city_id="toronto", timeout=90, poll=10)
    demo_calls()
    print("\n[start] Frontend is live at http://localhost:3000")
    print("[start] Backend API: http://localhost:8080 (use X-API-Key: devkey)")
    print("[start] Press Ctrl+C to stop the local frontend server. Docker services stay up; run 'docker compose -f infra/docker-compose.yml down' to stop them.")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n[start] Stopping frontend server...")
        frontend_proc.terminate()
        if alert_proc:
            alert_proc.terminate()
        try:
            frontend_proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            frontend_proc.kill()
        if alert_proc:
            try:
                alert_proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                alert_proc.kill()
        print("[start] Done.")


if __name__ == "__main__":
    main()
