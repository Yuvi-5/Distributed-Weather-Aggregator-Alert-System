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


ROOT = Path(__file__).parent.resolve()
COMPOSE_FILE = ROOT / "infra" / "docker-compose.yml"
FRONTEND_DIR = ROOT / "frontend"

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
    frontend_proc = start_frontend_server()
    open_browser()
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
        try:
            frontend_proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            frontend_proc.kill()
        print("[start] Done.")


if __name__ == "__main__":
    main()
