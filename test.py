#!/usr/bin/env python3
import subprocess, time, signal, sys, threading, requests, json, os
from dataclasses import dataclass
from typing import Dict

# ================= CONFIG =================
API_URL = "https://ani-box-nine.vercel.app/api/grok-chat"
GRAPH_VERSION = "v24.0"

ROTATION_INTERVAL = int(3.75 * 3600)  # 3h45m
RESTART_DELAY = 90
NEW_STREAM_DELAY = 5  # shorter for testing
FINAL_REPORT_DELAY = 300

DEFAULT_QUALITY = "auto"
CACHE_FILE = "stream_cache.json"

TELEGRAM_BOT_TOKEN = "7971806903:AAHwpdNzkk6ClL3O17JVxZnp5e9uI66L9WE"
TELEGRAM_CHAT_ID = "-1002181683719"

system_state = "running"

# ================= DATA =================
@dataclass
class StreamItem:
    id: str
    name: str
    source: str
    page_token: str
    live_id: str = ""
    stream_url: str = ""
    quality: str = DEFAULT_QUALITY

active_streams: Dict[str, subprocess.Popen] = {}
stream_items: Dict[str, StreamItem] = {}
rotation_timers = {}

# ================= LOG =================
def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)

# ================= TELEGRAM =================
def tg(msg):
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": msg},
            timeout=10
        )
    except Exception as e:
        log(f"❌ Telegram send failed: {e}")

# ================= CACHE =================
def load_cache():
    if not os.path.exists(CACHE_FILE):
        log("Cache not found, starting fresh.")
        return {}
    try:
        with open(CACHE_FILE, "r") as f:
            data = json.load(f)
            items = {}
            for sid, s in data.items():
                items[sid] = StreamItem(**s)
            log(f"Loaded {len(items)} streams from cache.")
            return items
    except Exception as e:
        log(f"❌ Failed to load cache: {e}")
        return {}

def save_cache():
    cache_data = {sid: vars(item) for sid, item in stream_items.items()}
    with open(CACHE_FILE, "w") as f:
        json.dump(cache_data, f, indent=2)
    log("Cache saved.")

# ================= FACEBOOK =================
def create_live(item: StreamItem):
    log(f"Creating live on Facebook for {item.name}...")
    r = requests.post(
        f"https://graph.facebook.com/{GRAPH_VERSION}/me/live_videos",
        data={"status": "LIVE_NOW", "title": item.name, "access_token": item.page_token},
        timeout=15
    )
    data = r.json()
    if "id" not in data:
        raise RuntimeError(f"Failed to create live for {item.name}: {data}")
    item.live_id = data["id"]
    log(f"Live created: live_id={item.live_id}")

def fetch_stream_url(item: StreamItem):
    log(f"Fetching stream_url for {item.name}...")
    r = requests.get(
        f"https://graph.facebook.com/{GRAPH_VERSION}/{item.live_id}",
        params={"fields": "status,stream_url", "access_token": item.page_token},
        timeout=15
    )
    data = r.json()
    if "stream_url" not in data:
        raise RuntimeError(f"No stream_url for {item.name}: {data}")
    item.stream_url = data["stream_url"]
    log(f"Stream URL fetched: {item.stream_url}")

# ================= FFMPEG =================
def ffmpeg_cmd(item: StreamItem):
    presets = {"auto": "veryfast", "low": "ultrafast", "medium": "veryfast", "high": "faster"}
    return [
        "ffmpeg",
        "-hide_banner", "-loglevel", "warning",
        "-re",
        "-i", item.source,
        "-map", "0:v?", "-map", "0:a?",
        "-c:v", "libx264",
        "-preset", presets[item.quality],
        "-pix_fmt", "yuv420p",
        "-c:a", "aac",
        "-f", "flv",
        item.stream_url
    ]

def start_ffmpeg(item: StreamItem):
    if item.id in active_streams or system_state != "running":
        return
    try:
        create_live(item)
        fetch_stream_url(item)
        log(f"▶ STARTING {item.name} | {item.stream_url}")
        proc = subprocess.Popen(
            ffmpeg_cmd(item),
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE
        )
        active_streams[item.id] = proc
        tg(f"🟢 STARTED {item.name}")
        schedule_rotation(item)
        threading.Thread(target=monitor_ffmpeg, args=(item, proc), daemon=True).start()
    except Exception as e:
        log(f"❌ START FAILED {item.name}: {e}")
        tg(f"❌ START FAILED {item.name}\n{e}")

def stop_ffmpeg(sid):
    proc = active_streams.pop(sid, None)
    if proc:
        log(f"Stopping FFmpeg for {sid}")
        try:
            proc.terminate()
            proc.wait(timeout=5)
        except:
            proc.kill()

def monitor_ffmpeg(item, proc):
    proc.wait()
    if system_state != "running":
        return
    log(f"🔴 CRASHED {item.name}")
    tg(f"🔴 CRASHED {item.name}")
    time.sleep(RESTART_DELAY)
    log(f"Restarting {item.name} after crash...")
    start_ffmpeg(item)

# ================= ROTATION =================
def schedule_rotation(item):
    if item.id in rotation_timers:
        rotation_timers[item.id].cancel()
    t = threading.Timer(ROTATION_INTERVAL, lambda: rotate_stream(item))
    t.daemon = True
    rotation_timers[item.id] = t
    t.start()
    log(f"Rotation scheduled for {item.name} in {ROTATION_INTERVAL} seconds.")

def rotate_stream(item):
    if system_state != "running":
        return
    log(f"🔄 ROTATING {item.name}")
    tg(f"🔄 ROTATING {item.name}")
    stop_ffmpeg(item.id)
    time.sleep(10)
    start_ffmpeg(item)

# ================= API WATCHER =================
def fetch_api():
    try:
        r = requests.get(API_URL, timeout=15)
        items = {}
        for i, s in enumerate(r.json()["data"]):
            items[str(i)] = StreamItem(
                id=str(i),              # use index as ID
                name=s["name"],
                source=s["source"],
                page_token=s["token"],  # map token → page_token
                quality=DEFAULT_QUALITY
            )
        log(f"Fetched {len(items)} streams from API.")
        return items
    except Exception as e:
        log(f"❌ API fetch failed: {e}")
        return stream_items

def watcher_loop():
    global stream_items
    while system_state == "running":
        new_items = fetch_api()

        # Start new streams
        for sid, item in new_items.items():
            if sid not in stream_items:
                log(f"New stream detected: {item.name}, starting in {NEW_STREAM_DELAY}s...")
                stream_items[sid] = item
                threading.Timer(NEW_STREAM_DELAY, lambda i=item: start_ffmpeg(i)).start()

        # Stop removed streams
        for sid in list(stream_items.keys()):
            if sid not in new_items:
                log(f"Stream removed from API: {stream_items[sid].name}, stopping...")
                stop_ffmpeg(sid)
                stream_items.pop(sid)

        save_cache()
        time.sleep(20)

# ================= FINAL DASH REPORT =================
def dash_report():
    lines = []
    for sid, item in stream_items.items():
        status = "🟢" if sid in active_streams else "🔴"
        lines.append(f"{status} {item.name} | {item.stream_url}")
    msg = "📡 DASH REPORT\n\n" + "\n".join(lines)
    log(msg)
    tg(msg)

# ================= SHUTDOWN =================
def shutdown(sig=None, f=None):
    global system_state
    system_state = "stopping"
    log("🛑 Stream Manager stopping")
    tg("🛑 Stream Manager stopping")
    for sid in list(active_streams.keys()):
        stop_ffmpeg(sid)
    dash_report()
    sys.exit(0)

signal.signal(signal.SIGINT, shutdown)
signal.signal(signal.SIGTERM, shutdown)

# ================= MAIN =================
if __name__ == "__main__":
    log("🚀 Stream Manager starting...")
    stream_items = load_cache()
    tg("🚀 Stream Manager ONLINE (FINAL Production)")
    threading.Thread(target=watcher_loop, daemon=True).start()
    while system_state == "running":
        time.sleep(1)
