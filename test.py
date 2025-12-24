#!/usr/bin/env python3
"""
STABLE FACEBOOK STREAM MANAGER (CORE VERSION)
- Stable FFmpeg execution
- Auto restart on crash
- No rotation
- No Telegram
- No watcher
- Designed to NEVER shutdown FFmpeg randomly
"""

import subprocess
import time
import signal
import sys
import threading
from dataclasses import dataclass
from typing import Dict

# ================= CONFIG =================

RESTART_DELAY = 120          # seconds
MAX_RESTARTS = 5

# ================= DATA =================

@dataclass
class StreamItem:
    id: str
    name: str
    source: str
    stream_url: str

active_streams: Dict[str, subprocess.Popen] = {}
restart_attempts: Dict[str, int] = {}
system_state = "running"

# ================= LOG =================

def log(msg: str):
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {msg}", flush=True)

# ================= FFMPEG =================

def build_ffmpeg_cmd(item: StreamItem):
    return [
        "ffmpeg",
        "-hide_banner",
        "-loglevel", "warning",

        "-thread_queue_size", "4096",
        "-i", item.source,

        "-map", "0:v:0?",
        "-map", "0:a:0?",

        "-c:v", "libx264",
        "-preset", "veryfast",
        "-tune", "zerolatency",
        "-pix_fmt", "yuv420p",
        "-profile:v", "main",
        "-level", "4.1",
        "-g", "50",
        "-keyint_min", "50",
        "-sc_threshold", "0",

        "-c:a", "aac",
        "-b:a", "128k",
        "-ac", "2",
        "-ar", "44100",

        "-f", "flv",
        item.stream_url
    ]

def start_ffmpeg(item: StreamItem):
    if system_state != "running":
        return

    attempts = restart_attempts.get(item.id, 0)
    if attempts >= MAX_RESTARTS:
        log(f"❌ {item.name} stopped permanently (max restarts reached)")
        return

    log(f"▶ STARTING FFMPEG: {item.name}")
    cmd = build_ffmpeg_cmd(item)

    try:
        proc = subprocess.Popen(
            cmd,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            text=True
        )

        active_streams[item.id] = proc
        restart_attempts[item.id] = 0

        threading.Thread(
            target=monitor_ffmpeg,
            args=(item, proc),
            daemon=True
        ).start()

        log(f"✅ FFMPEG STARTED (PID {proc.pid})")

    except FileNotFoundError:
        log("❌ FFmpeg not installed")
        sys.exit(1)
    except Exception as e:
        log(f"❌ Failed to start FFmpeg: {e}")

def monitor_ffmpeg(item: StreamItem, proc: subprocess.Popen):
    proc.wait()
    code = proc.returncode

    if system_state != "running":
        return

    log(f"🔴 FFMPEG EXITED ({item.name}) CODE={code}")

    active_streams.pop(item.id, None)

    restart_attempts[item.id] = restart_attempts.get(item.id, 0) + 1
    attempt = restart_attempts[item.id]

    if attempt > MAX_RESTARTS:
        log(f"❌ {item.name} reached max restart attempts")
        return

    log(f"🔄 Restarting {item.name} in {RESTART_DELAY}s (attempt {attempt})")
    time.sleep(RESTART_DELAY)
    start_ffmpeg(item)

def stop_all():
    global system_state
    system_state = "stopping"

    log("🛑 STOPPING ALL STREAMS")

    for sid, proc in list(active_streams.items()):
        try:
            log(f"🛑 Stopping PID {proc.pid}")
            proc.terminate()
            proc.wait(timeout=5)
        except:
            proc.kill()

    active_streams.clear()
    log("👋 Shutdown complete")
    sys.exit(0)

# ================= SIGNALS =================

def handle_signal(sig, frame):
    stop_all()

signal.signal(signal.SIGINT, handle_signal)
signal.signal(signal.SIGTERM, handle_signal)

# ================= MAIN =================

if __name__ == "__main__":

    # 🔧 CHANGE THIS TO YOUR REAL VALUES
    STREAM = StreamItem(
        id="stream1",
        name="Test Stream",
        source="http://dhoomtv.xyz/8zpo3GsVY7/beneficial2concern/274162",
        stream_url="rtmps://live-api-s.facebook.com:443/rtmp/FB-837586635754528-0-Ab3ellxfTai6csWiddUIIoRK"
    )

    log("🚀 STREAM MANAGER STARTED")
    start_ffmpeg(STREAM)

    while system_state == "running":
        time.sleep(1)
