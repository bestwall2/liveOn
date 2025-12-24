#!/usr/bin/env python3
"""
FACEBOOK MULTI STREAM MANAGER – ADVANCED
- dynamic list watcher
- cache stream_url
- auto add/remove streams
- final dash report
- Telegram bot commands
- 1:50 minute initial delay for ALL servers
- 30 second delay for NEW servers
- 2 minute delay for CRASHED servers
- 3:45 hour stream key rotation (NO quality checks)
- Server shutdown reports
- Stable stream IDs
"""

import os
import sys
import json
import time
import signal
import hashlib
import threading
import subprocess
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
import requests
from dataclasses import dataclass
import atexit

# ================= CONFIG =================

CONFIG = {
    "streamsApi": "https://ani-box-nine.vercel.app/api/grok-chat",
    "pollInterval": 20000,  # 20 seconds
    "telegram": {
        "botToken": "7971806903:AAHwpdNzkk6ClL3O17JVxZnp5e9uI66L9WE",
        "chatId": "-1002181683719",
    },
    "initialDelay": 110000,  # 1:50 minutes for ALL servers initial start
    "newServerDelay": 30000,  # 30 seconds for NEW servers
    "crashedServerDelay": 120000,  # 2 minutes (120 seconds) for CRASHED servers
    "rotationInterval": 13500000,  # 3:45 hours in milliseconds
}

CACHE_FILE = "./streams_cache.json"

# ================= DATA CLASSES =================

@dataclass
class StreamItem:
    id: str
    name: str
    token: str
    source: str

@dataclass
class StreamCache:
    liveId: str
    stream_url: str
    dash: str
    status: str = "UNKNOWN"

# ================= STATE =================

system_state = "running"
api_items: Dict[str, StreamItem] = {}
active_streams: Dict[str, subprocess.Popen] = {}
stream_cache: Dict[str, StreamCache] = {}
stream_start_times: Dict[str, float] = {}
stream_rotation_timers: Dict[str, threading.Timer] = {}
restart_timers: Dict[str, threading.Timer] = {}
server_states: Dict[str, str] = {}  # 'starting', 'running', 'restarting', 'rotating', 'failed'

# ================= LOGGER =================

def log(message: str):
    print(f"[{datetime.now().isoformat()}] {message}")

# ================= CACHE =================

def load_cache():
    global stream_cache
    if not os.path.exists(CACHE_FILE):
        log("📝 No cache file found, starting fresh")
        return
    
    try:
        with open(CACHE_FILE, 'r', encoding='utf-8') as f:
            data = f.read()
            if not data or data.strip() == "":
                log("📝 Cache file is empty, starting fresh")
                return
            
            json_data = json.loads(data)
            if not json_data:
                log("📝 Cache file has no entries, starting fresh")
                return
            
            for k, v in json_data.items():
                stream_cache[k] = StreamCache(**v)
            
            log(f"✅ Loaded {len(stream_cache)} cached streams from file")
    except Exception as e:
        log(f"❌ Error loading cache: {e}")
        # Backup corrupted cache
        try:
            backup_name = f"{CACHE_FILE}.corrupted.{int(time.time())}"
            os.rename(CACHE_FILE, backup_name)
            log(f"⚠️ Corrupted cache backed up to: {backup_name}")
        except:
            log("⚠️ Could not backup corrupted cache")
        stream_cache.clear()

def save_cache():
    try:
        data = {k: vars(v) for k, v in stream_cache.items()}
        with open(CACHE_FILE, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2)
        log(f"💾 Cache saved with {len(stream_cache)} streams")
    except Exception as e:
        log(f"❌ Error saving cache: {e}")

# ================= TELEGRAM =================

def tg(msg: str, chat_id: str = None):
    if chat_id is None:
        chat_id = CONFIG["telegram"]["chatId"]
    
    try:
        url = f"https://api.telegram.org/bot{CONFIG['telegram']['botToken']}/sendMessage"
        payload = {
            "chat_id": chat_id,
            "text": msg,
            "parse_mode": "HTML",
            "disable_web_page_preview": True
        }
        response = requests.post(url, json=payload, timeout=10)
        result = response.json()
        if not result.get("ok"):
            log(f"❌ Telegram error: {result.get('description')}")
    except Exception as e:
        log(f"❌ Telegram send error: {e}")

# ================= FACEBOOK API =================

def create_live(token: str, name: str) -> str:
    log(f"🌐 Creating Facebook Live for: {name}")
    url = "https://graph.facebook.com/v24.0/me/live_videos"
    payload = {
        "title": name,
        "status": "UNPUBLISHED",
        "access_token": token
    }
    
    response = requests.post(url, json=payload, timeout=30)
    data = response.json()
    
    if "error" in data:
        log(f"❌ Facebook API error: {data['error']['message']}")
        raise Exception(data["error"]["message"])
    
    live_id = data["id"]
    log(f"✅ Created Live ID: {live_id}")
    return live_id

def get_stream_and_dash(live_id: str, token: str) -> StreamCache:
    log(f"🌐 Getting stream URL for Live ID: {live_id}")
    fields = "stream_url,dash_preview_url,status"
    
    # Try for up to 30 seconds (15 attempts × 2 seconds)
    for i in range(15):
        try:
            url = f"https://graph.facebook.com/v24.0/{live_id}?fields={fields}&access_token={token}"
            response = requests.get(url, timeout=10)
            
            if not response.ok:
                log(f"⚠️ Facebook API error {response.status_code}, retrying...")
                time.sleep(2)
                continue
            
            data = response.json()
            
            if "error" in data:
                log(f"⚠️ Facebook API error: {data['error']['message']}, retrying...")
                time.sleep(2)
                continue
            
            if "stream_url" in data:
                log(f"✅ Stream URL ready for {live_id}")
                return StreamCache(
                    liveId=live_id,
                    stream_url=data["stream_url"],
                    dash=data.get("dash_preview_url", "N/A"),
                    status=data.get("status", "UNKNOWN")
                )
            
            log(f"⏳ Waiting for stream URL (attempt {i + 1}/15)...")
            
        except Exception as e:
            log(f"⚠️ Network error getting stream URL: {e}, retrying...")
        
        time.sleep(2)
    
    raise Exception("Preview not ready after 30 seconds")

# ================= STABLE STREAM ID GENERATION =================

def generate_stream_id(name: str, source: str) -> str:
    """Generate stable stream ID from name and source"""
    clean_name = name.strip()
    clean_source = source.strip()
    
    # Create hash from combined string
    combined = f"{clean_name}|{clean_source}"
    hash_obj = hashlib.md5(combined.encode())
    return f"stream_{hash_obj.hexdigest()[:8]}"

# ================= FFMPEG =================

def start_ffmpeg(item: StreamItem):
    cache = stream_cache.get(item.id)
    if not cache:
        log(f"❌ No cache for {item.name}, cannot start")
        return
    
    # Check if already starting or restarting
    if server_states.get(item.id) in ['starting', 'restarting']:
        log(f"⚠️ {item.name} is already starting/restarting, skipping")
        return
    
    log(f"▶ STARTING {item.name} (ID: {item.id})")
    server_states[item.id] = 'starting'
    
    # Clear any existing restart timer
    if item.id in restart_timers:
        restart_timers[item.id].cancel()
        del restart_timers[item.id]
    
    # Build FFmpeg command
    ffmpeg_cmd = [
        "ffmpeg",
        "-hide_banner",
        "-loglevel", "error",
        "-re",  # Read input at native frame rate
        "-thread_queue_size", "512",
        "-rtbufsize", "256M",
        "-probesize", "32",
        "-analyzeduration", "0",
        "-i", item.source,
        "-c:v", "libx264",
        "-preset", "ultrafast",
        "-tune", "zerolatency",
        "-b:v", "2500k",
        "-maxrate", "2500k",
        "-bufsize", "5000k",
        "-pix_fmt", "yuv420p",
        "-g", "50",
        "-r", "25",
        "-c:a", "aac",
        "-b:a", "96k",
        "-ar", "44100",
        "-ac", "2",
        "-f", "flv",
        "-flvflags", "no_duration_filesize",
        "-avoid_negative_ts", "make_zero",
        "-muxdelay", "0",
        "-muxpreload", "0",
        cache.stream_url
    ]
    
    try:
        # Start FFmpeg process
        proc = subprocess.Popen(
            ffmpeg_cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            stdin=subprocess.DEVNULL
        )
        
        active_streams[item.id] = proc
        stream_start_times[item.id] = time.time()
        server_states[item.id] = 'running'
        
        log(f"✅ FFmpeg started for {item.name} (ID: {item.id})")
        
        # Start rotation timer (3:45 hours)
        start_rotation_timer(item)
        
        # Monitor FFmpeg process in background thread
        threading.Thread(
            target=monitor_ffmpeg_process,
            args=(item, proc),
            daemon=True
        ).start()
        
    except Exception as e:
        log(f"❌ Error starting FFmpeg for {item.name}: {e}")
        handle_stream_crash(item, str(e))

def monitor_ffmpeg_process(item: StreamItem, proc: subprocess.Popen):
    """Monitor FFmpeg process and handle crashes"""
    try:
        # Wait for process to complete
        stdout, stderr = proc.communicate()
        return_code = proc.returncode
        
        if return_code != 0:
            error_msg = stderr.decode('utf-8', errors='ignore') if stderr else "Unknown error"
            log(f"🔚 FFmpeg ended with error for {item.name}: {error_msg[:100]}")
            handle_stream_crash(item, f"FFmpeg exited with code {return_code}")
        else:
            log(f"🔚 FFmpeg ended normally for {item.name}")
            handle_stream_crash(item, "Stream ended unexpectedly")
            
    except Exception as e:
        log(f"❌ Error monitoring FFmpeg for {item.name}: {e}")
        handle_stream_crash(item, str(e))

def handle_stream_crash(item: StreamItem, reason: str):
    state = server_states.get(item.id)
    
    # Don't send report if we're rotating or intentionally stopping
    if state == 'rotating':
        log(f"🔄 {item.name} crashed during rotation, will continue rotation process")
        return
    
    # Calculate uptime
    uptime = "Unknown"
    if item.id in stream_start_times:
        uptime_ms = (time.time() - stream_start_times[item.id]) * 1000
        uptime = format_uptime(uptime_ms)
    
    # Send crash report
    tg(f"🔴 <b>SERVER CRASH REPORT</b>\n\n"
       f"<b>{item.name}</b>\n"
       f"ID: {item.id}\n"
       f"Reason: {reason}\n"
       f"Uptime: {uptime}\n"
       f"Status: Will restart in 2 minutes")
    
    log(f"🔄 {item.name} (ID: {item.id}) will restart in 2 minutes")
    
    # Schedule restart in 2 MINUTES for crashed servers
    server_states[item.id] = 'restarting'
    stop_ffmpeg(item.id, skip_report=True)
    
    restart_timer = threading.Timer(
        CONFIG["crashedServerDelay"] / 1000,
        lambda: restart_stream(item)
    )
    restart_timer.daemon = True
    restart_timer.start()
    
    restart_timers[item.id] = restart_timer

def restart_stream(item: StreamItem):
    if system_state == 'running' and server_states.get(item.id) == 'restarting':
        log(f"▶ Attempting restart {item.name} (ID: {item.id})")
        start_ffmpeg(item)

def stop_ffmpeg(stream_id: str, skip_report: bool = False):
    if stream_id in active_streams:
        proc = active_streams[stream_id]
        try:
            proc.terminate()  # SIGTERM first
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()  # SIGKILL if not terminated
                proc.wait()
            
            if not skip_report:
                state = server_states.get(stream_id)
                if state == 'running':
                    item = api_items.get(stream_id)
                    if item:
                        uptime = "Unknown"
                        if stream_id in stream_start_times:
                            uptime_ms = (time.time() - stream_start_times[stream_id]) * 1000
                            uptime = format_uptime(uptime_ms)
                        log(f"⏹️ Stopped {item.name} (ID: {stream_id}) - was running for {uptime}")
                        
        except Exception as e:
            log(f"❌ Error stopping {stream_id}: {e}")
        
        # Clean up
        if stream_id in active_streams:
            del active_streams[stream_id]
        if stream_id in stream_start_times:
            del stream_start_times[stream_id]

# ================= ROTATION SYSTEM =================

def start_rotation_timer(item: StreamItem):
    # Clear existing rotation timer
    if item.id in stream_rotation_timers:
        stream_rotation_timers[item.id].cancel()
    
    log(f"⏰ Rotation timer started for {item.name} (ID: {item.id}) - 3:45 hours")
    
    rotation_timer = threading.Timer(
        CONFIG["rotationInterval"] / 1000,
        lambda: rotate_stream_key(item)
    )
    rotation_timer.daemon = True
    rotation_timer.start()
    
    stream_rotation_timers[item.id] = rotation_timer

def rotate_stream_key(item: StreamItem):
    try:
        log(f"🔄 Starting key rotation for {item.name} (ID: {item.id})")
        server_states[item.id] = 'rotating'
        
        # Stop current stream gracefully
        stop_ffmpeg(item.id, skip_report=True)
        
        # Remove old cache
        if item.id in stream_cache:
            del stream_cache[item.id]
        save_cache()
        
        # Create new live stream
        log(f"🌐 Creating new live stream for {item.name}")
        live_id = create_live(item.token, item.name)
        preview = get_stream_and_dash(live_id, item.token)
        
        # Update cache with new stream (keeping SAME ID)
        stream_cache[item.id] = preview
        save_cache()
        
        # Send rotation report
        tg(f"🔄 <b>STREAM KEY ROTATED</b>\n\n"
           f"<b>{item.name}</b>\n"
           f"ID: {item.id}\n"
           f"Old key: Removed\n"
           f"New key: Generated\n"
           f"DASH URL: <code>{preview.dash}</code>\n"
           f"Status: Will start in 30 seconds")
        
        # Start with new key after 30 seconds (NEW server delay)
        log(f"⏰ {item.name} (ID: {item.id}) will start with new key in 30 seconds")
        server_states[item.id] = 'starting'
        
        threading.Timer(
            CONFIG["newServerDelay"] / 1000,
            lambda: start_ffmpeg_after_rotation(item)
        ).start()
        
    except Exception as e:
        log(f"❌ Rotation failed for {item.name} (ID: {item.id}): {e}")
        server_states[item.id] = 'failed'
        
        # Try again in 5 minutes
        threading.Timer(
            300,
            lambda: rotate_stream_key(item) if system_state == 'running' else None
        ).start()

def start_ffmpeg_after_rotation(item: StreamItem):
    if system_state == 'running' and server_states.get(item.id) == 'starting':
        start_ffmpeg(item)

# ================= UPTIME CALCULATION =================

def format_uptime(uptime_ms: float) -> str:
    if uptime_ms <= 0:
        return "Not active"
    
    seconds = int((uptime_ms / 1000) % 60)
    minutes = int((uptime_ms / (1000 * 60)) % 60)
    hours = int((uptime_ms / (1000 * 60 * 60)) % 24)
    days = int(uptime_ms / (1000 * 60 * 60 * 24))
    
    parts = []
    if days > 0:
        parts.append(f"{days}d")
    if hours > 0:
        parts.append(f"{hours}h")
    if minutes > 0:
        parts.append(f"{minutes}m")
    if seconds > 0 or not parts:
        parts.append(f"{seconds}s")
    
    return " ".join(parts)

# ================= SERVER INFO =================

def get_server_info() -> Dict[str, Any]:
    import platform
    import psutil
    
    server_info = {
        "hostname": platform.node(),
        "platform": platform.platform(),
        "arch": platform.machine(),
        "pythonVersion": platform.python_version(),
        "uptime": format_uptime(time.time() - psutil.boot_time() * 1000),
        "memory": {
            "used": f"{psutil.virtual_memory().used // (1024 * 1024)}MB",
            "total": f"{psutil.virtual_memory().total // (1024 * 1024)}MB"
        },
        "streams": {
            "active": len(active_streams),
            "total": len(api_items),
            "cached": len(stream_cache)
        },
        "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "initialDelay": f"{CONFIG['initialDelay']/1000} seconds",
        "newServerDelay": f"{CONFIG['newServerDelay']/1000} seconds",
        "crashedServerDelay": f"{CONFIG['crashedServerDelay']/1000} seconds (2 minutes)",
        "rotationInterval": f"{CONFIG['rotationInterval']/(1000*60*60)} hours"
    }
    
    return server_info

# ================= INFO REPORT =================

def generate_info_report() -> str:
    server_info = get_server_info()
    now = datetime.now()
    
    report = f"📊 <b>SYSTEM STATUS REPORT</b>\n"
    report += f"⏰ <i>{now.strftime('%Y-%m-%d %H:%M:%S')}</i>\n\n"
    
    report += f"🖥️ <b>Server Info:</b>\n"
    report += f"• Host: {server_info['hostname']}\n"
    report += f"• Platform: {server_info['platform']}\n"
    report += f"• Python: {server_info['pythonVersion']}\n"
    report += f"• Server Uptime: {server_info['uptime']}\n"
    report += f"• Memory: {server_info['memory']['used']} / {server_info['memory']['total']}\n"
    report += f"• Initial Delay: {server_info['initialDelay']}\n"
    report += f"• New Server Delay: {server_info['newServerDelay']}\n"
    report += f"• Crashed Server Delay: {server_info['crashedServerDelay']}\n"
    report += f"• Rotation: {server_info['rotationInterval']}\n\n"
    
    report += f"📡 <b>Stream Stats:</b>\n"
    report += f"• Active: {server_info['streams']['active']}\n"
    report += f"• Total: {server_info['streams']['total']}\n"
    report += f"• Cached: {server_info['streams']['cached']}\n\n"
    
    report += f"🎬 <b>Stream Status:</b>\n"
    
    stream_count = 0
    for stream_id, cache in list(stream_cache.items())[:5]:
        item = api_items.get(stream_id)
        start_time = stream_start_times.get(stream_id)
        state = server_states.get(stream_id, 'unknown')
        is_active = stream_id in active_streams
        
        if item:
            report += f"\n<b>{item.name}</b>\n"
            report += f"• ID: {stream_id}\n"
            report += f"• Status: {state}\n"
            report += f"• Active: {'🟢' if is_active else '🔴'}\n"
            
            if start_time:
                uptime_ms = (time.time() - start_time) * 1000
                report += f"• Uptime: {format_uptime(uptime_ms)}\n"
            else:
                report += f"• Uptime: Not started\n"
            
            report += f"• DASH: <code>{cache.dash}</code>\n"
            
            stream_count += 1
    
    if stream_count == 0:
        report += f"\nNo streams configured.\n"
    elif len(stream_cache) > 5:
        report += f"\n... and {len(stream_cache) - 5} more streams"
    
    report += f"\n\n🔄 <i>Last checked: {now.strftime('%H:%M:%S')}</i>"
    
    return report

# ================= API WATCHER =================

def fetch_api_list() -> Dict[str, StreamItem]:
    try:
        log(f"🌐 Fetching API list from {CONFIG['streamsApi']}")
        response = requests.get(CONFIG["streamsApi"], timeout=30)
        data = response.json()
        
        items = {}
        for s in data.get("data", []):
            # Clean and trim the data
            clean_name = s.get("name", "Unnamed Stream").strip()
            clean_source = s.get("source", "").strip()
            clean_token = s.get("token", "").strip()
            
            # Generate stable ID
            stream_id = generate_stream_id(clean_name, clean_source)
            
            items[stream_id] = StreamItem(
                id=stream_id,
                name=clean_name,
                token=clean_token,
                source=clean_source
            )
        
        log(f"✅ Fetched {len(items)} items from API")
        return items
        
    except Exception as e:
        log(f"❌ Error fetching API list: {e}")
        return api_items  # Return current list on error

def watcher():
    try:
        new_list = fetch_api_list()
        
        # NEW ITEMS
        for stream_id, item in new_list.items():
            if stream_id not in api_items:
                log(f"➕ NEW SERVER DETECTED: {item.name} (ID: {stream_id})")
                try:
                    live_id = create_live(item.token, item.name)
                    preview = get_stream_and_dash(live_id, item.token)
                    stream_cache[stream_id] = preview
                    save_cache()
                    
                    # Wait 30 SECONDS before starting NEW servers
                    log(f"⏰ New server {item.name} (ID: {stream_id}) will start in 30 seconds")
                    server_states[stream_id] = 'starting'
                    
                    threading.Timer(
                        CONFIG["newServerDelay"] / 1000,
                        lambda sid=stream_id, it=item: start_new_server(sid, it)
                    ).start()
                    
                except Exception as e:
                    log(f"❌ Error creating live for {item.name} (ID: {stream_id}): {e}")
        
        # REMOVED ITEMS
        for stream_id, old_item in list(api_items.items()):
            if stream_id not in new_list:
                log(f"❌ REMOVED ITEM: {old_item.name} (ID: {stream_id})")
                
                # Clear all timers
                if stream_id in restart_timers:
                    restart_timers[stream_id].cancel()
                    del restart_timers[stream_id]
                
                if stream_id in stream_rotation_timers:
                    stream_rotation_timers[stream_id].cancel()
                    del stream_rotation_timers[stream_id]
                
                stop_ffmpeg(stream_id, skip_report=True)
                
                if stream_id in stream_cache:
                    del stream_cache[stream_id]
                if stream_id in stream_start_times:
                    del stream_start_times[stream_id]
                if stream_id in server_states:
                    del server_states[stream_id]
                
                save_cache()
        
        # Update global api_items
        api_items.clear()
        api_items.update(new_list)
        
    except Exception as e:
        log(f"❌ Watcher error: {e}")

def start_new_server(stream_id: str, item: StreamItem):
    if system_state == "running" and server_states.get(stream_id) == 'starting':
        log(f"▶ Starting NEW server: {item.name} (ID: {stream_id})")
        start_ffmpeg(item)

def start_watcher_loop():
    """Run watcher in a loop"""
    while system_state == "running":
        try:
            watcher()
        except Exception as e:
            log(f"❌ Watcher loop error: {e}")
        
        # Wait for next poll
        time.sleep(CONFIG["pollInterval"] / 1000)

# ================= TELEGRAM BOT COMMANDS =================

last_command_time = {}
command_lock = threading.Lock()

def telegram_bot_polling():
    offset = 0
    
    while system_state == "running":
        try:
            url = f"https://api.telegram.org/bot{CONFIG['telegram']['botToken']}/getUpdates"
            params = {"offset": offset, "timeout": 30}
            response = requests.get(url, params=params, timeout=35)
            data = response.json()
            
            if data.get("ok") and data.get("result"):
                for update in data["result"]:
                    offset = update["update_id"] + 1
                    handle_telegram_command(update)
                    
        except Exception as e:
            log(f"❌ Telegram polling error: {e}")
            time.sleep(5)
        
        time.sleep(1)

def handle_telegram_command(update: Dict):
    try:
        message = update.get("message")
        if not message or "text" not in message:
            return
        
        chat_id = message["chat"]["id"]
        user_id = message["from"]["id"]
        command = message["text"].strip()
        now = time.time()
        
        # Rate limiting: 1 command per 5 seconds per user
        with command_lock:
            if user_id in last_command_time:
                last_time = last_command_time[user_id]
                if now - last_time < 5:
                    tg("⏳ Please wait 5 seconds between commands.", chat_id)
                    return
            
            last_command_time[user_id] = now
            
            # Clean old entries
            if len(last_command_time) > 100:
                # Keep only recent 80 entries
                sorted_items = sorted(last_command_time.items(), key=lambda x: x[1])
                for uid, _ in sorted_items[:-80]:
                    del last_command_time[uid]
        
        # Handle /info command
        if command.startswith('/info'):
            report = generate_info_report()
            tg(report, chat_id)
            return
        
        # Handle /status command
        if command.startswith('/status'):
            status = (f"📊 <b>Stream Manager Status</b>\n\n"
                     f"🟢 Active Streams: {len(active_streams)}\n"
                     f"📋 Total Items: {len(api_items)}\n"
                     f"⏰ Server Uptime: {format_uptime(time.time() - psutil.boot_time())}\n"
                     f"🆕 New Server Delay: {CONFIG['newServerDelay']/1000}s\n"
                     f"🔧 Crashed Server Delay: {CONFIG['crashedServerDelay']/1000}s\n"
                     f"⏳ Rotation: {CONFIG['rotationInterval']/(1000*60*60)}h\n"
                     f"🕒 Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
                     f"Use /info for detailed report")
            tg(status, chat_id)
            return
        
        # Handle /help command
        if command.startswith('/help'):
            help_text = (f"🤖 <b>Stream Manager Bot Commands</b>\n\n"
                        f"/info - Get detailed system and stream report\n"
                        f"/status - Quick status check\n"
                        f"/help - Show this help message\n\n"
                        f"<i>Auto-monitoring {CONFIG['pollInterval']/1000}s intervals</i>\n"
                        f"<i>New server delay: {CONFIG['newServerDelay']/1000}s</i>\n"
                        f"<i>Crashed server delay: {CONFIG['crashedServerDelay']/1000}s</i>\n"
                        f"<i>Rotation interval: {CONFIG['rotationInterval']/(1000*60*60)}h</i>")
            tg(help_text, chat_id)
            
    except Exception as e:
        log(f"❌ Command handler error: {e}")

# ================= FINAL CHECK =================

def final_check_report():
    if not active_streams:
        tg("⚠️ <b>No active streams detected</b>\nSystem is running but no streams are active.")
        return
    
    lines = []
    for stream_id, cache in stream_cache.items():
        item = api_items.get(stream_id)
        start_time = stream_start_times.get(stream_id)
        state = server_states.get(stream_id, 'unknown')
        
        uptime = "Not started"
        if start_time:
            uptime_ms = (time.time() - start_time) * 1000
            uptime = format_uptime(uptime_ms)
        
        lines.append(
            f"<b>{item.name if item else stream_id}</b>\n"
            f"ID: {stream_id}\n"
            f"Status: {state}\n"
            f"DASH: <code>{cache.dash}</code>\n"
            f"Uptime: {uptime}"
        )
    
    tg(f"📡 <b>DASH REPORT</b>\n\n" + "\n\n".join(lines))

# ================= BOOT =================

def boot():
    log("🚀 Booting Stream Manager...")
    
    try:
        load_cache()
        global api_items
        api_items = fetch_api_list()
        
        log(f"📋 Loaded {len(api_items)} items from API")
        
        # Send startup notification
        delay_seconds = CONFIG["initialDelay"] / 1000
        tg(f"🚀 <b>Stream Manager Started</b>\n\n"
           f"Total items: {len(api_items)}\n"
           f"Cached streams: {len(stream_cache)}\n"
           f"⏳ All streams will start in {delay_seconds} seconds\n"
           f"🆕 New server delay: {CONFIG['newServerDelay']/1000}s\n"
           f"🔧 Crashed server delay: {CONFIG['crashedServerDelay']/1000}s\n"
           f"🔄 Auto-rotation: {CONFIG['rotationInterval']/(1000*60*60)} hours\n"
           f"Bot commands: /info /status /help")
        
        # Create Facebook Live for any missing items
        for item in api_items.values():
            if item.id not in stream_cache:
                log(f"🆕 Creating new live for {item.name} (ID: {item.id})")
                try:
                    live_id = create_live(item.token, item.name)
                    preview = get_stream_and_dash(live_id, item.token)
                    stream_cache[item.id] = preview
                    save_cache()
                except Exception as e:
                    log(f"❌ Failed to create live for {item.name} (ID: {item.id}): {e}")
        
        # Wait 1:50 minutes before starting ALL servers
        log(f"⏳ Waiting {delay_seconds} seconds before starting all servers...")
        
        global startup_timer
        startup_timer = threading.Timer(
            delay_seconds,
            start_all_servers
        )
        startup_timer.daemon = True
        startup_timer.start()
        
        # Start Telegram bot polling
        threading.Thread(target=telegram_bot_polling, daemon=True).start()
        log("🤖 Telegram bot polling started")
        
        # Start watcher loop
        threading.Thread(target=start_watcher_loop, daemon=True).start()
        log(f"🔍 Watcher started with {CONFIG['pollInterval']/1000}s intervals")
        
        # Send final report in 5 minutes
        threading.Timer(300, final_check_report).start()
        log("📊 Final report scheduled in 5 minutes")
        
    except Exception as e:
        log(f"❌ Boot failed: {e}")
        tg(f"❌ <b>Stream Manager Boot Failed</b>\n{e}")
        sys.exit(1)

def start_all_servers():
    log("▶ Starting ALL servers after initial delay")
    
    # Start all servers
    for item in api_items.values():
        start_ffmpeg(item)
    
    log(f"✅ Started {len(api_items)} servers")

# ================= SHUTDOWN =================

def graceful_shutdown(signum=None, frame=None):
    global system_state
    system_state = "stopping"
    log("🛑 Shutting down gracefully...")
    
    # Clear startup timer if it exists
    global startup_timer
    if 'startup_timer' in globals() and startup_timer:
        startup_timer.cancel()
    
    tg(f"🛑 <b>Stream Manager Shutting Down</b>\n"
       f"Stopping {len(active_streams)} active streams\n"
       f"Cleaning up all timers")
    
    # Clear all timers
    for timer in list(restart_timers.values()):
        timer.cancel()
    restart_timers.clear()
    
    for timer in list(stream_rotation_timers.values()):
        timer.cancel()
    stream_rotation_timers.clear()
    
    # Stop all streams
    for stream_id in list(active_streams.keys()):
        stop_ffmpeg(stream_id, skip_report=True)
    
    log("👋 Shutdown complete")
    sys.exit(0)

# ================= MAIN =================

if __name__ == "__main__":
    # Check dependencies
    try:
        import psutil
    except ImportError:
        log("❌ Please install psutil: pip install psutil")
        sys.exit(1)
    
    # Register signal handlers
    signal.signal(signal.SIGINT, graceful_shutdown)
    signal.signal(signal.SIGTERM, graceful_shutdown)
    atexit.register(graceful_shutdown)
    
    # Start the system
    boot()
    
    # Keep main thread alive
    try:
        while system_state == "running":
            time.sleep(1)
    except KeyboardInterrupt:
        graceful_shutdown()
