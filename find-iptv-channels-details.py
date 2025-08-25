#!/usr/bin/env python3
import os
import sys
import json
import csv
import time
import random
import signal
import argparse
import subprocess
import threading
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

# =========================
# Configuration & Globals
# =========================
CACHE_FILE_PATTERN = "cache-{server}-{data_type}.json"
DEBUG_MODE = False
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36"

# Locks for clean console output and shared state
print_lock = threading.Lock()

def debug_log(message: str):
    if DEBUG_MODE:
        with print_lock:
            print(f"[DEBUG] {message}")

# =========================
# Cache Utilities
# =========================
def load_cache(server, data_type):
    cache_file = CACHE_FILE_PATTERN.format(server=server, data_type=data_type)
    if os.path.exists(cache_file):
        file_date = datetime.fromtimestamp(os.path.getmtime(cache_file)).date()
        if file_date == datetime.today().date():
            try:
                with open(cache_file, 'r', encoding="utf-8") as f:
                    data = json.load(f)
                debug_log(f"Loaded cache {cache_file}")
                return data
            except (OSError, IOError, json.JSONDecodeError) as e:
                print(f"Error reading cache file {cache_file}: {e}", file=sys.stderr)
    return None

def save_cache(server, data_type, data):
    cache_file = CACHE_FILE_PATTERN.format(server=server, data_type=data_type)
    try:
        with open(cache_file, 'w', encoding="utf-8") as f:
            json.dump(data, f)
        debug_log(f"Saved cache {cache_file}")
    except (OSError, IOError) as e:
        print(f"Error saving cache file {cache_file}: {e}", file=sys.stderr)

# =========================
# Provider API
# =========================
def download_data(server, user, password, endpoint, additional_params=None):
    url = f"http://{server}/player_api.php"
    headers = {"User-Agent": USER_AGENT}
    params = {"username": user, "password": password, "action": endpoint}
    if additional_params:
        params.update(additional_params)

    resp = requests.get(url, headers=headers, params=params, timeout=15)
    if resp.status_code == 200:
        try:
            return resp.json()
        except json.JSONDecodeError:
            debug_log(f"Non-JSON response for {endpoint}: {resp.text[:300]}")
            return None
    else:
        raise RuntimeError(f"Failed to fetch {endpoint}: HTTP {resp.status_code}")

def check_epg(server, user, password, stream_id):
    try:
        epg = download_data(server, user, password, "get_simple_data_table", {"stream_id": stream_id})
        if isinstance(epg, dict) and epg.get("epg_listings"):
            return len(epg["epg_listings"])
        elif isinstance(epg, list):
            return len(epg)
        else:
            return 0
    except Exception as e:
        debug_log(f"EPG fetch error for stream {stream_id}: {e}")
        return 0

# =========================
# ffprobe Utilities
# =========================
def check_ffprobe_available():
    try:
        subprocess.run(
            ["ffprobe", "-version"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=True,
        )
        debug_log("ffprobe is installed.")
        return True
    except FileNotFoundError:
        print("Error: ffprobe not found in PATH. Install ffmpeg/ffprobe.", file=sys.stderr)
        return False
    except subprocess.CalledProcessError as e:
        print(f"Error: ffprobe check failed: {e}", file=sys.stderr)
        return False

def parse_frame_rate(avg_frame_rate):
    if not avg_frame_rate or avg_frame_rate == "N/A":
        return "N/A"
    if isinstance(avg_frame_rate, (int, float)):
        return round(avg_frame_rate)
    if "/" in avg_frame_rate:
        try:
            num, denom = avg_frame_rate.split("/")
            num = float(num)
            denom = float(denom)
            if denom == 0:
                return "N/A"
            return round(num / denom)
        except Exception:
            return "N/A"
    try:
        return round(float(avg_frame_rate))
    except Exception:
        return "N/A"

def human_kbps(bit_rate_value):
    if not bit_rate_value or bit_rate_value == "N/A":
        return "N/A"
    try:
        br = int(float(bit_rate_value))
        if br <= 0:
            return "N/A"
        return str(int(round(br / 1000.0)))  # bps -> kbps
    except Exception:
        return "N/A"

def ffprobe_channel(url, timeout_sec, rw_timeout_ms, analyze_ms, probesize_bytes, extra_http_connect=False):
    args = [
        "ffprobe",
        "-v", "error",
        "-select_streams", "v:0",
        "-show_entries", "stream=codec_name,width,height,avg_frame_rate,bit_rate:format=bit_rate",
        "-of", "json",
        "-analyzeduration", str(int(analyze_ms * 1000)),    # microseconds
        "-probesize", str(int(probesize_bytes)),            # bytes
        "-rw_timeout", str(int(rw_timeout_ms * 1000)),      # microseconds
        "-fflags", "nobuffer",
        "-user_agent", USER_AGENT,
    ]
    if extra_http_connect:
        args.extend(["-reconnect", "1", "-reconnect_streamed", "1", "-reconnect_delay_max", "2"])
    args.append(url)

    try:
        proc = subprocess.run(
            args,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=timeout_sec
        )
        out = proc.stdout.strip()
        if not out:
            return {"status": "no_data"}

        data = json.loads(out)
        streams = data.get("streams") or []
        fmt = data.get("format") or {}
        if not streams:
            return {"status": "no_stream"}

        s0 = streams[0]
        codec = s0.get("codec_name") or "Unknown"
        width = s0.get("width") or "N/A"
        height = s0.get("height") or "N/A"
        fps = parse_frame_rate(s0.get("avg_frame_rate") or "N/A")

        br_stream = human_kbps(s0.get("bit_rate"))
        br_format = human_kbps(fmt.get("bit_rate"))
        bitrate_kbps = br_stream if br_stream != "N/A" else br_format

        return {
            "status": "ok",
            "codec_name": codec,
            "width": width,
            "height": height,
            "frame_rate": fps,
            "bitrate_kbps": bitrate_kbps
        }
    except subprocess.TimeoutExpired:
        return {"status": "timeout"}
    except Exception as e:
        debug_log(f"ffprobe error: {e}")
        return {"status": "error"}

# =========================
# Bitrate fallback (active measurement)
# =========================
def measure_bitrate_active(url, sample_sec=2.5, max_bytes=2_000_000, connect_timeout=3, read_timeout=3, chunk_size=32 * 1024):
    """
    Opens the stream and reads for up to sample_sec or max_bytes, whichever occurs first,
    and computes bitrate in kbps. This counts as a streaming connection, so call only
    under the stream slot/semaphore.
    """
    headers = {"User-Agent": USER_AGENT}
    start = time.monotonic()
    bytes_read = 0
    try:
        with requests.get(url, headers=headers, stream=True, timeout=(connect_timeout, read_timeout)) as resp:
            resp.raise_for_status()
            for chunk in resp.iter_content(chunk_size=chunk_size):
                if not chunk:
                    continue
                bytes_read += len(chunk)
                now = time.monotonic()
                if (now - start) >= sample_sec or bytes_read >= max_bytes:
                    break
        elapsed = max(0.001, time.monotonic() - start)
        kbps = int(round((bytes_read * 8) / 1000.0 / elapsed))
        return str(kbps) if kbps > 0 else "N/A"
    except Exception as e:
        debug_log(f"bitrate fallback error: {e}")
        return "N/A"

# =========================
# Filtering
# =========================
def filter_streams(live_categories, live_streams, group, channel):
    filtered = []
    group_l = group.lower() if group else None
    channel_l = channel.lower() if channel else None

    allowed_cat_ids = None
    if group_l:
        allowed_cat_ids = {
            c["category_id"] for c in live_categories
            if group_l in (c.get("category_name") or "").lower()
        }

    for s in live_streams:
        if allowed_cat_ids is not None and s.get("category_id") not in allowed_cat_ids:
            continue
        if channel_l and channel_l not in (s.get("name") or "").lower():
            continue
        filtered.append(s)
    return filtered

# =========================
# Concurrency Management
# =========================
class StreamSlotManager:
    def __init__(self, max_slots: int, grace_hold: float):
        self.sem = threading.Semaphore(max_slots)
        self.grace_hold = max(0.0, float(grace_hold))

    def acquire(self):
        self.sem.acquire()

    def release(self):
        if self.grace_hold > 0:
            time.sleep(self.grace_hold)
        self.sem.release()

# =========================
# Color utilities
# =========================
ANSI_RESET = "\033[0m"
ANSI_RED = "\033[31m"            # dark red
ANSI_YELLOW = "\033[33m"         # used for "orange" fallback and 49 fps
ANSI_GREEN = "\033[32m"
ANSI_BRIGHT_GREEN = "\033[92m"   # "neon green"
ANSI_LIGHT_BLUE = "\033[94m"
ANSI_ORANGE_256 = "\033[38;5;208m"  # true orange in 256-color terminals

def colorize(s: str, code: str, enabled: bool):
    if not enabled or not code:
        return s
    return f"{code}{s}{ANSI_RESET}"

def pad_then_color(s: str, width: int, code: str, enabled: bool, align_left=True):
    if align_left:
        plain = s.ljust(width)
    else:
        plain = s.rjust(width)
    return colorize(plain, code, enabled)

def to_int_or_none(v):
    try:
        if v is None:
            return None
        if isinstance(v, (int, float)):
            return int(v)
        sv = str(v).strip()
        if sv.isdigit():
            return int(sv)
        return int(float(sv))
    except Exception:
        return None

# =========================
# CSV
# =========================
def save_to_csv(file_name, data, fieldnames):
    try:
        with open(file_name, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, quoting=csv.QUOTE_ALL)
            writer.writeheader()
            writer.writerows(data)
        print(f"Output saved to {file_name}")
    except Exception as e:
        print(f"Error saving to CSV: {e}", file=sys.stderr)

# =========================
# Worker
# =========================
def analyze_stream(
    stream,
    category_map,
    args,
    slot_mgr: StreamSlotManager,
    index: int,
    total: int,
    server: str,
    user: str,
    pw: str,
):
    stream_id = stream["stream_id"]
    name = (stream.get("name") or "")[:60]
    category_name = (category_map.get(stream.get("category_id")) or "Unknown")[:40]

    # EPG (API) – not a counted stream connection
    epg_count = ""
    if args.epgcheck:
        time.sleep(random.uniform(0.05, 0.2))
        epg_count = check_epg(server, user, pw, stream_id)

    codec = ""
    width = ""
    height = ""
    fps = ""
    bitrate_kbps = ""
    status = "ok"
    connected_via_fallback = False

    if args.check:
        time.sleep(random.uniform(0.3, 1.2))  # jitter before opening stream
        slot_mgr.acquire()
        try:
            url = f"http://{server}/{user}/{pw}/{stream_id}"
            info = ffprobe_channel(
                url=url,
                timeout_sec=args.ffprobe_timeout,
                rw_timeout_ms=args.ffprobe_rw_timeout_ms,
                analyze_ms=args.ffprobe_analyze_ms,
                probesize_bytes=args.ffprobe_probesize,
                extra_http_connect=args.ffprobe_reconnect
            )
            status = info.get("status", "error")
            if status == "ok":
                codec = (info.get("codec_name") or "")[:8]
                width = info.get("width") or "N/A"
                height = info.get("height") or "N/A"
                fps = info.get("frame_rate") or "N/A"
                bitrate_kbps = info.get("bitrate_kbps") or "N/A"
            else:
                codec = status
                width = "N/A"
                height = "N/A"
                fps = "N/A"
                bitrate_kbps = "N/A"

            # Bitrate fallback if missing; if we get data here, we consider the stream reachable
            if args.bitrate_fallback and (not bitrate_kbps or bitrate_kbps == "N/A"):
                if args.bitrate_fallback_gap > 0:
                    time.sleep(args.bitrate_fallback_gap)
                bitrate_kbps_fb = measure_bitrate_active(
                    url=url,
                    sample_sec=args.bitrate_fallback_sample_sec,
                    max_bytes=args.bitrate_fallback_bytes
                )
                if bitrate_kbps_fb and bitrate_kbps_fb != "N/A":
                    bitrate_kbps = bitrate_kbps_fb
                    connected_via_fallback = True
        finally:
            slot_mgr.release()

    resolution = f"{width}x{height}" if args.check else ""
    bitrate_str = f"{bitrate_kbps} kbps" if bitrate_kbps and bitrate_kbps != "N/A" else "N/A"

    # Determine offline (not detected / can't find connection)
    offline_statuses = {"timeout", "error", "no_data", "no_stream", "not working"}
    is_offline = args.check and (status in offline_statuses) and (not connected_via_fallback)

    # Build display with colors
    with print_lock:
        progress = f"[{index}/{total}] "
        id_col = f"{str(stream_id):<8}"
        name_col = f"{name:<60}"
        cat_col = f"{category_name:<40}"
        arch_col = f"{str(stream.get('tv_archive_duration', 'N/A')):<8}"
        epg_col = f"{str(epg_count):<5}"
        codec_col = f"{str(codec):<8}"

        # Prepare resolution colorization
        res_display = ""
        fps_display = ""
        bitrate_display = f"{bitrate_str:<12}"

        if args.check:
            # Resolution color rules based on height
            height_num = to_int_or_none(height)
            res_plain = resolution  # e.g., "1920x1080"
            if is_offline or height_num is None:
                res_display = f"{res_plain:<15}"
            else:
                if height_num <= 540:
                    res_color = ANSI_RED
                elif height_num <= 720:
                    res_color = ANSI_ORANGE_256 if args.color_enabled else ANSI_YELLOW
                elif height_num <= 1080:
                    res_color = ANSI_GREEN
                else:
                    res_color = ANSI_LIGHT_BLUE
                res_display = pad_then_color(res_plain, 15, res_color, args.color_enabled)

            # FPS color rules
            fps_num = to_int_or_none(fps)
            fps_plain = str(fps)
            if is_offline or fps_num is None:
                fps_display = f"{fps_plain:<5}"
            else:
                if fps_num == 49:
                    fps_color = ANSI_YELLOW
                elif fps_num > 49:
                    fps_color = ANSI_BRIGHT_GREEN
                else:
                    fps_color = ""  # default color for <= 49 except 49 itself
                fps_display = pad_then_color(fps_plain, 5, fps_color, args.color_enabled)
        else:
            res_display = f"{'':<15}"
            fps_display = f"{'':<5}"

        # Assemble the line
        line = (
            f"{progress}{id_col} {name_col} {cat_col} "
            f"{arch_col} {epg_col} {codec_col} {res_display} {fps_display} {bitrate_display}"
        )

        # If offline, color the entire line dark red
        if is_offline and args.color_enabled:
            line = colorize(line, ANSI_RED, True)

        print(line)

    return {
        "Stream ID": stream_id,
        "Name": name,
        "Category": category_name,
        "Archive": stream.get('tv_archive_duration', 'N/A'),
        "EPG": epg_count,
        "Codec": codec,
        "Resolution": resolution if args.check else "",
        "Frame Rate": fps if args.check else "",
        "Bitrate (kbps)": bitrate_kbps if bitrate_kbps and bitrate_kbps != "N/A" else "N/A"
    }

# =========================
# Main
# =========================
def main():
    global DEBUG_MODE

    def handle_sigint(sig, frame):
        print("\nInterrupted by user. Exiting...")
        sys.exit(0)

    signal.signal(signal.SIGINT, handle_sigint)

    parser = argparse.ArgumentParser(description="Xtream IPTV channel analyzer with connection-aware ffprobe.")
    parser.add_argument("--server", required=True, help="Xtream server host:port")
    parser.add_argument("--user", required=True, help="Username")
    parser.add_argument("--pw", required=True, help="Password")

    parser.add_argument("--nocache", action="store_true", help="Ignore cache and fetch fresh lists")
    parser.add_argument("--channel", help="Filter by channel name (substring match)")
    parser.add_argument("--category", help="Filter by category name (substring match)")

    parser.add_argument("--epgcheck", action="store_true", help="Fetch EPG counts per channel")
    parser.add_argument("--check", action="store_true", help="Probe stream for quality/fps/bitrate via ffprobe")

    parser.add_argument("--save", help="Save output to CSV file")
    parser.add_argument("--debug", action="store_true", help="Enable debug logs")

    # Connection and probe controls
    parser.add_argument("--stream-concurrency", type=int, default=2,
                        help="Max concurrent stream probes (default: 2). Set to 3 if provider is tolerant.")
    parser.add_argument("--grace-hold", type=float, default=8.0,
                        help="Seconds to hold a slot after ffprobe exit to avoid lingering session overlap (default: 8)")

    # ffprobe tuning
    parser.add_argument("--ffprobe-timeout", type=int, default=12, help="Overall ffprobe process timeout seconds (default: 12)")
    parser.add_argument("--ffprobe-rw-timeout-ms", type=int, default=3000, help="I/O timeout for ffprobe AVIO in ms (default: 3000)")
    parser.add_argument("--ffprobe-analyze-ms", type=int, default=700, help="Analyze duration in ms (default: 700)")
    parser.add_argument("--ffprobe-probesize", type=int, default=512_000, help="Probe size in bytes (default: 512000)")
    parser.add_argument("--ffprobe-reconnect", action="store_true", help="Enable ffprobe HTTP reconnect hints")

    # Worker threads
    parser.add_argument("--workers", type=int, default=4, help="Thread pool size to schedule probes (default: 4)")

    # Bitrate fallback controls
    parser.add_argument("--bitrate-fallback", dest="bitrate_fallback", action="store_true", help="Enable active bitrate fallback if ffprobe returns N/A (default ON)")
    parser.add_argument("--no-bitrate-fallback", dest="bitrate_fallback", action="store_false", help="Disable bitrate fallback")
    parser.set_defaults(bitrate_fallback=True)
    parser.add_argument("--bitrate-fallback-sample-sec", type=float, default=2.5, help="Seconds to read bytes for bitrate fallback (default: 2.5)")
    parser.add_argument("--bitrate-fallback-bytes", type=int, default=2_000_000, help="Max bytes to read during fallback (default: 2,000,000)")
    parser.add_argument("--bitrate-fallback-gap", type=float, default=1.0, help="Seconds to wait after ffprobe before fallback to avoid overlap (default: 1.0)")

    # Color controls
    parser.add_argument("--no-color", action="store_true", help="Disable ANSI colors in console output")
    parser.add_argument("--force-color", action="store_true", help="Force-enable ANSI colors even if stdout is not a TTY")

    args = parser.parse_args()
    DEBUG_MODE = args.debug

    # Determine color support
    args.color_enabled = (not args.no_color) and (args.force_color or sys.stdout.isatty())
    if args.color_enabled and os.name == "nt":
        try:
            import colorama
            colorama.just_fix_windows_console()
        except Exception:
            pass  # best-effort; recent Windows terminals support ANSI without this

    if args.check and not check_ffprobe_available():
        sys.exit(1)

    masked_server = f"{'.'.join(['xxxxx'] + args.server.split('.')[1:])}"
    run_time = datetime.now().strftime("%Y-%m-%d %H:%M")
    print(f"\nfind-iptv-channels-details - Running for server {masked_server} on {run_time}")
    print(f"Stream concurrency limit: {args.stream_concurrency} (grace-hold: {args.grace_hold}s)")
    if args.check:
        debug_log(f"ffprobe: timeout={args.ffprobe_timeout}s, rw_timeout={args.ffprobe_rw_timeout_ms}ms, "
                  f"analyze={args.ffprobe_analyze_ms}ms, probesize={args.ffprobe_probesize} bytes, reconnect={args.ffprobe_reconnect}")
        debug_log(f"bitrate fallback: enabled={args.bitrate_fallback}, sample={args.bitrate_fallback_sample_sec}s, "
                  f"max_bytes={args.bitrate_fallback_bytes}, gap={args.bitrate_fallback_gap}s")

    # Fetch live categories and streams (cache per day)
    if not args.nocache:
        live_categories = load_cache(args.server, "live_categories")
        live_streams = load_cache(args.server, "live_streams")
    else:
        live_categories, live_streams = None, None

    if not live_categories or not live_streams:
        debug_log("Fetching categories/streams from provider...")
        live_categories = download_data(args.server, args.user, args.pw, "get_live_categories") or []
        live_streams = download_data(args.server, args.user, args.pw, "get_live_streams") or []
        save_cache(args.server, "live_categories", live_categories)
        save_cache(args.server, "live_streams", live_streams)

    category_map = {c.get("category_id"): c.get("category_name", "") for c in live_categories}
    filtered = filter_streams(live_categories, live_streams, args.category, args.channel)
    total = len(filtered)

    print("")
    print(f"{'':<10}{'ID':<8} {'Name':<60} {'Category':<40} {'Arch':<8} {'EPG':<5} {'Codec':<8} {'Resolution':<15} {'FPS':<5} {'Bitrate':<12}")
    print("=" * 170)

    if total == 0:
        print("No streams match the filter.")
        return

    slot_mgr = StreamSlotManager(max_slots=max(1, args.stream_concurrency), grace_hold=args.grace_hold)

    rows = []
    futures = []
    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as pool:
        for idx, stream in enumerate(filtered, start=1):
            futures.append(
                pool.submit(
                    analyze_stream,
                    stream,
                    category_map,
                    args,
                    slot_mgr,
                    idx,
                    total,
                    args.server,
                    args.user,
                    args.pw
                )
            )

        for f in as_completed(futures):
            row = f.result()
            rows.append(row)

    if args.save:
        fieldnames = ["Stream ID", "Name", "Category", "Archive", "EPG", "Codec", "Resolution", "Frame Rate", "Bitrate (kbps)"]
        # Preserve original filtered order
        order = {s["stream_id"]: i for i, s in enumerate(filtered)}
        rows_sorted = sorted(rows, key=lambda r: order.get(r["Stream ID"], 1_000_000))
        save_to_csv(args.save, rows_sorted, fieldnames)

    print("\nDone.\n")

if __name__ == "__main__":
    main()
