import os
import sys
import requests
import json
import argparse
import subprocess
import signal
import csv
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

CACHE_FILE_PATTERN = "cache-{server}-{data_type}.json"
DEBUG_MODE = False  # Default: Debugging is off

# Legacy flag (deprecated) kept for backward compatibility
DEFAULT_MAX_CONNECTIONS = 3  # (not used directly anymore)

# Default Phase-1 throttling parameters
DEFAULT_MAX_PROBE = 1          # ffprobe processes at once (conservative)
DEFAULT_MAX_EPG = 4            # parallel EPG fetches (lightweight requests)
DEFAULT_PROBE_INTERVAL = 0.35  # seconds between launching ffprobe processes

PRINT_LOCK = threading.Lock()  # prevent interleaved lines

def debug_log(message):
    if DEBUG_MODE:
        print(f"[DEBUG] {message}")

def load_cache(server, data_type):
    debug_log(f"Attempting to load cache for {data_type} on server {server}")
    cache_file = CACHE_FILE_PATTERN.format(server=server, data_type=data_type)
    if os.path.exists(cache_file):
        file_date = datetime.fromtimestamp(os.path.getmtime(cache_file)).date()
        if file_date == datetime.today().date():
            try:
                with open(cache_file, 'r') as file:
                    return json.load(file)
            except (OSError, IOError, json.JSONDecodeError) as e:
                print(f"Error reading cache file {cache_file}: {e}", file=sys.stderr)
    return None

def save_cache(server, data_type, data):
    debug_log(f"Attempting to save cache for {data_type} on server {server}")
    cache_file = CACHE_FILE_PATTERN.format(server=server, data_type=data_type)
    try:
        with open(cache_file, 'w') as file:
            json.dump(data, file)
    except (OSError, IOError) as e:
        print(f"Error saving cache file {cache_file}: {e}", file=sys.stderr)

def download_data(server, user, password, endpoint, additional_params=None):
    debug_log(f"Downloading data from {server}, endpoint: {endpoint}")
    url = f"http://{server}/player_api.php"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36"
    }
    params = {"username": user, "password": password, "action": endpoint}
    if additional_params:
        params.update(additional_params)
    response = requests.get(url, headers=headers, params=params, timeout=20)
    if response.status_code == 200:
        debug_log(f"Response from server ({endpoint}): {response.text[:300]}")
        try:
            return response.json()
        except json.JSONDecodeError:
            print(f"Failed to parse JSON for {endpoint}", file=sys.stderr)
            return {}
    else:
        print(f"Failed to fetch {endpoint} data: {response.status_code}", file=sys.stderr)
        sys.exit(1)

def check_epg(server, user, password, stream_id):
    debug_log(f"Checking EPG for stream ID {stream_id}")
    epg_data = download_data(server, user, password, "get_simple_data_table", {"stream_id": stream_id})
    if isinstance(epg_data, dict) and epg_data.get("epg_listings"):
        return len(epg_data["epg_listings"])  # Count of EPG entries
    elif isinstance(epg_data, list):
        debug_log(f"Unexpected list response for EPG data: {len(epg_data)} entries")
        return len(epg_data)
    else:
        debug_log(f"Unexpected EPG response type: {type(epg_data)}")
        return 0

def filter_data(live_categories, live_streams, group, channel):
    filtered_streams = []
    group = group.lower() if group else None
    channel = channel.lower() if channel else None
    for stream in live_streams:
        if group:
            matching_categories = [cat for cat in live_categories if group in cat["category_name"].lower()]
            if not any(cat["category_id"] == stream["category_id"] for cat in matching_categories):
                continue
        if channel and channel not in stream["name"].lower():
            continue
        filtered_streams.append(stream)
    return filtered_streams

def check_ffprobe():
    try:
        subprocess.run(["ffprobe", "-version"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True)
        debug_log("ffprobe is installed and reachable.")
    except FileNotFoundError:
        print("Error: ffprobe is not installed or not found in the system PATH. Please install ffprobe before running this program.")
        sys.exit(1)
    except subprocess.CalledProcessError as e:
        print(f"Error: ffprobe check failed with error: {e}")
        sys.exit(1)

def run_ffprobe(stream_url):
    try:
        result = subprocess.run([
            "ffprobe", "-v", "error",
            "-select_streams", "v:0",
            "-show_entries", "stream=codec_name,width,height,avg_frame_rate",
            "-of", "json",
            stream_url
        ], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=20)
        output = json.loads(result.stdout) if result.stdout else {}
        if 'streams' in output and output['streams']:
            s = output['streams'][0]
            codec_name = s.get('codec_name', 'Unknown')[:8]
            width = s.get('width', 'Unknown')
            height = s.get('height', 'Unknown')
            afr = s.get('avg_frame_rate', 'Unknown')
            if afr != 'Unknown' and '/' in afr:
                try:
                    num, denom = map(int, afr.split('/'))
                    frame_rate = round(num / denom) if denom else 'Unknown'
                except Exception:
                    frame_rate = afr
            else:
                frame_rate = afr
            return {"status": "working", "codec_name": codec_name, "width": width, "height": height, "frame_rate": frame_rate}
        else:
            return {"status": "not working"}
    except subprocess.TimeoutExpired:
        return {"status": "timeout"}
    except Exception as e:
        debug_log(f"ffprobe error: {e}")
        return {"status": "error", "error_message": str(e)}

def save_to_csv(file_name, data, fieldnames):
    try:
        with open(file_name, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, quoting=csv.QUOTE_ALL)
            writer.writeheader()
            writer.writerows(data)
        print(f"Output saved to {file_name}")
    except Exception as e:
        print(f"Error saving to CSV: {e}")

def handle_sigint(signal_received, frame):
    print("\nProgram interrupted by user. Exiting...")
    sys.exit(0)

def print_stream_line(stream, category_name, epg_count, stream_info):
    resolution = ""
    if stream_info:
        width = stream_info.get('width', 'Unknown')
        height = stream_info.get('height', 'Unknown')
        if width != 'Unknown' and height != 'Unknown':
            resolution = f"{width}x{height}"
        else:
            resolution = "N/A"
    status = stream_info.get('status', '') if stream_info else ''
    line = f"{stream['stream_id']:<10}{stream['name'][:60]:<60} {category_name[:40]:<40}{stream.get('tv_archive_duration', 'N/A'):<8}{str(epg_count):<5}{(stream_info or {}).get('codec_name', ''):<10}{resolution:<15}{str((stream_info or {}).get('frame_rate', '')):<10}{status:<10}"
    with PRINT_LOCK:
        print(line, flush=True)

def gather_epg_counts(streams, args, category_map):
    if not args.epgcheck:
        return {}
    epg_counts = {}
    max_workers = max(1, args.max_epg)
    debug_log(f"Starting EPG fetch with max workers = {max_workers}")
    def epg_task(stream):
        epg_counts[stream['stream_id']] = check_epg(args.server, args.user, args.pw, stream['stream_id'])
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = [ex.submit(epg_task, s) for s in streams]
        for f in as_completed(futures):
            pass  # just wait; could add progress output
    return epg_counts

def probe_streams(streams, args, epg_counts, category_map, csv_data, fieldnames):
    if not args.check:
        # Just print without probing
        for stream in streams:
            epg_count = epg_counts.get(stream['stream_id'], '')
            print_stream_line(stream, category_map.get(stream['category_id'], 'Unknown'), epg_count, {})
            csv_data.append({
                "Stream ID": stream['stream_id'],
                "Name": stream['name'][:60],
                "Category": category_map.get(stream['category_id'], 'Unknown')[:40],
                "Archive": stream.get('tv_archive_duration', 'N/A'),
                "EPG": epg_count,
                "Codec": '',
                "Resolution": '',
                "Frame Rate": ''
            })
        return

    # ffprobe path
    if args.max_probe < 1:
        args.max_probe = 1
    max_workers = args.max_probe
    interval = max(0.0, args.probe_interval)
    debug_log(f"Starting ffprobe with max_probe={max_workers}, interval={interval}s")

    # Use executor with controlled launch pacing
    executor = ThreadPoolExecutor(max_workers=max_workers)
    inflight = {}
    last_launch_time = 0.0

    def launch(stream):
        nonlocal last_launch_time
        # Rate pacing
        now = time.time()
        delay = interval - (now - last_launch_time)
        if delay > 0:
            time.sleep(delay)
        stream_url = f"http://{args.server}/{args.user}/{args.pw}/{stream['stream_id']}"
        future = executor.submit(run_ffprobe, stream_url)
        inflight[future] = stream
        last_launch_time = time.time()

    streams_iter = iter(streams)
    # Pre-fill up to max_workers
    try:
        for _ in range(max_workers):
            s = next(streams_iter)
            launch(s)
    except StopIteration:
        pass

    while inflight:
        # Wait for any future to complete
        done, _ = wait_for_any(inflight)
        for fut in done:
            stream = inflight.pop(fut)
            try:
                res = fut.result()
            except Exception as e:
                res = {"status": "error", "error_message": str(e)}
            epg_count = epg_counts.get(stream['stream_id'], '')
            print_stream_line(stream, category_map.get(stream['category_id'], 'Unknown'), epg_count, res)
            # CSV accumulation
            width = res.get('width', 'Unknown')
            height = res.get('height', 'Unknown')
            resolution = f"{width}x{height}" if width not in ('Unknown', '') and height not in ('Unknown', '') else 'N/A'
            csv_data.append({
                "Stream ID": stream['stream_id'],
                "Name": stream['name'][:60],
                "Category": category_map.get(stream['category_id'], 'Unknown')[:40],
                "Archive": stream.get('tv_archive_duration', 'N/A'),
                "EPG": epg_count,
                "Codec": res.get('codec_name', ''),
                "Resolution": resolution,
                "Frame Rate": res.get('frame_rate', ''),
            })
            # Launch next stream (if any)
            try:
                s = next(streams_iter)
                launch(s)
            except StopIteration:
                pass

    executor.shutdown(wait=True)

def wait_for_any(future_dict):
    # Wait for at least one future to complete
    # Using a polling approach with small sleep to avoid busy wait
    while True:
        done = [f for f in future_dict if f.done()]
        if done:
            return done, [f for f in future_dict if not f.done()]
        time.sleep(0.05)

def main():
    global DEBUG_MODE
    signal.signal(signal.SIGINT, handle_sigint)

    parser = argparse.ArgumentParser(description="Xtream IPTV Downloader and Filter (Throttled)")
    parser.add_argument("--server", required=True, help="The Xtream server to connect to.")
    parser.add_argument("--user", required=True, help="The username to use.")
    parser.add_argument("--pw", required=True, help="The password to use.")
    parser.add_argument("--nocache", action="store_true", help="Force download and ignore cache.")
    parser.add_argument("--channel", help="Filter by channel name.")
    parser.add_argument("--category", help="Filter by category name.")
    parser.add_argument("--debug", action="store_true", help="Enable debug mode.")
    parser.add_argument("--epgcheck", action="store_true", help="Check if channels provide EPG data and count entries.")
    parser.add_argument("--check", action="store_true", help="Check stream resolution and frame rate using ffprobe.")
    parser.add_argument("--save", help="Save the output to a CSV file. Provide the file name.")
    # New throttling flags
    parser.add_argument("--max-probe", type=int, default=DEFAULT_MAX_PROBE, help="Maximum concurrent ffprobe processes (default 1).")
    parser.add_argument("--max-epg", type=int, default=DEFAULT_MAX_EPG, help="Maximum concurrent EPG fetch operations (default 4).")
    parser.add_argument("--probe-interval", type=float, default=DEFAULT_PROBE_INTERVAL, help="Seconds to wait between launching ffprobe processes (default 0.35).")
    # Deprecated legacy flag (ignored except for backward mapping)
    parser.add_argument("--max-connections", type=int, help=argparse.SUPPRESS)
    args = parser.parse_args()

    # Backward compatibility mapping
    if args.max_connections is not None:
        if args.max_probe == DEFAULT_MAX_PROBE and args.max_connections != DEFAULT_MAX_CONNECTIONS:
            args.max_probe = max(1, args.max_connections)
            print(f"[INFO] --max-connections is deprecated. Using its value ({args.max_connections}) as --max-probe.")
        else:
            print("[INFO] --max-connections is deprecated. Use --max-probe, --max-epg, and --probe-interval.")

    masked_server = f"{'.'.join(['xxxxx'] + args.server.split('.')[1:])}" if '.' in args.server else args.server
    run_time = datetime.now().strftime("%Y-%m-%d %H:%M")
    print(f"\n\nfind-iptv-channels-details - Running for server {masked_server} on {run_time}\n")

    DEBUG_MODE = args.debug
    debug_log("Debug mode enabled")

    if args.check:
        check_ffprobe()

    live_categories = load_cache(args.server, "live_categories") if not args.nocache else None
    live_streams = load_cache(args.server, "live_streams") if not args.nocache else None

    if not live_categories or not live_streams:
        live_categories = download_data(args.server, args.user, args.pw, "get_live_categories")
        live_streams = download_data(args.server, args.user, args.pw, "get_live_streams")
        save_cache(args.server, "live_categories", live_categories)
        save_cache(args.server, "live_streams", live_streams)

    filtered_streams = filter_data(live_categories, live_streams, args.category, args.channel)

    fieldnames = ["Stream ID", "Name", "Category", "Archive", "EPG", "Codec", "Resolution", "Frame Rate"]
    csv_data = []

    print(f"{'ID':<10}{'Name':<60} {'Category':<40}{'Archive':<8}{'EPG':<5}{'Codec':<10}{'Resolution':<15}{'Frame':<10}{'Status':<10}")
    print("=" * 168, flush=True)

    category_map = {cat["category_id"]: cat["category_name"] for cat in live_categories}

    # Phase 1: Fetch EPG counts (parallel and relatively cheap)
    epg_counts = gather_epg_counts(filtered_streams, args, category_map)

    # Phase 1: Probe streams with paced ffprobe
    probe_streams(filtered_streams, args, epg_counts, category_map, csv_data, fieldnames)

    print("\n")
    if args.save:
        save_to_csv(args.save, csv_data, fieldnames)
    print("\n\n")

if __name__ == "__main__":
    main()