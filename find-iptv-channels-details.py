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

# Default maximum simultaneous stream-related operations
DEFAULT_MAX_CONNECTIONS = 3

# Global lock for printing so lines don't interleave
PRINT_LOCK = threading.Lock()

def debug_log(message):
    """Logs a debug message if debugging is enabled."""
    if DEBUG_MODE:
        print(f"[DEBUG] {message}")

def load_cache(server, data_type):
    """Load data from the cache file if it exists and is up-to-date."""
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
    """Save data to the cache file."""
    debug_log(f"Attempting to save cache for {data_type} on server {server}")
    cache_file = CACHE_FILE_PATTERN.format(server=server, data_type=data_type)
    try:
        with open(cache_file, 'w') as file:
            json.dump(data, file)
    except (OSError, IOError) as e:
        print(f"Error saving cache file {cache_file}: {e}", file=sys.stderr)

def download_data(server, user, password, endpoint, additional_params=None):
    """Download data from the Xtream IPTV server."""
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
    """Check EPG data for a specific channel and return count."""
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
    """Filter the live streams based on group and channel arguments."""
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
    """Checks if the ffprobe command is available on the system."""
    try:
        subprocess.run(
            ["ffprobe", "-version"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=True,
        )
        debug_log("ffprobe is installed and reachable.")
    except FileNotFoundError:
        print("Error: ffprobe is not installed or not found in the system PATH. Please install ffprobe before running this program.")
        sys.exit(1)
    except subprocess.CalledProcessError as e:
        print(f"Error: ffprobe check failed with error: {e}")
        sys.exit(1)

def check_channel(url):
    """Run ffprobe on a stream URL and return basic info."""
    try:
        result = subprocess.run(
            [
                "ffprobe",
                "-v", "error",
                "-select_streams", "v:0",
                "-show_entries", "stream=codec_name,width,height,avg_frame_rate",
                "-of", "json",
                url
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=15
        )

        output = json.loads(result.stdout) if result.stdout else {}

        if 'streams' in output and len(output['streams']) > 0:
            stream_info = output['streams'][0]
            codec_name = stream_info.get('codec_name', 'Unknown')[:8]
            width = stream_info.get('width', 'Unknown')
            height = stream_info.get('height', 'Unknown')
            avg_frame_rate = stream_info.get('avg_frame_rate', 'Unknown')

            if avg_frame_rate != 'Unknown' and '/' in avg_frame_rate:
                try:
                    num, denom = map(int, avg_frame_rate.split('/'))
                    frame_rate = round(num / denom) if denom != 0 else "Unknown"
                except Exception:
                    frame_rate = avg_frame_rate
            else:
                frame_rate = avg_frame_rate

            return {
                "status": "working",
                "codec_name": codec_name,
                "width": width,
                "height": height,
                "frame_rate": frame_rate
            }
        else:
            debug_log(f"No streams found in ffprobe output for URL: {url}")
            return {"status": "not working"}
    except subprocess.TimeoutExpired:
        return {"status": "timeout"}
    except Exception as e:
        debug_log(f"Error in check_channel: {e}")
        return {"status": "error", "error_message": str(e)}

def save_to_csv(file_name, data, fieldnames):
    """Save data to a CSV file, ensuring all fields are enclosed in double quotes."""
    try:
        with open(file_name, "w", newline="", encoding="utf-8") as file:
            writer = csv.DictWriter(file, fieldnames=fieldnames, quoting=csv.QUOTE_ALL)
            writer.writeheader()
            writer.writerows(data)
        print(f"Output saved to {file_name}")
    except Exception as e:
        print(f"Error saving to CSV: {e}")

def handle_sigint(signal_received, frame):
    """Handle Ctrl+C gracefully."""
    print("\nProgram interrupted by user. Exiting...")
    sys.exit(0)

def process_stream(stream, args, category_map, semaphore):
    """Perform (optional) EPG and ffprobe checks for a single stream.

    A semaphore is used in addition to the thread pool to enforce the maximum
    number of simultaneous network/ffprobe operations. (Redundant with pool size
    but explicit for safety if future parallelism is added.)
    """
    with semaphore:
        category_name = category_map.get(stream["category_id"], "Unknown")
        epg_count = ""
        if args.epgcheck:
            epg_count = check_epg(args.server, args.user, args.pw, stream["stream_id"])

        stream_info = {"codec_name": "", "width": "", "height": "", "frame_rate": "", "status": ""}
        if args.check:
            stream_url = f"http://{args.server}/{args.user}/{args.pw}/{stream['stream_id']}"
            stream_info = check_channel(stream_url)

        resolution = ""
        if args.check:
            width = stream_info.get('width', 'N/A')
            height = stream_info.get('height', 'N/A')
            resolution = f"{width}x{height}" if width != 'Unknown' and height != 'Unknown' else "N/A"

        return {
            "stream": stream,
            "category_name": category_name,
            "epg_count": epg_count,
            "stream_info": stream_info,
            "resolution": resolution
        }

def print_stream_line(stream, category_name, epg_count, stream_info, resolution):
    status = stream_info.get('status', '')
    line = f"{stream['stream_id']:<10}{stream['name'][:60]:<60} {category_name[:40]:<40}{stream.get('tv_archive_duration', 'N/A'):<8}{str(epg_count):<5}{stream_info.get('codec_name', ''):<10}{resolution:<15}{str(stream_info.get('frame_rate', '')):<10}{status:<10}"
    with PRINT_LOCK:
        print(line, flush=True)

def main():
    global DEBUG_MODE

    signal.signal(signal.SIGINT, handle_sigint)

    parser = argparse.ArgumentParser(description="Xtream IPTV Downloader and Filter")
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
    parser.add_argument("--max-connections", type=int, default=DEFAULT_MAX_CONNECTIONS, help="Maximum simultaneous EPG/ffprobe operations (default 3).")
    parser.add_argument("--ordered", action="store_true", help="Preserve original ordering in output (slower if many streams).")
    args = parser.parse_args()

    masked_server = f"{'.'.join(['xxxxx'] + args.server.split('.')[1:])}"
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

    csv_data = []
    fieldnames = ["Stream ID", "Name", "Category", "Archive", "EPG", "Codec", "Resolution", "Frame Rate"]

    print(f"{'ID':<10}{'Name':<60} {'Category':<40}{'Archive':<8}{'EPG':<5}{'Codec':<10}{'Resolution':<15}{'Frame':<10}{'Status':<10}")
    print("=" * 168, flush=True)
    category_map = {cat["category_id"]: cat["category_name"] for cat in live_categories}

    # Concurrency control: process streams with a thread pool limiting simultaneous operations
    if args.max_connections < 1:
        args.max_connections = 1

    debug_log(f"Processing {len(filtered_streams)} streams with max {args.max_connections} concurrent operations")

    semaphore = threading.Semaphore(args.max_connections)

    # Submit tasks
    with ThreadPoolExecutor(max_workers=args.max_connections) as executor:
        futures = [executor.submit(process_stream, stream, args, category_map, semaphore) for stream in filtered_streams]

        if args.ordered:
            # Preserve order: iterate futures list in submission order
            for fut in futures:
                result = fut.result()
                stream = result["stream"]
                category_name = result["category_name"]
                epg_count = result["epg_count"]
                stream_info = result["stream_info"]
                resolution = result["resolution"]
                print_stream_line(stream, category_name, epg_count, stream_info, resolution)
                csv_data.append({
                    "Stream ID": stream["stream_id"],
                    "Name": stream['name'][:60],
                    "Category": category_name[:40],
                    "Archive": stream.get('tv_archive_duration', 'N/A'),
                    "EPG": epg_count,
                    "Codec": stream_info.get('codec_name', ''),
                    "Resolution": resolution,
                    "Frame Rate": stream_info.get('frame_rate', ''),
                })
        else:
            # Print as tasks complete for immediate feedback
            for fut in as_completed(futures):
                result = fut.result()
                stream = result["stream"]
                category_name = result["category_name"]
                epg_count = result["epg_count"]
                stream_info = result["stream_info"]
                resolution = result["resolution"]
                print_stream_line(stream, category_name, epg_count, stream_info, resolution)
                csv_data.append({
                    "Stream ID": stream["stream_id"],
                    "Name": stream['name'][:60],
                    "Category": category_name[:40],
                    "Archive": stream.get('tv_archive_duration', 'N/A'),
                    "EPG": epg_count,
                    "Codec": stream_info.get('codec_name', ''),
                    "Resolution": resolution,
                    "Frame Rate": stream_info.get('frame_rate', ''),
                })

    print("\n")
    if args.save:
        save_to_csv(args.save, csv_data, fieldnames)
    print("\n\n")

if __name__ == "__main__":
    main()