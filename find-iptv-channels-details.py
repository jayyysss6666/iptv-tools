import os
import sys
import requests
import json
import argparse
import subprocess
import signal
import csv
import time
import random
from datetime import datetime
from collections import deque

CACHE_FILE_PATTERN = "cache-{server}-{data_type}.json"
DEBUG_MODE = False  # Default: Debugging is off
MAX_CONNECTIONS = 3  # Maximum concurrent connections allowed
connection_queue = deque(maxlen=MAX_CONNECTIONS)  # Track active connections

def debug_log(message):
    """
    Logs a debug message if debugging is enabled.
    """
    if DEBUG_MODE:
        print(f"[DEBUG] {message}")

def load_cache(server, data_type):
    """Load data from the cache file if it exists and is up-to-date."""
    debug_log(f"Attempting to load cache for {data_type} on server {server}")
    cache_file = CACHE_FILE_PATTERN.format(server=server, data_type=data_type)
    if os.path.exists(cache_file):
        # Check if the cache file was created today
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
    response = requests.get(url, headers=headers, params=params)
    if response.status_code == 200:
        debug_log(f"Response from server ({endpoint}): {response.text[:500]}")
        return response.json()
    else:
        print(f"Failed to fetch {endpoint} data: {response.status_code}", file=sys.stderr)
        sys.exit(1)

def check_epg(server, user, password, stream_id):
    """Check EPG data for a specific channel."""
    debug_log(f"Checking EPG for stream ID {stream_id}")
    epg_data = download_data(server, user, password, "get_simple_data_table", {"stream_id": stream_id})

    if isinstance(epg_data, dict) and epg_data.get("epg_listings"):
        return len(epg_data["epg_listings"])  # Return the count of EPG entries

    elif isinstance(epg_data, list):
        debug_log(f"Unexpected list response for EPG data: {epg_data}")
        return len(epg_data)  # Return the length of the list

    else:
        debug_log(f"Unexpected EPG response type: {type(epg_data)}")
        return 0  # No EPG data available

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

def manage_connections(stream_id):
    """
    Manage connection queue to prevent exceeding connection limit.
    Returns how long to wait before proceeding.
    """
    current_time = time.time()
    
    # Remove expired connections (older than 60 seconds)
    while connection_queue and current_time - connection_queue[0][1] > 60:
        connection_queue.popleft()
    
    # If we're at max connections, wait
    if len(connection_queue) >= MAX_CONNECTIONS:
        # Calculate wait time based on oldest connection
        wait_time = max(5, 60 - (current_time - connection_queue[0][1])) 
        debug_log(f"Connection limit reached. Waiting {wait_time:.1f} seconds before checking stream {stream_id}.")
        return wait_time
    
    # Add this connection to the queue
    connection_queue.append((stream_id, current_time))
    return 0

def check_channel(url, stream_id):
    """Check channel details using ffprobe with connection management."""
    # First, manage connections to avoid exceeding limits
    wait_time = manage_connections(stream_id)
    if wait_time > 0:
        time.sleep(wait_time)
    
    # Add a small random delay to prevent exactly simultaneous connections
    jitter = random.uniform(0.5, 2.0)
    time.sleep(jitter)
    
    debug_log(f"Checking stream {stream_id} with URL: {url}")
    
    start_time = time.time()
    try:
        result = subprocess.run(
            [
                "ffprobe",
                "-v", "error",
                "-show_entries", "stream=codec_type,codec_name,width,height,avg_frame_rate,bit_rate",
                "-of", "json",
                "-timeout", "10000000",  # 10 seconds timeout in microseconds
                url
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=15  # Overall process timeout of 15 seconds
        )
        load_time = time.time() - start_time
        
        output = json.loads(result.stdout) if result.stdout.strip() else {}

        if 'streams' in output and len(output['streams']) > 0:
            video_stream = next((s for s in output['streams'] if s.get('codec_type') == 'video'), None)
            audio_stream = next((s for s in output['streams'] if s.get('codec_type') == 'audio'), None)

            info = {
                "status": "working",
                "load_time": f"{load_time:.2f}s",
                "codec_name": "N/A", "width": "N/A", "height": "N/A", "frame_rate": "N/A", "video_bitrate": "N/A",
                "audio_bitrate": "N/A"
            }

            if video_stream:
                info["codec_name"] = video_stream.get('codec_name', 'Unknown')[:5]
                info["width"] = video_stream.get('width', 'N/A')
                info["height"] = video_stream.get('height', 'N/A')
                avg_frame_rate = video_stream.get('avg_frame_rate', 'N/A')
                if avg_frame_rate != 'N/A' and '/' in str(avg_frame_rate):
                    num, denom = map(int, avg_frame_rate.split('/'))
                    info["frame_rate"] = round(num / denom) if denom != 0 else "N/A"
                else:
                    info["frame_rate"] = avg_frame_rate
                
                if 'bit_rate' in video_stream:
                    info["video_bitrate"] = f"{int(video_stream['bit_rate']) // 1000}k"
                
            if audio_stream and 'bit_rate' in audio_stream:
                info["audio_bitrate"] = f"{int(audio_stream['bit_rate']) // 1000}k"

            return info
        else:
            debug_log(f"No streams found in ffprobe output for URL: {url}")
            return {"status": "not working", "load_time": f"{load_time:.2f}s"}
    except subprocess.TimeoutExpired:
        load_time = time.time() - start_time
        debug_log(f"Timeout when checking stream {stream_id}")
        return {"status": "timeout", "load_time": f"{load_time:.2f}s"}
    except Exception as e:
        load_time = time.time() - start_time
        debug_log(f"Error in check_channel for stream {stream_id}: {e}")
        return {"status": "error", "load_time": f"{load_time:.2f}s"}

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

def handle_sigint(signal, frame):
    """Handle Ctrl+C gracefully."""
    print("\nProgram interrupted by user. Exiting...")
    sys.exit(0)

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
    parser.add_argument("--max-connections", type=int, default=3, 
                        help="Maximum concurrent connections to the server (default: 3).")
    args = parser.parse_args()

    global MAX_CONNECTIONS
    MAX_CONNECTIONS = args.max_connections

    masked_server = f"{'.'.join(['xxxxx'] + args.server.split('.')[1:])}"
    run_time = datetime.now().strftime("%Y-%m-%d %H:%M")
    print(f"\n\nfind-iptv-channels-details - Running for server {masked_server} on {run_time}\n")
    print(f"Connection limit set to: {MAX_CONNECTIONS}")

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
    fieldnames = ["Stream ID", "Name", "Category", "Archive", "EPG", "Load Time", "Codec", "Resolution", "Frame Rate", "Video Bitrate", "Audio Bitrate"]

    print(f"{'ID':<10}{'Name':<50} {'Category':<30}{'Archive':<8}{'EPG':<5}{'Load':<8}{'Codec':<8}{'Resolution':<12}{'Frame':<8}{'V-Rate':<10}{'A-Rate':<10}")
    print("=" * 174)
    category_map = {cat["category_id"]: cat["category_name"] for cat in live_categories}
    
    total_channels = len(filtered_streams)
    for i, stream in enumerate(filtered_streams):
        category_name = category_map.get(stream["category_id"], "Unknown")
        epg_count = ""
        
        if args.epgcheck:
            epg_count = check_epg(args.server, args.user, args.pw, stream["stream_id"])
            # Increased sleep to better manage connections with EPG
            time.sleep(5)  

        stream_url = f"http://{args.server}/{args.user}/{args.pw}/{stream['stream_id']}"
        stream_info = (
            check_channel(stream_url, stream["stream_id"]) if args.check 
            else {}
        )
        
        resolution = ""
        if args.check:
            resolution = f"{stream_info.get('width', 'N/A')}x{stream_info.get('height', 'N/A')}"
        
        # Progress indicator
        progress = f"[{i+1}/{total_channels}] "
        
        print(f"{progress}{stream['stream_id']:<10}{stream['name'][:50]:<50} {category_name[:30]:<30}"
              f"{stream.get('tv_archive_duration', 'N/A'):<8}{epg_count:<5}"
              f"{stream_info.get('load_time', 'N/A'):<8}"
              f"{stream_info.get('codec_name', 'N/A'):<8}"
              f"{resolution:<12}"
              f"{stream_info.get('frame_rate', 'N/A'):<8}"
              f"{stream_info.get('video_bitrate', 'N/A'):<10}"
              f"{stream_info.get('audio_bitrate', 'N/A'):<10}")

        csv_data.append({
            "Stream ID": stream["stream_id"],
            "Name": stream['name'],
            "Category": category_name,
            "Archive": stream.get('tv_archive_duration', 'N/A'),
            "EPG": epg_count,
            "Load Time": stream_info.get('load_time', 'N/A'),
            "Codec": stream_info.get('codec_name', 'N/A'),
            "Resolution": resolution,
            "Frame Rate": stream_info.get('frame_rate', 'N/A'),
            "Video Bitrate": stream_info.get('video_bitrate', 'N/A'),
            "Audio Bitrate": stream_info.get('audio_bitrate', 'N/A'),
        })

    print(f"\n")
    if args.save:
        save_to_csv(args.save, csv_data, fieldnames)
    print(f"\n\n")

if __name__ == "__main__":
    main()
