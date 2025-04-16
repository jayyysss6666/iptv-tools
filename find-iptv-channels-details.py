import os
import sys
import requests
import json
import argparse
import subprocess
import signal
import csv
import re  # Added for M3U parsing
from datetime import datetime
from urllib.parse import urlparse  # Added for URL parsing

CACHE_FILE_PATTERN = "cache-{server}-{data_type}.json"
DEBUG_MODE = False  # Default: Debugging is off


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
        debug_log(f"Response from server ({endpoint}): {response.text[:500]}")  # Print first 500 characters
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
    group_lower = group.lower() if group else None
    channel_lower = channel.lower() if channel else None

    # Create a set of matching category IDs for faster lookup if filtering by group
    matching_category_ids = set()
    if group_lower:
        for cat in live_categories:
            if group_lower in cat["category_name"].lower():
                matching_category_ids.add(cat["category_id"])

    for stream in live_streams:
        # Filter by group if specified
        # The category_id for M3U streams is set to the category_name during parsing
        if group_lower and stream["category_id"] not in matching_category_ids:
            continue
        # Filter by channel if specified
        if channel_lower and channel_lower not in stream["name"].lower():
            continue
        # Add the stream to the filtered list
        filtered_streams.append(stream)

    return filtered_streams


def check_ffprobe():
    """
    Checks if the ffprobe command is available on the system.
    Exits the program with an error message if not found.
    """
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
            text=True
        )

        output = json.loads(result.stdout)

        if 'streams' in output and len(output['streams']) > 0:
            stream_info = output['streams'][0]
            codec_name = stream_info.get('codec_name', 'Unknown')[:5]
            width = stream_info.get('width', 'Unknown')
            height = stream_info.get('height', 'Unknown')
            avg_frame_rate = stream_info.get('avg_frame_rate', 'Unknown')

            if avg_frame_rate != 'Unknown' and '/' in avg_frame_rate:
                num, denom = map(int, avg_frame_rate.split('/'))
                frame_rate = round(num / denom) if denom != 0 else "Unknown"
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
    except Exception as e:
        debug_log(f"Error in check_channel: {e}")
        return {"status": "error", "error_message": str(e)}


def save_to_csv(file_name, data, fieldnames):
    """
    Save data to a CSV file, ensuring all fields are enclosed in double quotes.

    :param file_name: The name of the CSV file to save.
    :param data: A list of dictionaries containing the data to write.
    :param fieldnames: A list of field names for the CSV header.
    """
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


def download_m3u_content(url):
    """Download content from an M3U URL."""
    debug_log(f"Downloading M3U content from {url}")
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36"
    }
    try:
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()  # Raise an exception for bad status codes
        return response.text
    except requests.exceptions.RequestException as e:
        print(f"Failed to download M3U content from {url}: {e}", file=sys.stderr)
        sys.exit(1)


def parse_m3u(m3u_content):
    """Parse M3U content to extract stream information."""
    debug_log("Parsing M3U content")
    streams = []
    category_map = {}  # Will map category name to a pseudo-ID if needed, or just store names
    current_stream_info = {}
    stream_id_counter = 1  # Simple counter for unique ID

    # Regex to capture relevant info from #EXTINF line
    extinf_regex = re.compile(r'#EXTINF:(?P<duration>-?\d+)(?:\s+(?P<attributes>.*?))?,(?P<name>.*)')
    # Regex to capture specific attributes like tvg-id, tvg-name, group-title
    attribute_regex = re.compile(r'(\S+?)="([^"]*)"')

    # Normalize line endings and split, then filter empty lines
    lines = [line.strip() for line in m3u_content.replace('\r\n', '\n').replace('\r', '\n').split('\n') if line.strip()]

    # Check if the first non-empty line *contains* #EXTM3U, allowing for potential BOM or odd characters before it
    if not lines or not lines[0].lstrip('\ufeff').startswith("#EXTM3U"):
        print("Error: Invalid M3U file format. Missing #EXTM3U header.", file=sys.stderr)
        debug_log(f"First line received: '{lines[0] if lines else '<empty>'}'")
        sys.exit(1)

    # Iterate starting from the line *after* the header
    header_found_index = 0
    for i, line in enumerate(lines):
        if line.lstrip('\ufeff').startswith("#EXTM3U"):
            header_found_index = i
            break

    for line in lines[header_found_index + 1:]:
        line = line.strip()
        if line.startswith('#EXTINF:'):
            match = extinf_regex.match(line)
            if match:
                current_stream_info = match.groupdict()
                attributes_str = current_stream_info.pop('attributes', '')
                current_stream_info['attributes'] = dict(attribute_regex.findall(attributes_str))
                # Prioritize group-title, fallback name if needed
                category_name = current_stream_info['attributes'].get('group-title', 'Unknown Group')
                current_stream_info['category_name'] = category_name
                if category_name not in category_map:
                    category_map[category_name] = category_name  # Store category name
        elif line and not line.startswith('#'):
            if 'name' in current_stream_info:  # Ensure we have preceding #EXTINF info
                stream_data = {
                    "stream_id": str(stream_id_counter),  # Use counter as ID
                    "name": current_stream_info['name'].strip(),
                    "category_name": current_stream_info['category_name'],
                    "stream_url": line,
                    # Add placeholders for fields expected by later processing
                    "category_id": current_stream_info['category_name'],  # Use name as category identifier
                    "tvg_id": current_stream_info['attributes'].get('tvg-id'),
                    "tvg_name": current_stream_info['attributes'].get('tvg-name'),
                    "tvg_logo": current_stream_info['attributes'].get('tvg-logo'),
                    "tv_archive_duration": "N/A"  # M3U usually doesn't contain this
                }
                streams.append(stream_data)
                current_stream_info = {}  # Reset for next entry
                stream_id_counter += 1
            else:
                debug_log(f"Skipping URL line without preceding #EXTINF: {line}")

    debug_log(f"Parsed {len(streams)} streams from M3U")
    # Convert category_map to the format expected by filter_data if needed, or adapt filter_data
    # For now, filter_data expects a list of dicts with 'category_id' and 'category_name'
    # Let's create a dummy live_categories structure
    live_categories = [{"category_id": name, "category_name": name} for name in category_map.keys()]

    return live_categories, streams


def main():
    global DEBUG_MODE

    # Set up the signal handler for Ctrl+C
    signal.signal(signal.SIGINT, handle_sigint)

    parser = argparse.ArgumentParser(description="Xtream IPTV Downloader and Filter")
    parser.add_argument("--nocache", action="store_true", help="Force download and ignore cache (only applies to Xtream mode).")
    parser.add_argument("--channel", help="Filter by channel name.")
    parser.add_argument("--category", help="Filter by category name.")
    parser.add_argument("--debug", action="store_true", help="Enable debug mode.")  # Debug flag
    parser.add_argument("--epgcheck", action="store_true", help="Check if channels provide EPG data (only applies to Xtream mode).")
    parser.add_argument("--check", action="store_true", help="Check stream resolution and frame rate using ffprobe.")
    parser.add_argument("--save", help="Save the output to a CSV file. Provide the file name.")

    # Mutually exclusive group for input method
    input_group = parser.add_mutually_exclusive_group(required=True)
    input_group.add_argument("--server", help="The Xtream server hostname or IP to connect to.")
    input_group.add_argument("--m3u_url", help="URL of the M3U playlist.")

    # Arguments dependent on --server
    parser.add_argument("--user", help="The username to use (requires --server).")
    parser.add_argument("--pw", help="The password to use (requires --server).")

    args = parser.parse_args()

    # Validate dependent arguments
    if args.server and (not args.user or not args.pw):
        parser.error("--user and --pw are required when --server is specified.")
    if args.m3u_url and (args.user or args.pw):
        parser.error("--user and --pw cannot be used with --m3u_url.")
    if args.m3u_url and args.nocache:
        print("Warning: --nocache flag has no effect when using --m3u_url.", file=sys.stderr)
    if args.m3u_url and args.epgcheck:
        print("Warning: --epgcheck flag is not supported when using --m3u_url. EPG info will be marked N/A.", file=sys.stderr)
        args.epgcheck = False  # Disable it internally

    # Print the date and time when the program is run
    run_time = datetime.now().strftime("%Y-%m-%d %H:%M")
    source_info = ""
    if args.server:
        masked_server = f"{'.'.join(['xxxxx'] + args.server.split('.')[1:])}"
        source_info = f"server {masked_server}"
    elif args.m3u_url:
        parsed_url = urlparse(args.m3u_url)
        masked_url = f"{parsed_url.scheme}://{parsed_url.netloc}/.../{parsed_url.path.split('/')[-1]}" if parsed_url.netloc else args.m3u_url
        source_info = f"M3U URL {masked_url}"

    print(f"\n\nfind-iptv-channels-details - Running for {source_info} on {run_time}\n")

    # Enable debug mode if the --debug flag is present
    DEBUG_MODE = args.debug
    debug_log("Debug mode enabled")     # Will only print if debug mode is set.  else ignored

    # Check ffprobe if --check is enabled
    if args.check:
        check_ffprobe()

    live_categories = []
    live_streams = []

    # --- Data Loading ---
    if args.m3u_url:
        # M3U Mode
        debug_log(f"Processing M3U URL: {args.m3u_url}")
        m3u_content = download_m3u_content(args.m3u_url)
        live_categories, live_streams = parse_m3u(m3u_content)
    elif args.server:
        # Xtream Mode
        debug_log(f"Processing Xtream Server: {args.server}")
        live_categories = load_cache(args.server, "live_categories") if not args.nocache else None
        live_streams = load_cache(args.server, "live_streams") if not args.nocache else None

        if not live_categories or not live_streams:
            debug_log("Cache miss or --nocache used. Downloading data from Xtream API.")
            live_categories = download_data(args.server, args.user, args.pw, "get_live_categories")
            live_streams = download_data(args.server, args.user, args.pw, "get_live_streams")
            if live_categories and live_streams:  # Save only if download succeeded
                save_cache(args.server, "live_categories", live_categories)
                save_cache(args.server, "live_streams", live_streams)
            else:
                print("Error: Failed to download data from Xtream server. Exiting.", file=sys.stderr)
                sys.exit(1)  # Exit if download fails

    if not live_streams:
        print("Error: No stream data loaded. Exiting.", file=sys.stderr)
        sys.exit(1)

    # Filter data
    filtered_streams = filter_data(live_categories, live_streams, args.category, args.channel)

    # Prepare CSV data and headers
    csv_data = []
    # Adjust headers based on mode? For now, keep them consistent, using N/A for missing M3U data
    fieldnames = ["ID", "Name", "Category", "Archive", "EPG", "Codec", "Resolution", "Frame Rate"]
    if args.m3u_url:
        # ID is just sequential number, Archive/EPG not available from basic M3U
        fieldnames = ["#", "Name", "Category", "Codec", "Resolution", "Frame Rate"]

    # Print and collect results
    if args.m3u_url:
        print(f"{'#':<5}{'Name':<60} {'Category':<40}{'Codec':<8}{'Resolution':<15}{'Frame':<10}")
        print("=" * 138)
    else:  # Xtream Mode
        print(f"{'ID':<10}{'Name':<60} {'Category':<40}{'Archive':<8}{'EPG':<5}{'Codec':<8}{'Resolution':<15}{'Frame':<10}")
        print("=" * 156)  # Adjusted width

    # Create category map only needed for Xtream mode display logic
    category_map = {}
    if args.server:
        category_map = {cat["category_id"]: cat["category_name"] for cat in live_categories}

    for stream in filtered_streams:
        # Get category name - logic differs slightly
        if args.m3u_url:
            category_name = stream.get("category_name", "Unknown")  # Directly available from parsing
            stream_url = stream.get("stream_url")
            archive_duration = "N/A"
            epg_count = "N/A"
            stream_id_display = stream.get('stream_id', 'N/A')  # Use the counter ID

        else:  # Xtream mode
            category_name = category_map.get(stream["category_id"], "Unknown")
            stream_url = f"http://{args.server}/{args.user}/{args.pw}/{stream['stream_id']}"
            archive_duration = stream.get('tv_archive_duration', 'N/A')
            epg_count = (
                check_epg(args.server, args.user, args.pw, stream["stream_id"]) if args.epgcheck else "N/A"
            )
            stream_id_display = stream.get('stream_id', 'N/A')

        stream_info = (
            check_channel(stream_url) if args.check else {"codec_name": "", "width": "", "height": "", "frame_rate": ""}
        )
        resolution = f"{stream_info.get('width', 'N/A')}x{stream_info.get('height', 'N/A')}" if args.check else ""  # Show N/A only if check fails, otherwise empty

        # Print to console - Adjust format based on mode
        if args.m3u_url:
            print(f"{stream_id_display:<5}{stream['name'][:60]:<60} {category_name[:40]:<40}{stream_info.get('codec_name', 'N/A'):<8}{resolution:<15}{stream_info.get('frame_rate', 'N/A'):<10}")
            # Collect data for CSV (M3U mode)
            csv_data.append({
                "#": stream_id_display,
                "Name": stream['name'][:60],
                "Category": category_name[:40],
                "Codec": stream_info.get('codec_name', 'N/A'),
                "Resolution": resolution,
                "Frame Rate": stream_info.get('frame_rate', 'N/A'),
            })
        else:  # Xtream mode
            print(f"{stream_id_display:<10}{stream['name'][:60]:<60} {category_name[:40]:<40}{archive_duration:<8}{epg_count:<5}{stream_info.get('codec_name', 'N/A'):<8}{resolution:<15}{stream_info.get('frame_rate', 'N/A'):<10}")
            # Collect data for CSV (Xtream mode)
            csv_data.append({
                "ID": stream_id_display,
                "Name": stream['name'][:60],
                "Category": category_name[:40],
                "Archive": archive_duration,
                "EPG": epg_count,
                "Codec": stream_info.get('codec_name', 'N/A'),
                "Resolution": resolution,
                "Frame Rate": stream_info.get('frame_rate', 'N/A'),
            })

    print(f"\n")
    # Write to CSV if --save is provided
    if args.save:
        # Ensure fieldnames match the mode
        save_to_csv(args.save, csv_data, fieldnames)
    print(f"\n\n")


if __name__ == "__main__":
    main()
