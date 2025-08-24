import os
import sys
import requests
import json
import argparse
import subprocess
import signal
import csv
import time
import statistics
from datetime import datetime

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

def check_ffmpeg():
    """Checks if the ffmpeg command is available on the system."""
    try:
        subprocess.run(
            ["ffmpeg", "-version"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=True,
        )
        debug_log("ffmpeg is installed and reachable.")
    except FileNotFoundError:
        print("Error: ffmpeg is not installed or not found in the system PATH. Please install ffmpeg before running this program.")
        sys.exit(1)
    except subprocess.CalledProcessError as e:
        print(f"Error: ffmpeg check failed with error: {e}")
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

def calculate_stability_score(metrics):
    """
    Calculate a stream stability score based on comprehensive playback metrics.
    
    Args:
        metrics (dict): Dictionary containing stream metrics
    
    Returns:
        dict: Stability assessment with score and detailed information
    """
    score = 100.0  # Start with perfect score
    penalty = 0.0  # Track total penalty
    reasons = []   # Track penalty reasons for debugging
    
    # === 1. Dropped Frames (30 points max) ===
    frames_processed = metrics.get('frames_processed', 0)
    dropped_frames = metrics.get('dropped_frames', 0)
    
    if frames_processed > 0:
        drop_percentage = (dropped_frames / frames_processed) * 100
        frame_penalty = min(30, drop_percentage * 3)  # 10% drops = 30 point penalty
        penalty += frame_penalty
        if frame_penalty > 0:
            reasons.append(f"Dropped frames ({drop_percentage:.2f}%): -{frame_penalty:.1f}")
    
    # === 2. Playback Speed (25 points max) ===
    # Speed < 1.0 means buffering, speed > 1.2 means skipping ahead to catch up
    speed = metrics.get('playback_speed', 0)
    
    if speed < 0.97:  # Significant buffering
        speed_penalty = min(25, (1 - speed) * 50)  # 0.5x speed = 25 point penalty
        penalty += speed_penalty
        reasons.append(f"Buffering (speed {speed:.2f}x): -{speed_penalty:.1f}")
    elif speed > 1.2:  # Skip-ahead playback
        speed_penalty = min(15, (speed - 1) * 30)  # 1.5x speed = 15 point penalty
        penalty += speed_penalty
        reasons.append(f"Speed fluctuation ({speed:.2f}x): -{speed_penalty:.1f}")
    
    # === 3. Buffering Score (25 points max) ===
    # Specifically counting rebuffering events
    rebuffer_count = metrics.get('rebuffer_events', 0)
    rebuffer_duration = metrics.get('rebuffer_duration', 0)
    
    if rebuffer_count > 0:
        rebuffer_penalty = min(25, rebuffer_count * 5 + rebuffer_duration)
        penalty += rebuffer_penalty
        reasons.append(f"Rebuffering (x{rebuffer_count}): -{rebuffer_penalty:.1f}")
    
    # Calculate final score
    final_score = max(0, 100 - penalty)
    
    # Determine status label
    if final_score >= 90:
        status = "excellent"
    elif final_score >= 75:
        status = "good"
    elif final_score >= 50:
        status = "fair"
    elif final_score >= 30:
        status = "poor"
    else:
        status = "unstable"
    
    return {
        'score': round(final_score, 1),
        'status': status,
        'penalty': round(penalty, 1),
        'reasons': reasons
    }

def check_stream_quality(url, duration=30):
    """
    Check stream for buffering/skipping by watching for a specified duration.
    Returns metrics about stream stability.
    
    Args:
        url (str): Stream URL to check
        duration (int): Duration in seconds to monitor the stream
    
    Returns:
        dict: Quality metrics including stability score, dropped frames, etc.
    """
    debug_log(f"Checking stream quality for {url} for {duration} seconds")
    
    start_time = time.time()
    metrics = {
        'duration_checked': duration,
        'status': 'unknown',
        'rebuffer_events': 0,
        'rebuffer_duration': 0.0,
    }
    
    try:
        # Run FFmpeg to analyze stream for specified duration
        cmd = [
            "ffmpeg",
            "-loglevel", "info",
            "-re",
            "-i", url,
            "-t", str(duration),
            "-f", "null",
            "-"
        ]
        
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        
        # Track when first frame appears
        first_frame_time = None
        last_frame_time = None
        last_speed = None
        frame_times = []
        current_status = "connecting"
        
        # Non-blocking read with periodic polling for real-time metrics
        while process.poll() is None:
            line = process.stderr.readline()
            if not line:
                time.sleep(0.1)
                continue
                
            # Process line for metrics
            if "frame=" in line:
                current_time = time.time()
                
                if first_frame_time is None:
                    first_frame_time = current_time
                
                last_frame_time = current_time
                frame_times.append(current_time)
                
                # Extract speed info
                if "speed=" in line:
                    speed_part = line.split("speed=")[1].split()[0]
                    try:
                        current_speed = float(speed_part.rstrip('x'))
                        
                        # Detect rebuffering events (significant speed drop)
                        if last_speed is not None and current_speed < 0.5 and last_speed > 0.9:
                            metrics['rebuffer_events'] += 1
                            current_status = "rebuffering"
                        elif current_speed > 0.9 and current_status == "rebuffering":
                            current_status = "playing"
                            
                        last_speed = current_speed
                    except ValueError:
                        pass
        
        # Get final output
        stdout, stderr = process.communicate()
        
        # Process the entire output for overall metrics
        frames_processed = 0
        fps = 0
        speed = 0
        drops = 0
        
        for line in stderr.split('\n'):
            if "frame=" in line:
                parts = line.split()
                for i, part in enumerate(parts):
                    if part == "frame=":
                        try:
                            frames_processed = int(parts[i+1])
                        except (ValueError, IndexError):
                            pass
                    elif part == "fps=":
                        try:
                            fps = float(parts[i+1])
                        except (ValueError, IndexError):
                            pass
                    elif part == "speed=":
                        try:
                            speed_str = parts[i+1].rstrip('x')
                            speed = float(speed_str)
                        except (ValueError, IndexError):
                            speed = 0
                    elif "drop=" in part:
                        try:
                            drops = int(part.split("=")[1])
                        except (ValueError, IndexError):
                            pass
        
        metrics.update({
            'frames_processed': frames_processed,
            'average_fps': fps,
            'playback_speed': speed,
            'dropped_frames': drops
        })
        
        # Calculate frame timing consistency (if we have enough frames)
        if len(frame_times) > 5:
            frame_intervals = [frame_times[i+1] - frame_times[i] for i in range(len(frame_times)-1)]
            metrics['frame_interval_std_dev'] = statistics.stdev(frame_intervals) if len(frame_intervals) > 1 else 0
        
        # Calculate total playback time
        if first_frame_time and last_frame_time:
            metrics['actual_playback_time'] = last_frame_time - first_frame_time
        
        # Get stability assessment
        stability_result = calculate_stability_score(metrics)
        metrics.update(stability_result)
        
        return metrics
        
    except Exception as e:
        debug_log(f"Error checking stream quality: {e}")
        return {
            'duration_checked': 0,
            'status': 'error',
            'error_message': str(e)
        }

def check_connection_time(url, timeout=10):
    """
    Check how long it takes to establish a connection and get first video frame.
    
    Args:
        url (str): Stream URL to check
        timeout (int): Maximum seconds to wait
        
    Returns:
        dict: Connection time metrics
    """
    debug_log(f"Checking connection time for {url} (timeout: {timeout}s)")
    
    metrics = {
        'status': 'unknown',
        'connection_time': None
    }
    
    try:
        connect_start = time.time()
        
        # Use FFmpeg to check connection time to first frame
        cmd = [
            "ffmpeg",
            "-loglevel", "info",
            "-i", url,
            "-t", "1", # Only need 1 second to test connection
            "-f", "null",
            "-"
        ]
        
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        
        # Wait for first frame or timeout
        first_frame_time = None
        start_time = time.time()
        
        while process.poll() is None:
            # Check if we've reached timeout
            if time.time() - start_time > timeout:
                process.terminate()
                metrics['status'] = 'timeout'
                metrics['connection_time'] = timeout
                return metrics
                
            line = process.stderr.readline()
            if not line:
                time.sleep(0.05)
                continue
                
            # First frame indicator
            if "frame=" in line and "fps=" in line:
                first_frame_time = time.time()
                process.terminate()  # We got what we needed
                break
        
        # Calculate connection time
        if first_frame_time is not None:
            conn_time = first_frame_time - connect_start
            metrics['connection_time'] = round(conn_time, 2)
            
            # Rate the connection time
            if conn_time < 1.0:
                metrics['status'] = 'excellent'
            elif conn_time < 2.5:
                metrics['status'] = 'good'
            elif conn_time < 5.0:
                metrics['status'] = 'fair'
            else:
                metrics['status'] = 'slow'
        else:
            metrics['status'] = 'failed'
            
        return metrics
        
    except Exception as e:
        debug_log(f"Error checking connection time: {e}")
        return {
            'status': 'error',
            'error_message': str(e)
        }

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
    parser.add_argument("--quality", action="store_true", help="Check stream quality for buffering/skipping issues.")
    parser.add_argument("--quality-duration", type=int, default=30, help="Duration in seconds to monitor stream quality (default: 30).")
    parser.add_argument("--conn", action="store_true", help="Check connection time to first frame.")
    parser.add_argument("--conn-timeout", type=int, default=10, help="Maximum seconds to wait for connection (default: 10).")
    parser.add_argument("--save", help="Save the output to a CSV file. Provide the file name.")
    args = parser.parse_args()

    masked_server = f"{'.'.join(['xxxxx'] + args.server.split('.')[1:])}"
    run_time = datetime.now().strftime("%Y-%m-%d %H:%M")
    print(f"\n\nfind-iptv-channels-details - Running for server {masked_server} on {run_time}\n")

    DEBUG_MODE = args.debug
    debug_log("Debug mode enabled")

    if args.check:
        check_ffprobe()
        
    if args.quality or args.conn:
        check_ffmpeg()

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
    if args.quality:
        fieldnames.extend(["Quality", "Stability Score", "Dropped Frames", "Buffering"])
    if args.conn:
        fieldnames.extend(["Conn Time", "Conn Status"])

    # Create header based on what options are enabled
    header = f"{'ID':<10}{'Name':<50} {'Category':<30}{'Archive':<8}{'EPG':<5}{'Codec':<8}{'Resolution':<15}{'Frame':<6}"
    if args.quality:
        header += f"{'Quality':<10}{'Stability':<8}{'Drops':<6}{'Buffer':<8}"
    if args.conn:
        header += f"{'Conn(s)':<8}{'ConnStat':<8}"
    
    print(header)
    print("=" * (len(header) + 20))  # Add some extra for safety
    
    category_map = {cat["category_id"]: cat["category_name"] for cat in live_categories}
    total_streams = len(filtered_streams)
    
    for index, stream in enumerate(filtered_streams):
        category_name = category_map.get(stream["category_id"], "Unknown")
        epg_count = ""
        if args.epgcheck:
            epg_count = check_epg(args.server, args.user, args.pw, stream["stream_id"])
            time.sleep(3)  # 3-second pause after EPG check

        stream_url = f"http://{args.server}/{args.user}/{args.pw}/{stream['stream_id']}"
        
        stream_info = {"codec_name": "", "width": "", "height": "", "frame_rate": ""}
        if args.check:
            stream_info = check_channel(stream_url)
            time.sleep(3)  # 3-second pause after ffprobe/stream check

        conn_metrics = {}
        if args.conn:
            print(f"[{index+1}/{total_streams}] Checking connection time for {stream['name']} (ID: {stream['stream_id']})...")
            conn_metrics = check_connection_time(stream_url, args.conn_timeout)
            time.sleep(1)  # Brief pause after connection check

        quality_metrics = {}
        if args.quality:
            print(f"[{index+1}/{total_streams}] Checking quality for {stream['name']} (ID: {stream['stream_id']})...")
            quality_metrics = check_stream_quality(stream_url, args.quality_duration)
            time.sleep(3)  # Brief pause after quality check

        resolution = f"{stream_info.get('width', 'N/A')}x{stream_info.get('height', 'N/A')}" if args.check else ""
        
        # Build the row data for display
        row_data = {
            "Stream ID": stream["stream_id"],
            "Name": stream['name'][:50],
            "Category": category_name[:30],
            "Archive": stream.get('tv_archive_duration', 'N/A'),
            "EPG": epg_count,
            "Codec": stream_info.get('codec_name', 'N/A'),
            "Resolution": resolution,
            "Frame Rate": stream_info.get('frame_rate', 'N/A'),
        }
        
        # Prepare the output line
        stream_line = f"{stream['stream_id']:<10}{stream['name'][:50]:<50} {category_name[:30]:<30}"
        stream_line += f"{stream.get('tv_archive_duration', 'N/A'):<8}{epg_count:<5}"
        stream_line += f"{stream_info.get('codec_name', 'N/A'):<8}{resolution:<15}"
        stream_line += f"{stream_info.get('frame_rate', 'N/A'):<6}"

        # Add quality metrics if requested
        if args.quality:
            quality_status = quality_metrics.get('status', 'N/A')
            stability_score = quality_metrics.get('score', 'N/A')
            dropped_frames = quality_metrics.get('dropped_frames', 'N/A')
            
            # Get rebuffer event count for buffering metric
            rebuffer_count = quality_metrics.get('rebuffer_events', 'N/A')
            
            stream_line += f"{quality_status:<10}{stability_score:<8}{dropped_frames:<6}{rebuffer_count:<8}"
            
            # Update the row data
            row_data.update({
                "Quality": quality_status,
                "Stability Score": stability_score,
                "Dropped Frames": dropped_frames,
                "Buffering": rebuffer_count
            })

        # Add connection metrics if requested
        if args.conn:
            conn_time = conn_metrics.get('connection_time', 'N/A')
            conn_status = conn_metrics.get('status', 'N/A')
            
            # Format connection time nicely (add 's' suffix for seconds)
            if isinstance(conn_time, (int, float)):
                conn_time_str = f"{conn_time:.1f}"
            else:
                conn_time_str = str(conn_time)
            
            stream_line += f"{conn_time_str:<8}{conn_status:<8}"
            
            # Update the row data
            row_data.update({
                "Conn Time": conn_time,
                "Conn Status": conn_status
            })

        print(stream_line)
        csv_data.append(row_data)

    print(f"\nProcessed {len(filtered_streams)} streams")
    if args.save:
        save_to_csv(args.save, csv_data, fieldnames)
    print(f"\n\n")

if __name__ == "__main__":
    main()
