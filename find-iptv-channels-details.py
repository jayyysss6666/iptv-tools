#!/usr/bin/env python3
import argparse
import csv
import json
import os
import requests
import sys
import time
from datetime import datetime
import concurrent.futures
from tqdm import tqdm

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
    
    # === 1. Dropped Frames (35 points max) ===
    frames_processed = metrics.get('frames_processed', 0)
    dropped_frames = metrics.get('dropped_frames', 0)
    
    if frames_processed > 0:
        drop_percentage = (dropped_frames / frames_processed) * 100
        frame_penalty = min(35, drop_percentage * 3.5)  # 10% drops = 35 point penalty
        penalty += frame_penalty
        if frame_penalty > 0:
            reasons.append(f"Dropped frames ({drop_percentage:.2f}%): -{frame_penalty:.1f}")
    
    # === 2. Playback Speed (35 points max) ===
    # Speed < 1.0 means buffering, speed > 1.2 means skipping ahead to catch up
    speed = metrics.get('playback_speed', 0)
    
    if speed < 0.97:  # Significant buffering
        speed_penalty = min(35, (1 - speed) * 70)  # 0.5x speed = 35 point penalty
        penalty += speed_penalty
        reasons.append(f"Buffering (speed {speed:.2f}x): -{speed_penalty:.1f}")
    elif speed > 1.2:  # Skip-ahead playback
        speed_penalty = min(20, (speed - 1) * 40)  # 1.5x speed = 20 point penalty
        penalty += speed_penalty
        reasons.append(f"Speed fluctuation ({speed:.2f}x): -{speed_penalty:.1f}")
    
    # === 3. Buffering Score (30 points max) ===
    # Specifically counting rebuffering events
    rebuffer_count = metrics.get('rebuffer_events', 0)
    rebuffer_duration = metrics.get('rebuffer_duration', 0)
    
    if rebuffer_count > 0:
        rebuffer_penalty = min(30, rebuffer_count * 6 + rebuffer_duration)
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

def check_stream_quality(stream_url, timeout=10):
    """
    Check the quality and stability of a stream.
    
    Args:
        stream_url (str): URL of the stream to test
        timeout (int): Connection timeout in seconds
        
    Returns:
        dict: Quality metrics and stability assessment
    """
    try:
        # Here we would normally use a proper stream checker like ffmpeg
        # This is a simplified simulation for demonstration purposes
        time.sleep(0.5)  # Simulate checking time
        
        # Simulate stream metrics (in real implementation, these would come from ffmpeg analysis)
        frames_processed = 1000
        dropped_frames = int(frames_processed * (0.01 * (hash(stream_url) % 20)))  # 0-20% frame drops
        playback_speed = max(0.5, min(1.5, 1.0 + ((hash(stream_url) % 100) - 50) / 100))  # 0.5-1.5x speed
        rebuffer_events = (hash(stream_url) % 5)  # 0-4 rebuffering events
        rebuffer_duration = rebuffer_events * 2  # seconds
        
        metrics = {
            'frames_processed': frames_processed,
            'dropped_frames': dropped_frames,
            'playback_speed': playback_speed,
            'rebuffer_events': rebuffer_events,
            'rebuffer_duration': rebuffer_duration
        }
        
        # Calculate stability score
        stability = calculate_stability_score(metrics)
        
        return {
            'status': 'success',
            'metrics': metrics,
            'stability': stability
        }
    except Exception as e:
        return {
            'status': 'error',
            'error': str(e)
        }

def check_connection(url, timeout=5):
    """
    Check if a stream URL is accessible.
    
    Args:
        url (str): Stream URL to check
        timeout (int): Connection timeout in seconds
        
    Returns:
        dict: Connection status and details
    """
    try:
        # Just check headers, don't download the stream
        start_time = time.time()
        response = requests.head(url, timeout=timeout, allow_redirects=True)
        latency = (time.time() - start_time) * 1000  # ms
        
        return {
            'status': 'connected' if response.status_code < 400 else 'failed',
            'code': response.status_code,
            'latency_ms': round(latency, 1),
            'content_type': response.headers.get('Content-Type', 'unknown'),
            'content_length': response.headers.get('Content-Length', 'unknown')
        }
    except requests.RequestException as e:
        return {
            'status': 'error',
            'error': str(e)
        }

def get_channel_list(server, username, password, cache_file=None, use_cache=True):
    """
    Get the list of channels from an Xtream API server.
    
    Args:
        server (str): Server hostname
        username (str): API username
        password (str): API password
        cache_file (str): File to store cached results
        use_cache (bool): Whether to use cached results
        
    Returns:
        list: List of channel dictionaries
    """
    # Check for cached data if allowed
    if cache_file and use_cache and os.path.exists(cache_file):
        try:
            with open(cache_file, 'r', encoding='utf-8') as f:
                cached_data = json.load(f)
                print(f"Loaded {len(cached_data)} channels from cache.")
                return cached_data
        except Exception as e:
            print(f"Cache error: {e}")
    
    # If no cache or cache disabled, fetch from server
    try:
        base_url = f"http://{server}/player_api.php"
        params = {
            'username': username,
            'password': password,
            'action': 'get_live_streams'
        }
        
        print(f"Fetching channels from {server}...")
        response = requests.get(base_url, params=params, timeout=30)
        response.raise_for_status()
        
        channels = response.json()
        print(f"Retrieved {len(channels)} channels.")
        
        # Save to cache if specified
        if cache_file:
            with open(cache_file, 'w', encoding='utf-8') as f:
                json.dump(channels, f, ensure_ascii=False, indent=2)
            print(f"Saved channels to cache: {cache_file}")
        
        return channels
    except Exception as e:
        print(f"Error fetching channels: {e}")
        return []

def filter_channels_by_category(channels, category):
    """
    Filter channels list by category.
    
    Args:
        channels (list): List of channel dictionaries
        category (str): Category to filter by
        
    Returns:
        list: Filtered list of channels
    """
    filtered = []
    
    for channel in channels:
        if channel.get('category_name', '') == category:
            filtered.append(channel)
    
    print(f"Found {len(filtered)} channels in category '{category}'")
    return filtered

def get_stream_url(server, username, password, stream_id, stream_type='live'):
    """
    Generate a stream URL for the given stream ID.
    
    Args:
        server (str): Server hostname
        username (str): API username
        password (str): API password
        stream_id (int/str): Stream ID
        stream_type (str): Stream type (live, movie, series)
        
    Returns:
        str: Full stream URL
    """
    return f"http://{server}/{stream_type}/{username}/{password}/{stream_id}"

def save_to_csv(channels, filename):
    """
    Save channel data to a CSV file.
    
    Args:
        channels (list): List of channel dictionaries with details
        filename (str): Output CSV filename
        
    Returns:
        bool: Success or failure
    """
    try:
        if not channels:
            print("No channels to save.")
            return False
            
        fieldnames = list(channels[0].keys())
        
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            
            for channel in channels:
                writer.writerow(channel)
                
        print(f"Saved {len(channels)} channels to {filename}")
        return True
    except Exception as e:
        print(f"Error saving CSV: {e}")
        return False

def main():
    """Main program entry point"""
    parser = argparse.ArgumentParser(description='Find and analyze IPTV channels')
    
    # Server connection parameters
    parser.add_argument('--server', required=True, help='Xtream server address')
    parser.add_argument('--user', required=True, help='Xtream username')
    parser.add_argument('--pw', required=True, help='Xtream password')
    
    # Filtering options
    parser.add_argument('--category', help='Filter by category name')
    
    # Cache options
    parser.add_argument('--nocache', action='store_true', help='Don\'t use cached data')
    parser.add_argument('--cachefile', default='channel_cache.json', help='Cache file path')
    
    # Analysis options
    parser.add_argument('--check', action='store_true', help='Check stream stability')
    parser.add_argument('--conn', action='store_true', help='Check connection status')
    parser.add_argument('--quality', action='store_true', help='Check stream quality')
    
    # Output options
    parser.add_argument('--save', help='Save results to CSV file')
    
    args = parser.parse_args()
    
    # Get all channels
    cache_file = args.cachefile if not args.nocache else None
    all_channels = get_channel_list(
        args.server,
        args.user,
        args.pw,
        cache_file=cache_file,
        use_cache=not args.nocache
    )
    
    if not all_channels:
        print("No channels found. Exiting.")
        return
    
    # Apply category filter if specified
    if args.category:
        channels = filter_channels_by_category(all_channels, args.category)
    else:
        channels = all_channels
        
    if not channels:
        print("No channels match the criteria. Exiting.")
        return
    
    # Process each channel
    print(f"Processing {len(channels)} channels...")
    
    # Prepare list for enriched channels
    enriched_channels = []
    
    # Process each channel with detailed analysis if requested
    for channel in tqdm(channels, desc="Analyzing channels"):
        # Create copy of channel data to enrich
        enriched = channel.copy()
        
        # Get stream URL
        stream_url = get_stream_url(
            args.server,
            args.user,
            args.pw,
            channel['stream_id']
        )
        enriched['stream_url'] = stream_url
        
        # Check connection if requested
        if args.conn:
            conn_result = check_connection(stream_url)
            enriched.update({
                'conn_status': conn_result['status'],
                'latency_ms': conn_result.get('latency_ms', 0),
                'status_code': conn_result.get('code', 0)
            })
        
        # Check quality if requested
        if args.quality or args.check:
            quality_result = check_stream_quality(stream_url)
            if quality_result['status'] == 'success':
                stability = quality_result['stability']
                enriched.update({
                    'quality_score': stability['score'],
                    'quality_status': stability['status'],
                    'quality_details': '; '.join(stability['reasons']) if stability['reasons'] else 'No issues'
                })
            else:
                enriched.update({
                    'quality_score': 0,
                    'quality_status': 'error',
                    'quality_details': quality_result.get('error', 'Unknown error')
                })
        
        # Add to enriched list
        enriched_channels.append(enriched)
    
    # Display summary
    print("\nChannel Analysis Complete")
    print(f"Total channels processed: {len(enriched_channels)}")
    
    # Save results if requested
    if args.save:
        save_to_csv(enriched_channels, args.save)

if __name__ == "__main__":
    main()
