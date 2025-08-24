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
