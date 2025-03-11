#!/usr/bin/env python3
"""
TikTok Sound Info - Optimized for high-volume API calls
This script efficiently retrieves sound information from TikTok by reusing browser sessions.
It can process millions of sound IDs per day with minimal overhead.

Usage:
  python sound_info_optimized.py <sound_id>                # Process a single sound
  python sound_info_optimized.py --batch <count>           # Process multiple sounds from sounds_ids.csv
  python sound_info_optimized.py --file <filename> <count> # Process sounds from a specific file
"""

from TikTokApi import TikTokApi
import asyncio
import os
import json
import sys
import time
import csv
import logging
import signal
import argparse
from datetime import datetime
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("sound_info.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("sound_info")

# Get ms_token from environment variable
ms_token = os.environ.get("ms_token", None)

# Global API instance to reuse across calls
api_instance = None

# Track when we last made a request to avoid rate limiting
last_request_time = 0

# Minimum delay between requests (in seconds)
MIN_REQUEST_DELAY = 0.1

# Flag to indicate if we're shutting down
shutting_down = False

def signal_handler(sig, frame):
    """Handle interrupt signals gracefully"""
    global shutting_down
    if shutting_down:
        logger.warning("Forced exit requested, terminating immediately")
        sys.exit(1)
    
    logger.warning("Interrupt received, finishing current task and shutting down...")
    shutting_down = True

# Register signal handlers
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

async def initialize_api():
    """Initialize the API once and reuse it"""
    global api_instance
    if api_instance is None:
        logger.info("Initializing API and creating session...")
        start_time = time.time()
        
        api_instance = TikTokApi()
        
        # Create only one session and reuse it
        # Disable resource types that aren't needed for API calls
        suppress_resources = ["image", "media", "font", "stylesheet"]
        
        await api_instance.create_sessions(
            ms_tokens=[ms_token] if ms_token else None,
            num_sessions=1,
            sleep_after=1,
            browser=os.getenv("TIKTOK_BROWSER", "chromium"),
            suppress_resource_load_types=suppress_resources,
            headless=True,
            override_browser_args=["--headless=new"],
            timeout=15000
        )
        
        elapsed = time.time() - start_time
        logger.info(f"API initialization completed in {elapsed:.2f} seconds")
    
    return api_instance

async def sound_info(sound_id, retry_count=1, output_dir="sound_data"):
    """Get sound info for a specific sound ID with retry logic"""
    global last_request_time, shutting_down
    
    if shutting_down:
        logger.info(f"Skipping sound {sound_id} due to shutdown")
        return None, 0
    
    start_time = time.time()
    
    # Add a small delay between requests to avoid rate limiting
    elapsed_since_last = start_time - last_request_time
    if last_request_time > 0 and elapsed_since_last < MIN_REQUEST_DELAY:
        delay = MIN_REQUEST_DELAY - elapsed_since_last
        await asyncio.sleep(delay)
    
    # Create output directory if it doesn't exist
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    
    attempts = 0
    while attempts <= retry_count and not shutting_down:
        try:
            api = await initialize_api()
            sound = api.sound(id=sound_id)
            sound_details = await sound.info()
            
            elapsed = time.time() - start_time
            last_request_time = time.time()
            
            logger.info(f"Retrieved sound info for {sound_id} in {elapsed:.2f} seconds")
            
            # Save to file with sound ID in filename
            output_file = Path(output_dir) / f"sound_info_{sound_id}.json"
            with open(output_file, "w") as f:
                json.dump(sound_details, f, indent=4)
                
            return sound_details, elapsed
        
        except Exception as e:
            attempts += 1
            if attempts <= retry_count and not shutting_down:
                retry_delay = 1 * attempts
                logger.warning(f"Error retrieving sound {sound_id}: {str(e)}. Retrying in {retry_delay}s... (Attempt {attempts}/{retry_count})")
                await asyncio.sleep(retry_delay)
            else:
                elapsed = time.time() - start_time
                logger.error(f"Failed to retrieve sound {sound_id} after {retry_count} retries: {str(e)} in {elapsed:.2f} seconds")
                return None, elapsed

async def process_multiple_sounds(sound_ids, max_sounds=None, output_csv=None, output_dir="sound_data"):
    """Process multiple sound IDs sequentially, reusing the same session"""
    global shutting_down
    
    results = []
    total_start_time = time.time()
    
    # Limit the number of sounds to process if specified
    if max_sounds and max_sounds > 0:
        sound_ids = sound_ids[:max_sounds]
    
    # Create CSV file for results if specified
    csv_file = None
    csv_writer = None
    if output_csv:
        csv_file = open(output_csv, 'w', newline='')
        csv_writer = csv.writer(csv_file)
        csv_writer.writerow(['sound_id', 'timestamp', 'time_taken', 'success'])
    
    try:
        for i, sound_id in enumerate(sound_ids):
            if shutting_down:
                logger.info(f"Shutdown requested, stopping after processing {i} sounds")
                break
                
            logger.info(f"Processing sound {i+1}/{len(sound_ids)}: {sound_id}")
            result, time_taken = await sound_info(sound_id, output_dir=output_dir)
            success = result is not None
            timestamp = datetime.now().isoformat()
            
            results.append((sound_id, time_taken, success))
            
            # Write to CSV if enabled
            if csv_writer:
                csv_writer.writerow([sound_id, timestamp, time_taken, success])
                csv_file.flush()  # Ensure data is written immediately
        
        total_time = time.time() - total_start_time
        processed_count = len(results)
        
        if processed_count > 0:
            logger.info(f"\nProcessed {processed_count} sounds in {total_time:.2f} seconds")
            logger.info(f"Average time per sound: {total_time/processed_count:.2f} seconds")
            
            # Print summary of results
            success_count = sum(1 for _, _, success in results if success)
            success_rate = (success_count/processed_count*100) if processed_count > 0 else 0
            logger.info(f"Success rate: {success_count}/{processed_count} ({success_rate:.1f}%)")
        
        return results
    finally:
        if csv_file:
            csv_file.close()

def load_sound_ids_from_file(filename):
    """Load sound IDs from a file"""
    sound_ids = []
    with open(filename, "r") as f:
        for line in f:
            sound_id = line.strip()
            if sound_id:
                sound_ids.append(sound_id)
    return sound_ids

async def main():
    """Main function that handles both the API call and cleanup"""
    global api_instance, shutting_down
    
    parser = argparse.ArgumentParser(description="Retrieve TikTok sound information efficiently")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("sound_id", nargs="?", help="A single sound ID to process")
    group.add_argument("--batch", type=int, help="Process N sounds from sounds_ids.csv")
    group.add_argument("--file", nargs=2, metavar=("FILENAME", "COUNT"), help="Process N sounds from the specified file")
    
    parser.add_argument("--output-dir", default="sound_data", help="Directory to save sound data (default: sound_data)")
    parser.add_argument("--retry", type=int, default=1, help="Number of retries for failed requests (default: 1)")
    
    args = parser.parse_args()
    
    try:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_csv = f"sound_results_{timestamp}.csv"
        
        if args.batch:
            # Process sounds from the default CSV file
            sound_ids = load_sound_ids_from_file("sounds_ids.csv")
            await process_multiple_sounds(sound_ids, args.batch, output_csv, args.output_dir)
        elif args.file:
            # Process sounds from a specific file
            filename, count = args.file
            sound_ids = load_sound_ids_from_file(filename)
            await process_multiple_sounds(sound_ids, int(count), output_csv, args.output_dir)
        elif args.sound_id:
            # Process a single sound ID
            result, time_taken = await sound_info(args.sound_id, args.retry, args.output_dir)
            logger.info(f"Total time: {time_taken:.2f} seconds")
        else:
            # Default sound ID if none provided
            sound_id = "10104523"
            result, time_taken = await sound_info(sound_id, args.retry, args.output_dir)
            logger.info(f"Total time: {time_taken:.2f} seconds")
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
    finally:
        # Clean up resources
        if api_instance:
            try:
                logger.info("Cleaning up resources...")
                await api_instance.close_sessions()
                await api_instance.stop_playwright()
                logger.info("Cleanup completed")
            except Exception as e:
                logger.error(f"Error during cleanup: {str(e)}")

if __name__ == "__main__":
    asyncio.run(main()) 