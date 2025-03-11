from TikTokApi import TikTokApi
import asyncio
import os
import json
import sys
import time
import csv
from datetime import datetime

# Get ms_token from environment variable or use a known working one
ms_token = os.environ.get("ms_token", None)  # set your own ms_token

# Global API instance to reuse across calls
api_instance = None
# Track when we last made a request to avoid rate limiting
last_request_time = 0
# Minimum delay between requests (in seconds)
MIN_REQUEST_DELAY = 0.1

async def initialize_api():
    """Initialize the API once and reuse it"""
    global api_instance
    if api_instance is None:
        print("Initializing API and creating session...")
        start_time = time.time()
        
        api_instance = TikTokApi()
        
        # Create only one session and reuse it
        # Disable resource types that aren't needed for API calls
        suppress_resources = ["image", "media", "font", "stylesheet"]
        
        await api_instance.create_sessions(
            ms_tokens=[ms_token] if ms_token else None,
            num_sessions=1,
            sleep_after=1,  # Reduced sleep time
            browser=os.getenv("TIKTOK_BROWSER", "chromium"),
            suppress_resource_load_types=suppress_resources,
            # Use headless mode with the new headless flag format
            headless=True,
            override_browser_args=["--headless=new"],
            # Reduce timeout for faster failure if something goes wrong
            timeout=15000
        )
        
        elapsed = time.time() - start_time
        print(f"API initialization completed in {elapsed:.2f} seconds")
    
    return api_instance

async def sound_info(sound_id, retry_count=1):
    """Get sound info for a specific sound ID with retry logic"""
    global last_request_time
    start_time = time.time()
    
    # Add a small delay between requests to avoid rate limiting
    elapsed_since_last = start_time - last_request_time
    if last_request_time > 0 and elapsed_since_last < MIN_REQUEST_DELAY:
        delay = MIN_REQUEST_DELAY - elapsed_since_last
        await asyncio.sleep(delay)
    
    attempts = 0
    while attempts <= retry_count:
        try:
            api = await initialize_api()
            sound = api.sound(id=sound_id)
            sound_details = await sound.info()
            
            elapsed = time.time() - start_time
            last_request_time = time.time()
            
            print(f"Retrieved sound info for {sound_id} in {elapsed:.2f} seconds")
            
            # Save to file with sound ID in filename
            with open(f"sound_info_{sound_id}.json", "w") as f:
                json.dump(sound_details, f, indent=4)
                
            return sound_details, elapsed
        
        except Exception as e:
            attempts += 1
            if attempts <= retry_count:
                retry_delay = 1 * attempts
                print(f"Error retrieving sound {sound_id}: {str(e)}. Retrying in {retry_delay}s... (Attempt {attempts}/{retry_count})")
                await asyncio.sleep(retry_delay)
            else:
                elapsed = time.time() - start_time
                print(f"Failed to retrieve sound {sound_id} after {retry_count} retries: {str(e)} in {elapsed:.2f} seconds")
                return None, elapsed

async def process_multiple_sounds(sound_ids, max_sounds=None, output_csv=None):
    """Process multiple sound IDs sequentially, reusing the same session"""
    results = []
    total_start_time = time.time()
    
    # Limit the number of sounds to process if specified
    if max_sounds:
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
            print(f"Processing sound {i+1}/{len(sound_ids)}: {sound_id}")
            result, time_taken = await sound_info(sound_id)
            success = result is not None
            timestamp = datetime.now().isoformat()
            
            results.append((sound_id, time_taken, success))
            
            # Write to CSV if enabled
            if csv_writer:
                csv_writer.writerow([sound_id, timestamp, time_taken, success])
                csv_file.flush()  # Ensure data is written immediately
        
        total_time = time.time() - total_start_time
        print(f"\nProcessed {len(sound_ids)} sounds in {total_time:.2f} seconds")
        print(f"Average time per sound: {total_time/len(sound_ids):.2f} seconds")
        
        # Print summary of results
        success_count = sum(1 for _, _, success in results if success)
        print(f"Success rate: {success_count}/{len(results)} ({success_count/len(results)*100:.1f}%)")
        
        return results
    finally:
        if csv_file:
            csv_file.close()

async def main():
    """Main function that handles both the API call and cleanup"""
    global api_instance
    try:
        # Check if we're processing a single sound ID or multiple from the CSV
        if len(sys.argv) > 1:
            if sys.argv[1] == "--batch":
                # Process sounds from the CSV file
                sound_ids = []
                with open("sounds_ids.csv", "r") as f:
                    for line in f:
                        sound_id = line.strip()
                        if sound_id:
                            sound_ids.append(sound_id)
                
                # Determine how many sounds to process
                max_sounds = int(sys.argv[2]) if len(sys.argv) > 2 else 5
                
                # Create output CSV filename with timestamp
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                output_csv = f"sound_results_{timestamp}.csv"
                
                await process_multiple_sounds(sound_ids, max_sounds, output_csv)
            else:
                # Process a single sound ID
                sound_id = sys.argv[1]
                result, time_taken = await sound_info(sound_id)
                print(f"Total time: {time_taken:.2f} seconds")
        else:
            # Default sound ID if none provided
            sound_id = "10104523"
            result, time_taken = await sound_info(sound_id)
            print(f"Total time: {time_taken:.2f} seconds")
    finally:
        # Clean up resources
        if api_instance:
            try:
                await api_instance.close_sessions()
                await api_instance.stop_playwright()
            except Exception as e:
                print(f"Error during cleanup: {str(e)}")

if __name__ == "__main__":
    asyncio.run(main())
