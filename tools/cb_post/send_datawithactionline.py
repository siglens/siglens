import json
import requests
import os
import time
from concurrent.futures import ThreadPoolExecutor

# Path to hits.json file
ndjson_file = "hits.json"

# Target URL for HTTP POST request
url = "http://localhost:8081/elastic/_bulk"

# index string
index_string = '{ "index" : { "_index" : "hits" } }\n'

# Size of buffer before sending
buffer_limit = 10 * 1024 * 1024  # 10MB

# Number of concurrent threads
max_workers = 8

def post_data(buffer):
    """Function to send HTTP POST request with buffer as payload."""
    try:
        response = requests.post(url, data=buffer)
        if response.status_code != 200:
            print(f"Failed to post data. Status code: {response.status_code}")
    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")

def process_ndjson(file_path):
    buffer = ""
    buffer_size = 0

    start_time = time.time()

    # Create a thread pool
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        with open(file_path, "r") as file:
            sent = 0
            for line in file:
                # Parse the JSON line
                entry = json.loads(line)

                # Convert "UserID" to integer (bigint)
                if "UserID" in entry:
                    entry["UserID"] = int(entry["UserID"])

                # Add the hardcoded index string, followed by the JSON line
                json_line = json.dumps(entry)
                buffer += index_string + json_line + "\n"
                buffer_size += len(index_string) + len(json_line) + 1
                sent += 1

                # Check if buffer exceeds the limit
                if buffer_size >= buffer_limit:
                    # Submit the post task to a thread
                    futures.append(executor.submit(post_data, buffer))
                    # Reset buffer
                    buffer = ""
                    buffer_size = 0

                if sent % 1_000_000 == 0:
                    elapsed_time = time.time() - start_time
                    print(f"Sent : {sent:,} records in {elapsed_time:.2f} seconds")

            # Post any remaining data
            if buffer:
                futures.append(executor.submit(post_data, buffer))

        # Wait for all posts to finish
        for future in futures:
            future.result()

        elapsed_time = time.time() - start_time
        print(f"\nTotal Sent : {sent:,} records in {elapsed_time:.2f} seconds")

if __name__ == "__main__":
    process_ndjson(ndjson_file)
