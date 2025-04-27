from multiprocessing import Process
import json
import requests
import os
import time
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")
logger = logging.getLogger()

# Target URL for data ingestion service
url = "http://localhost:8081/elastic/_bulk"

buffer_limit = 10 * 1024 * 1024

def post_data(buffer):
    """Function to send HTTP POST request with buffer as payload."""
    try:
        response = requests.post(url, data=buffer)
        if response.status_code != 200:
            logger.error(f"Failed to post data. Status code: {response}")
    except requests.exceptions.RequestException as e:
        logger.error(f"Request failed: {e}")

def process_ndjson(file_path):
    buffer = ""
    buffer_size = 0

    start_time = time.time()

    with open(file_path, "r") as file:
        sent = 0
        i = 0
        for line in file:

            buffer += line
            buffer_size += len(line)
            i += 1
            if i%2 != 0:
                continue
            sent += 1

            # Check if buffer exceeds the limit
            if buffer_size >= buffer_limit:
                post_data(buffer)
                # Reset buffer and buffer size
                buffer = ""
                buffer_size = 0
            if sent%1_000_000 == 0 :
                elapsed_time = time.time() - start_time
                logger.info(f"Sent : {sent:,} records in {elapsed_time:.2f} seconds")

        # Post any remaining data in the buffer
        if buffer:
            post_data(buffer)

        elapsed_time = time.time() - start_time
        logger.info(f"\n Total Sent : {sent:,} records in {elapsed_time:.2f} seconds")

if __name__ == "__main__":
    # Get a list of all NDJSON files in the directory
    ndjson_files = [f for f in os.listdir("./") if f.startswith("splithits_")]

    running_procs = []
    for fname in ndjson_files:
        p = Process(target=process_ndjson, args=(fname,))
        running_procs.append(p)
        p.start()

    for t in running_procs:
        t.join()
