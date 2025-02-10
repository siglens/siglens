import json
import requests
import os
import time

# Path to hits.json file
ndjson_file = "hits.json"

out_file = "sighits.json"

# index string
index_string = '{ "index" : { "_index" : "hits" } }\n'

buffer_limit = 10 * 1024 * 1024

def process_ndjson(file_path):
    buffer = ""
    buffer_size = 0

    start_time = time.time()

    out_fd = open(out_file, "w")

    with open(file_path, "r") as file:
        created = 0
        for line in file:
            # Parse the JSON line
            entry = json.loads(line)

            # Convert "UserID" to integer (bigint)
            if "UserID" in entry:
                entry["UserID"] = int(entry["UserID"])

            # Add the hardcoded index string, followed by the JSON line
            buffer += index_string
            buffer += json.dumps(entry) + "\n"
            buffer_size += len(index_string) + len(json.dumps(entry)) + 1
            created += 1

            # Check if buffer exceeds the limit
            if buffer_size >= buffer_limit:
                out_fd.write(buffer)
                # Reset buffer and buffer size
                buffer = ""
                buffer_size = 0
            if created%1_000_000 == 0 :
                elapsed_time = time.time() - start_time
                print(f"Created : {created:,} records in {elapsed_time:.2f} seconds")

        # Post any remaining data in the buffer
        if buffer:
            out_fd.write(buffer)

        elapsed_time = time.time() - start_time
        print(f"\n Total Created : {created:,} records in {elapsed_time:.2f} seconds")
    out_fd.close()

if __name__ == "__main__":
    process_ndjson(ndjson_file)
