import subprocess
import sys


def ingest(filename, batch_size=100):
    # Determine the total number of lines in the file
    total_lines = sum(1 for _ in open(filename, "r"))

    lines = []
    with open(filename, 'r') as f:
        for i, line in enumerate(f):
            lines.append(line)

            if len(lines) >= batch_size:
                print(f"\rProcessing... {((i + 1) / total_lines) * 100:.2f}%", end='')
                ingest_lines(lines)
                lines = []
    if lines:
        ingest_lines(lines)
        print(f"\rProcessing... 100.00%")


def ingest_lines(lines):
    index_data = '{"index": {"_index": "trips", "_type": "_doc"}}'
    data = ''
    for line in lines:
        data += index_data + '\n' + line

    # Prepare the curl command
    curl_command = [
        "curl",
        "-s",
        "-o", "/dev/null",
        "http://localhost:8081/elastic/_bulk",
        "-X", "POST",
        "-H", "Authorization: Bearer ",
        "-H", "Content-Type: application/json",
        "--data-binary", data
    ]

    # Execute the curl command
    process = subprocess.run(curl_command, capture_output=False, text=False)
    if process.stderr:
        print("Error:", process.stderr)


if __name__ == "__main__":
    ingest(sys.argv[1])

