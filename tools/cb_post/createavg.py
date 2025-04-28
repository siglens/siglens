import json

def calculate_averages(input_file, output_file):
    averages = []

    with open(input_file, 'r') as file:
        for line in file:

            entry = line.strip().rstrip(",")
            if len(entry) == 0 :
                continue
            if "null" in entry :
                averages.append(-1)
                continue

            # Strip whitespace and parse the line as a JSON array
            row = json.loads(entry)
            # Calculate the average for the row
            avg = sum(row) / len(row)
            averages.append(avg)

    # Write the averages to the output file
    with open(output_file, 'w') as file:
        for avg in averages:
            if avg == -1 :
                file.write(f"null\n")
            else:
                file.write(f"{avg:.3f}\n")

    print(f"Averages written to {output_file}")

# File paths
## input file format, each line contains responses for N tries for the given query matching with queries.spl:
#  [n1, n2. n3]
#  [n1, n2. n3]
input_file = "input.csv"  # Replace with your input file path
output_file = "out.csv"  # Replace with your desired output file path

# Calculate and write averages
calculate_averages(input_file, output_file)
