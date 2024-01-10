import pandas as pd
import glob
import os
import sys


def convert_parquet_to_tsv(input_directory, output_directory):
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)

    for parquet_file in glob.glob(os.path.join(input_directory, '*.parquet')):
        df = pd.read_parquet(parquet_file)
        base_name = os.path.basename(parquet_file)
        tsv_file = os.path.join(output_directory, base_name.replace('.parquet', '.tsv'))
        df.to_csv(tsv_file, index=False)
        print(f"Converted {parquet_file} to {tsv_file}")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python script.py <input_directory> <output_directory>")
        sys.exit(1)

    input_dir = sys.argv[1]
    output_dir = sys.argv[2]
    convert_parquet_to_tsv(input_dir, output_dir)
