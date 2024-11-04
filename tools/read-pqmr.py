#!/usr/bin/env python3
import struct
from collections import Counter
from typing import Tuple, List, Dict

def read_chunk(file, offset: int) -> Tuple[int, int, bytes]:
    """
    Read a single chunk from the file starting at given offset.
    Returns (block_num, bitset_size, bitset) or raises EOFError if no more chunks.
    """
    # Read block number (16-bit little-endian)
    file.seek(offset)
    block_data = file.read(2)
    if not block_data or len(block_data) < 2:
        raise EOFError("End of file reached")
    block_num = struct.unpack('<H', block_data)[0]
    
    # Read bitset size (16-bit little-endian)
    bitset_size_data = file.read(2)
    if not bitset_size_data or len(bitset_size_data) < 2:
        raise EOFError("Incomplete chunk: missing bitset size")
    bitset_size = struct.unpack('<H', bitset_size_data)[0]
    
    # Read the bitset
    bitset = file.read(bitset_size)
    if len(bitset) < bitset_size:
        raise EOFError("Incomplete chunk: truncated bitset")
        
    return block_num, bitset_size, bitset

def parse_bitset(data):
    # Ensure the data is at least 8 bytes long to read the length
    if len(data) < 8:
        raise ValueError("Data is too short to contain a valid BitSet")

    # Read the first 8 bytes to get the length (number of bits)
    length = struct.unpack('>Q', data[:8])[0]

    # Calculate the number of 64-bit words needed
    num_words = (length + 63) // 64

    # Ensure the data contains enough bytes for the bitset
    expected_length = 8 + num_words * 8
    if len(data) < expected_length:
        raise ValueError("Data is too short to contain the expected number of words")

    # Read the bitset words
    bitset = []
    for i in range(num_words):
        start = 8 + i * 8
        end = start + 8
        word = struct.unpack('>Q', data[start:end])[0]
        bitset.append(word)

    return length, bitset

def count_bits_from_bitset(bitset: bytes) -> Tuple[int, int]:
    """
    Parse the bitset format and count the bits.
    Returns (count of 1s, total bits)
    """
    length, words = parse_bitset(bitset)
    
    # Count bits set to 1 in each 64-bit word
    count = sum(bin(word).count('1') for word in words)
    
    return count, length

def analyze_binary_file(filename: str) -> None:
    """
    Analyze the binary file containing chunks of [block num][bitset size][bitset].
    """
    chunks: List[Tuple[int, int, int]] = []  # [(block_num, matched_records, total_records), ...]
    block_numbers: Dict[int, int] = {}  # block_num -> count of occurrences
    
    try:
        with open(filename, 'rb') as f:
            offset = 0
            while True:
                try:
                    block_num, bitset_size, bitset = read_chunk(f, offset)
                    matched_records, total_records = count_bits_from_bitset(bitset)
                    chunks.append((block_num, matched_records, total_records))
                    
                    # Track block number occurrences
                    block_numbers[block_num] = block_numbers.get(block_num, 0) + 1
                    
                    # Update offset for next chunk
                    offset += 2 + 2 + bitset_size
                    
                except EOFError:
                    break
                
    except FileNotFoundError:
        print(f"Error: File '{filename}' not found")
        return
    except Exception as e:
        print(f"Error processing file: {e}")
        return

    # Print results
    print(f"Total chunks found: {len(chunks)}")
    
    # Check for duplicates
    duplicates = {block: count for block, count in block_numbers.items() if count > 1}
    if duplicates:
        print("\nDuplicate block numbers found:")
        for block, count in sorted(duplicates.items()):
            print(f"  Block {block} appears {count} times")
    else:
        print("\nNo duplicate block numbers found")
    
    print("\nChunk Analysis:")
    for i, (block_num, matched, total) in enumerate(chunks, 1):
        print(f"Chunk {i}:")
        print(f"  Block number: {block_num}")
        print(f"  Records matched: {matched} of {total}")

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) != 2:
        print(f"Usage: python {sys.argv[0]} <filename>")
        sys.exit(1)
    
    analyze_binary_file(sys.argv[1])
