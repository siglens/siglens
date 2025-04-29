// Copyright (c) 2021-2025 SigScalr, Inc.
//
// # This file is part of SigLens Observability Solution
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package utils

import (
	"fmt"
	"hash/crc32"
	"io"
	"os"
)

const MagicNumber uint32 = 0x87654321
const checksumOffset = 4
const lengthOffset = 8
const dataOffset = 12

// This is used to read and write files in chunks that are checksummed. It's
// also backward compatible, so it can read files that were not written this
// way. In the new format, the file consists of chunks; the layout of each
// chunk is:
//   - magic number (4 bytes)
//   - checksum (4 bytes)
//   - length (4 bytes)
//   - data (length bytes)
type ChecksumFile struct {
	Fd *os.File

	// These are used for partial chunk writes.
	chunkOffset   int64 // Start offset of the chunk (so the offset of the magic number).
	checksum      uint32
	curChunkLen   int
	curCSFMetaLen int64
}

// This is not thread-safe.
func (csf *ChecksumFile) AppendChunk(data []byte) error {
	if len(data) == 0 {
		return nil
	}

	if csf.Fd == nil {
		return fmt.Errorf("checksumFile.AppendChunk: File descriptor is nil")
	}

	if csf.curChunkLen != 0 {
		return fmt.Errorf("checksumFile.AppendChunk: Last chunk is not flushed")
	}

	_, err := csf.Fd.Seek(0, 2) // Seek to the end of the file
	if err != nil {
		return fmt.Errorf("checksumFile.AppendChunk: Cannot seek to end of file %v, err=%v", csf.Fd.Name(), err)
	}

	_, err = csf.Fd.Write(Uint32ToBytesLittleEndian(MagicNumber))
	if err != nil {
		return fmt.Errorf("checksumFile.AppendChunk: Cannot write magic number to file %v, err=%v", csf.Fd.Name(), err)
	}

	checksum := crc32.ChecksumIEEE(data)
	_, err = csf.Fd.Write(Uint32ToBytesLittleEndian(checksum))
	if err != nil {
		return fmt.Errorf("checksumFile.AppendChunk: Cannot write checksum to file %v, err=%v", csf.Fd.Name(), err)
	}

	_, err = csf.Fd.Write(Uint32ToBytesLittleEndian(uint32(len(data))))
	if err != nil {
		return fmt.Errorf("checksumFile.AppendChunk: Cannot write length to file %v, err=%v", csf.Fd.Name(), err)
	}

	_, err = csf.Fd.Write(data)
	if err != nil {
		return fmt.Errorf("checksumFile.AppendChunk: Cannot write data to file %v, err=%v", csf.Fd.Name(), err)
	}

	return nil
}

// Use this instead of AppendChunk() if you want to have multiple write calls
// but combine them into one chunk. You MUST call Flush() to finish the chunk.
func (csf *ChecksumFile) AppendPartialChunk(data []byte) error {
	if len(data) == 0 {
		return nil
	}

	if csf.Fd == nil {
		return fmt.Errorf("checksumFile.AppendPartialChunk: File descriptor is nil")
	}

	offset, err := csf.Fd.Seek(0, 2) // Seek to the end of the file
	if err != nil {
		return fmt.Errorf("checksumFile.AppendPartialChunk: Cannot seek to end of file %v, err=%v", csf.Fd.Name(), err)
	}

	if csf.curChunkLen == 0 {
		// We want to write the data now, but we don't know what checksum or
		// length to write. So skip some bytes, and we'll write them later.
		csf.chunkOffset = offset
		_, err = csf.Fd.Write(make([]byte, dataOffset))
		if err != nil {
			return fmt.Errorf("checksumFile.AppendPartialChunk: Cannot write placeholders to file %v, err=%v",
				csf.Fd.Name(), err)
		}
	}

	csf.checksum = crc32.Update(csf.checksum, crc32.IEEETable, data)
	csf.curChunkLen += len(data)

	_, err = csf.Fd.Write(data)
	if err != nil {
		return fmt.Errorf("checksumFile.AppendPartialChunk: Cannot write data to file %v, err=%v", csf.Fd.Name(), err)
	}

	return nil
}

func (csf *ChecksumFile) Flush() error {
	if csf.Fd == nil {
		return fmt.Errorf("checksumFile.Flush: File descriptor is nil")
	}

	_, err := csf.Fd.WriteAt(Uint32ToBytesLittleEndian(MagicNumber), csf.chunkOffset)
	if err != nil {
		return fmt.Errorf("checksumFile.Flush: Cannot write magic number to file %v, err=%v", csf.Fd.Name(), err)
	}

	_, err = csf.Fd.WriteAt(Uint32ToBytesLittleEndian(csf.checksum), csf.chunkOffset+checksumOffset)
	if err != nil {
		return fmt.Errorf("checksumFile.Flush: Cannot write checksum to file %v, err=%v", csf.Fd.Name(), err)
	}

	_, err = csf.Fd.WriteAt(Uint32ToBytesLittleEndian(uint32(csf.curChunkLen)), csf.chunkOffset+lengthOffset)
	if err != nil {
		return fmt.Errorf("checksumFile.Flush: Cannot write length to file %v, err=%v", csf.Fd.Name(), err)
	}

	csf.chunkOffset = 0
	csf.curChunkLen = 0
	csf.checksum = 0

	return nil
}

// It's safe to call this concurrently at different offsets.
func (csf *ChecksumFile) ReadAt(buf []byte, offset int64) (int, error) {
	if csf.Fd == nil {
		return 0, fmt.Errorf("checksumFile.ReadAt: File descriptor is nil")
	}

	totalBytesRead := 0
	for i := 0; ; i++ {
		numBytesRead, err := csf.readChunkAt(buf[totalBytesRead:], offset+int64(totalBytesRead+i*dataOffset))
		totalBytesRead += numBytesRead
		if err != nil {
			return totalBytesRead, err
		}

		if totalBytesRead >= len(buf) {
			return totalBytesRead, nil
		}
	}
}

func (csf *ChecksumFile) readChunkAt(buf []byte, offset int64) (int, error) {
	magic, err := readUint32At(csf.Fd, offset)
	if err != nil {
		return 0, fmt.Errorf("checksumFile.readChunkAt: Cannot read magic number from file %v, err=%v",
			csf.Fd.Name(), err)
	}

	if magic != MagicNumber {
		if magic, err := readUint32At(csf.Fd, 0); err != nil {
			return 0, fmt.Errorf("checksumFile.readChunkAt: Cannot read magic number from start of file %v, err=%v",
				csf.Fd.Name(), err)
		} else if magic == MagicNumber {
			return 0, fmt.Errorf("checksumFile.readChunkAt: offset is not the start of a chunk")
		}

		// It's not a checksum file, so read the data directly for backward compatibility.
		return csf.Fd.ReadAt(buf, offset)
	}

	checksum, err := readUint32At(csf.Fd, offset+checksumOffset)
	if err != nil {
		return 0, fmt.Errorf("checksumFile.readChunkAt: Cannot read checksum from file %v at offset %v, err=%v",
			csf.Fd.Name(), offset, err)
	}

	length, err := readUint32At(csf.Fd, offset+lengthOffset)
	if err != nil {
		return 0, fmt.Errorf("checksumFile.readChunkAt: Cannot read length from file %v, err=%v",
			csf.Fd.Name(), err)
	}

	if length > uint32(len(buf)) {
		// TODO: Handle this case
		return 0, fmt.Errorf("checksumFile.readChunkAt: buffer length mismatch: expected %d, got %d", length, len(buf))
	}

	numBytesToRead := min(int(length), len(buf))
	numBytesRead, err := csf.Fd.ReadAt(buf[:numBytesToRead], offset+dataOffset)
	if err != nil && err != io.EOF {
		return 0, fmt.Errorf("checksumFile.readChunkAt: Cannot read data from file %v, err=%v",
			csf.Fd.Name(), err)
	}

	// Verify the checksum
	if crc32.ChecksumIEEE(buf[:numBytesRead]) != checksum {
		return 0, fmt.Errorf("checksumFile.readChunkAt: checksum mismatch")
	}

	return numBytesRead, err
}

func readUint32At(fd *os.File, offset int64) (uint32, error) {
	buf := make([]byte, 4)
	_, err := fd.ReadAt(buf, offset)
	if err != nil {
		return 0, err
	}
	return BytesToUint32LittleEndian(buf), nil
}

// We will remove this method after all files have read data from the checksum files.
func (csf *ChecksumFile) ReadBlock(buf []byte, offset int64) (int, error) {
	magic, err := readUint32At(csf.Fd, csf.curCSFMetaLen+offset)
	if err != nil {
		if err == io.EOF {
			return 0, err
		}
		return 0, fmt.Errorf("checksumFile.ReadBlock: Cannot read magic number from file %v, err=%v", csf.Fd.Name(), err)
	}

	if magic != MagicNumber {
		if magic, err := readUint32At(csf.Fd, 0); err != nil {
			return 0, fmt.Errorf("checksumFile.ReadBlock: Cannot read magic number from start of file %v, err=%v",
				csf.Fd.Name(), err)
		} else if magic == MagicNumber {
			return 0, fmt.Errorf("checksumFile.ReadBlock: offset is not the start of a chunk")
		}

		// It's not a checksum file, so read the data directly for backward compatibility.
		return csf.Fd.ReadAt(buf, offset)
	}

	checksum, err := readUint32At(csf.Fd, csf.curCSFMetaLen+offset+checksumOffset)
	if err != nil {
		return 0, fmt.Errorf("checksumFile.ReadBlock: Cannot read checksum from file %v at offset %v, err=%v",
			csf.Fd.Name(), offset, err)
	}

	length, err := readUint32At(csf.Fd, csf.curCSFMetaLen+offset+lengthOffset)
	if err != nil {
		return 0, fmt.Errorf("checksumFile.ReadBlock: Cannot read length from file %v, err=%v",
			csf.Fd.Name(), err)
	}

	if length > uint32(len(buf)) {
		// TODO: Handle this case
		return 0, fmt.Errorf("checksumFile.ReadBlock: buffer length mismatch: expected %d, got %d", length, len(buf))
	}

	numBytesToRead := min(int(length), len(buf))
	buf = ResizeSlice(buf, numBytesToRead)
	numBytesRead, err := csf.Fd.ReadAt(buf[:numBytesToRead], csf.curCSFMetaLen+offset+dataOffset)
	if err != nil && err != io.EOF {
		return 0, fmt.Errorf("checksumFile.ReadBlock: Cannot read data from file %v, err=%v",
			csf.Fd.Name(), err)
	}

	// Verify the checksum
	if crc32.ChecksumIEEE(buf[:numBytesRead]) != checksum {
		return 0, fmt.Errorf("checksumFile.ReadBlock: checksum mismatch")
	}

	csf.curCSFMetaLen += dataOffset
	return numBytesRead, err
}
