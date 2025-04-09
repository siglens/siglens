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

const magicNumber uint32 = 0x87654321

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
	chunkOffset int64 // Start offset of the chunk (so the offset of the magic number).
	checksum    uint32
	curChunkLen int
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

	_, err = csf.Fd.Write(Uint32ToBytesLittleEndian(magicNumber))
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
		_, err = csf.Fd.Write(make([]byte, 12)) // magic, checksum, and length
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

	// Go to the beginning of this chunk.
	_, err := csf.Fd.Seek(csf.chunkOffset, 0)
	if err != nil {
		return fmt.Errorf("checksumFile.Flush: Cannot seek to chunk offset %d in file %v, err=%v",
			csf.chunkOffset, csf.Fd.Name(), err)
	}

	_, err = csf.Fd.Write(Uint32ToBytesLittleEndian(magicNumber))
	if err != nil {
		return fmt.Errorf("checksumFile.AppendChunk: Cannot write magic number to file %v, err=%v", csf.Fd.Name(), err)
	}

	_, err = csf.Fd.Write(Uint32ToBytesLittleEndian(csf.checksum))
	if err != nil {
		return fmt.Errorf("checksumFile.AppendChunk: Cannot write checksum to file %v, err=%v", csf.Fd.Name(), err)
	}

	_, err = csf.Fd.Write(Uint32ToBytesLittleEndian(uint32(csf.curChunkLen)))
	if err != nil {
		return fmt.Errorf("checksumFile.AppendChunk: Cannot write length to file %v, err=%v", csf.Fd.Name(), err)
	}

	csf.chunkOffset = 0
	csf.curChunkLen = 0
	csf.checksum = 0

	return nil
}

func (csf *ChecksumFile) ReadAt(buf []byte, offset int64) (int, error) {
	if csf.Fd == nil {
		return 0, fmt.Errorf("checksumFile.ReadAt: File descriptor is nil")
	}

	_, err := csf.Fd.Seek(offset, 0)
	if err != nil {
		return 0, fmt.Errorf("checksumFile.ReadAt: Cannot seek to offset %d in file %v, err=%v",
			offset, csf.Fd.Name(), err)
	}

	magic, err := readUint32(csf.Fd)
	if err != nil {
		return 0, fmt.Errorf("checksumFile.ReadAt: Cannot read magic number from file %v, err=%v",
			csf.Fd.Name(), err)
	}

	if magic != magicNumber {
		// Check if this is a checksum file. If it is, it will have the magic
		// number at the start.
		_, err := csf.Fd.Seek(0, 0) // Seek to the start of the file.
		if err != nil {
			return 0, fmt.Errorf("checksumFile.ReadAt: Cannot seek to start of file %v, err=%v",
				csf.Fd.Name(), err)
		}

		if magic, err := readUint32(csf.Fd); err != nil {
			return 0, fmt.Errorf("checksumFile.ReadAt: Cannot read magic number from start of file %v, err=%v",
				csf.Fd.Name(), err)
		} else if magic == magicNumber {
			return 0, fmt.Errorf("checksumFile.ReadAt: offset is not the start of a chunk")
		}

		// It's not a checksum file, so read the data directly for backward compatibility.
		return csf.Fd.ReadAt(buf, offset)
	}

	checksum, err := readUint32(csf.Fd)
	if err != nil {
		return 0, fmt.Errorf("checksumFile.ReadAt: Cannot read checksum from file %v, err=%v",
			csf.Fd.Name(), err)
	}

	length, err := readUint32(csf.Fd)
	if err != nil {
		return 0, fmt.Errorf("checksumFile.ReadAt: Cannot read length from file %v, err=%v",
			csf.Fd.Name(), err)
	}

	if length != uint32(len(buf)) {
		// TODO: Handle this case
		return 0, fmt.Errorf("checksumFile.ReadAt: buffer length mismatch: expected %d, got %d", length, len(buf))
	}

	numBytesRead, err := csf.Fd.Read(buf)
	if err != nil && err != io.EOF {
		return 0, fmt.Errorf("checksumFile.ReadAt: Cannot read data from file %v, err=%v",
			csf.Fd.Name(), err)
	}

	// Verify the checksum
	if crc32.ChecksumIEEE(buf) != checksum {
		return 0, fmt.Errorf("checksumFile.ReadAt: checksum mismatch")
	}

	return numBytesRead, err
}

func readUint32(fd *os.File) (uint32, error) {
	buf := make([]byte, 4)
	_, err := fd.Read(buf)
	if err != nil {
		return 0, err
	}
	return BytesToUint32LittleEndian(buf), nil
}
