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
type checksumFile struct {
	fd *os.File
}

func NewChecksumFile(fileName string) (*checksumFile, error) {
	fd, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, fmt.Errorf("NewChecksumFile: Cannot open file %v, err=%v", fileName, err)
	}

	return &checksumFile{fd: fd}, nil
}

func (csf *checksumFile) Close() error {
	return csf.fd.Close()
}

// This is not thread-safe.
func (csf *checksumFile) AppendChunk(data []byte) error {
	if len(data) == 0 {
		return nil
	}

	if csf.fd == nil {
		return fmt.Errorf("checksumFile.AppendChunk: File descriptor is nil")
	}

	_, err := csf.fd.Seek(0, 2) // Seek to the end of the file
	if err != nil {
		return fmt.Errorf("checksumFile.AppendChunk: Cannot seek to end of file %v, err=%v", csf.fd.Name(), err)
	}

	_, err = csf.fd.Write(Uint32ToBytesLittleEndian(magicNumber))
	if err != nil {
		return fmt.Errorf("checksumFile.AppendChunk: Cannot write magic number to file %v, err=%v", csf.fd.Name(), err)
	}

	checksum := crc32.ChecksumIEEE(data)
	_, err = csf.fd.Write(Uint32ToBytesLittleEndian(checksum))
	if err != nil {
		return fmt.Errorf("checksumFile.AppendChunk: Cannot write checksum to file %v, err=%v", csf.fd.Name(), err)
	}

	_, err = csf.fd.Write(Uint32ToBytesLittleEndian(uint32(len(data))))
	if err != nil {
		return fmt.Errorf("checksumFile.AppendChunk: Cannot write length to file %v, err=%v", csf.fd.Name(), err)
	}

	_, err = csf.fd.Write(data)
	if err != nil {
		return fmt.Errorf("checksumFile.AppendChunk: Cannot write data to file %v, err=%v", csf.fd.Name(), err)
	}

	return nil
}

func (csf *checksumFile) ReadAt(buf []byte, offset int64) (int, error) {
	if csf.fd == nil {
		return 0, fmt.Errorf("checksumFile.ReadAt: File descriptor is nil")
	}

	_, err := csf.fd.Seek(offset, 0)
	if err != nil {
		return 0, fmt.Errorf("checksumFile.ReadAt: Cannot seek to offset %d in file %v, err=%v",
			offset, csf.fd.Name(), err)
	}

	if magic, err := readUint32(csf.fd); err != nil {
		return 0, fmt.Errorf("checksumFile.ReadAt: Cannot read magic number from file %v, err=%v",
			csf.fd.Name(), err)
	} else if magic != magicNumber {
		// Check if this is a checksum file. If it is, it will have the magic
		// number at the start.
		_, err := csf.fd.Seek(0, 0) // Seek to the start of the file.
		if err != nil {
			return 0, fmt.Errorf("checksumFile.ReadAt: Cannot seek to start of file %v, err=%v",
				csf.fd.Name(), err)
		}

		if magic, err := readUint32(csf.fd); err != nil {
			return 0, fmt.Errorf("checksumFile.ReadAt: Cannot read magic number from start of file %v, err=%v",
				csf.fd.Name(), err)
		} else if magic == magicNumber {
			return 0, fmt.Errorf("checksumFile.ReadAt: offset is not the start of a chunk")
		}

		// It's not a checksum file, so read the data directly for backward compatibility.
		return csf.fd.ReadAt(buf, offset)
	}

	checksum, err := readUint32(csf.fd)
	if err != nil {
		return 0, fmt.Errorf("checksumFile.ReadAt: Cannot read checksum from file %v, err=%v",
			csf.fd.Name(), err)
	}

	length, err := readUint32(csf.fd)
	if err != nil {
		return 0, fmt.Errorf("checksumFile.ReadAt: Cannot read length from file %v, err=%v",
			csf.fd.Name(), err)
	}

	if length != uint32(len(buf)) {
		// TODO: Handle this case
		return 0, fmt.Errorf("checksumFile.ReadAt: buffer length mismatch: expected %d, got %d", length, len(buf))
	}

	numBytesRead, err := csf.fd.Read(buf)
	if err != nil && err != io.EOF {
		return 0, fmt.Errorf("checksumFile.ReadAt: Cannot read data from file %v, err=%v",
			csf.fd.Name(), err)
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
