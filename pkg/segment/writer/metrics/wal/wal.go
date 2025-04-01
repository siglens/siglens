package wal

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"

	segutils "github.com/siglens/siglens/pkg/segment/utils"
	"github.com/siglens/siglens/pkg/utils"

	"github.com/klauspost/compress/zstd"
	toputils "github.com/siglens/siglens/pkg/utils"
	log "github.com/sirupsen/logrus"
)

type WalDatapoint struct {
	Timestamp uint32
	DpVal     float64
	Tsid      uint64
}

type WAL struct {
	fd            *os.File
	fNameWithPath string
	totalDps      uint32
	encodedSize   uint64
	encodedBuf    []byte
	rawBlockBuf   *bytes.Buffer
	checksumBuf   []byte
}

var encoder, _ = zstd.NewWriter(nil)
var decoder, _ = zstd.NewReader(nil)
var encoderLock sync.Mutex

const UINT32_SIZE = 4

func NewWAL(filePath string) (*WAL, error) {
	dir := filepath.Dir(filePath)
	err := os.MkdirAll(dir, 0755)
	if err != nil {
		log.Errorf("NewWAL : Failed to create directories for path %s: %v", dir, err)
		return nil, err
	}

	f, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		log.Errorf("NewWAL : Failed to open WAL file at path %s: %v", filePath, err)
		return nil, err
	}

	_, err = f.Write(segutils.VERSION_WALFILE)
	if err != nil {
		log.Infof("NewWAL: Could not write version byte to file %v. Err %v", filePath, err)
		return nil, err
	}

	return &WAL{
		fNameWithPath: filePath,
		totalDps:      0,
		encodedSize:   0,
		fd:            f,
		rawBlockBuf:   &bytes.Buffer{},
		checksumBuf:   make([]byte, 4),
	}, nil
}

/*
This function appends a new block to the file.

File Format:
	Version:                1 Byte  // File format version

	// Repeating Blocks Structure
	[Block] {
		BlockLen:               4 Bytes  // Size of this block (excluding this field)
		Checksum:               4 Bytes  // CRC32 checksum for data integrity
		ZstdEncoded Block:
			NumOfDatapoints (N): 4 Bytes  // Number of datapoints in this block
			BinaryEncodedAllTimestamps   // Encoded timestamps for all datapoints
			BinaryEncodedAllDpVals       // Encoded values for all datapoints
			BinaryEncodedAllTsid         // Encoded Time-Series IDs
	}

	Multiple such blocks are appended continuously.
*/

func (w *WAL) Append(dps []WalDatapoint) error {
	err := w.encodeWALBlock(dps)
	if err != nil {
		log.Errorf("AppendToWAL: dataCompression failed: %v", err)
		return err
	}

	checksum := crc32.ChecksumIEEE(w.encodedBuf)

	blockSize := uint32(len(w.encodedBuf) + UINT32_SIZE) // UINT32_SIZE : 4 bytes for CRC32 checksum
	_, err = w.fd.Write(utils.Uint32ToBytesLittleEndian(blockSize))
	if err != nil {
		log.Errorf("AppendToWAL : failed to write block size: %v", err)
		return err
	}

	binary.LittleEndian.PutUint32(w.checksumBuf, checksum)
	_, err = w.fd.Write(w.checksumBuf)
	if err != nil {
		log.Errorf("AppendToWAL: failed to write checksum: %v", err)
		return err
	}

	_, err = w.fd.Write(w.encodedBuf)
	if err != nil {
		log.Errorf("AppendToWAL: failed to write block content of size %d to WAL file: %v", len(w.encodedBuf), err)
		return err
	}

	w.totalDps += uint32(len(dps))
	w.encodedSize += uint64(UINT32_SIZE + blockSize) // Adding 4-byte UINT32 (blockSize field) size to encodedSize, excluded from blockSize.

	return nil
}

func (w *WAL) Close() error {
	if w.fd != nil {
		return w.fd.Close()
	}
	return errors.New("file descriptor is nil")
}

func (w *WAL) DeleteWAL() error {
	if w.fd != nil {
		_ = w.fd.Close()
	}

	if err := os.Remove(w.fNameWithPath); err != nil {
		log.Errorf("DeleteWAL : failed to delete WAL file: %v", err)
		return err
	}

	return nil

}

type WalIterator struct {
	fd           *os.File
	currentIndex int
	readBuf      []byte
	readBlockBuf []byte
	readDps      []WalDatapoint
}

func (w *WAL) GetWALStats() (string, uint32, uint64) {
	return w.fNameWithPath, w.totalDps, w.encodedSize
}

func NewWALReader(filePath string) (*WalIterator, error) {
	f, err := os.Open(filePath)
	if err != nil {
		log.Errorf("NewReaderWAL: failed to open WAL file at path %s: %v", filePath, err)
		return nil, err
	}

	versionBuf := make([]byte, 1)
	_, err = f.Read(versionBuf)
	if err != nil {
		log.Errorf("NewReaderWAL: failed to read WAL file version: %v", err)
		return nil, err
	}

	if versionBuf[0] != segutils.VERSION_WALFILE[0] {
		log.Errorf("NewReaderWAL: Unexpected WAL file version: %+v", versionBuf[0])
		return nil, fmt.Errorf("unexpected WAL file version: %+v", versionBuf[0])
	}

	return &WalIterator{
		fd:      f,
		readBuf: make([]byte, 0),
		readDps: make([]WalDatapoint, 0),
	}, nil
}

func (it *WalIterator) Next() (*WalDatapoint, error) {
	if it.currentIndex < len(it.readDps) {
		it.currentIndex++
		return &it.readDps[it.currentIndex-1], nil
	}

	var blockSize uint32
	err := binary.Read(it.fd, binary.LittleEndian, &blockSize)
	if errors.Is(err, io.EOF) {
		return nil, nil
	} else if err != nil {
		log.Errorf("WalIterator Next: failed to read block size from WAL file: %v", it.fd.Name())
		return nil, err
	}

	if blockSize < UINT32_SIZE { // Checking if block size is less than checksum size (4 bytes)
		log.Errorf("WalIterator Next: invalid block size (%d), less than checksum size", blockSize)
		return nil, errors.New("invalid block size")
	}

	var checksum uint32
	err = binary.Read(it.fd, binary.LittleEndian, &checksum)
	if err != nil {
		log.Errorf("WalIterator Next: failed to read checksum: %v", err)
		return nil, err
	}

	it.readBuf = toputils.ResizeSlice(it.readBuf, int(blockSize-UINT32_SIZE)) // remove checksum length and read the actual data block
	_, err = io.ReadFull(it.fd, it.readBuf)
	if err != nil {
		log.Errorf("WalIterator Next: failed to read block data of size %d from file %s: %v", blockSize, it.fd.Name(), err)
		return nil, err
	}

	calculatedChecksum := crc32.ChecksumIEEE(it.readBuf)
	if calculatedChecksum != checksum {
		log.Errorf("WalIterator Next: checksum mismatch! Calculated: %v, Expected: %v", calculatedChecksum, checksum)
		return nil, errors.New("checksum mismatch")
	}

	blockBuf := bytes.NewReader(it.readBuf)
	err = it.decodeWALBlock(blockBuf)

	if err != nil {
		log.Errorf("WalIterator Next: dataDecompression failed: %v", err)
		return nil, err
	}
	it.currentIndex = 1
	return &it.readDps[0], nil
}

func (it *WalIterator) Close() error {
	if it.fd != nil {
		return it.fd.Close()
	}
	return errors.New("file descriptor is nil")
}

func (w *WAL) encodeWALBlock(dps []WalDatapoint) error {
	N := uint32(len(dps))
	if N == 0 {
		log.Warn("EncodeWALBlock: received empty data points slice")
		return errors.New("empty data points")
	}

	w.rawBlockBuf.Reset()
	err := binary.Write(w.rawBlockBuf, binary.LittleEndian, N)
	if err != nil {
		log.Errorf("EncodeWALBlock: failed to write datapoint count: %v", err)
		return err
	}

	for _, dp := range dps {
		if err := binary.Write(w.rawBlockBuf, binary.LittleEndian, dp.Timestamp); err != nil {
			log.Errorf("EncodeWALBlock: failed to write timestamp : %v err : %v", dp.Timestamp, err)
			return err
		}
	}

	for _, dp := range dps {
		if err := binary.Write(w.rawBlockBuf, binary.LittleEndian, dp.DpVal); err != nil {
			log.Errorf("EncodeWALBlock: failed to write dpVal : %v err : %v", dp.DpVal, err)
			return err
		}
	}

	for _, dp := range dps {
		if err := binary.Write(w.rawBlockBuf, binary.LittleEndian, dp.Tsid); err != nil {
			log.Errorf("EncodeWALBlock: failed to write tsid : %v err : %v", dp.Tsid, err)
			return err
		}
	}

	encoderLock.Lock()
	w.encodedBuf = encoder.EncodeAll(w.rawBlockBuf.Bytes(), w.encodedBuf[:0])
	encoderLock.Unlock()
	return nil
}

func (it *WalIterator) decodeWALBlock(blockBuf *bytes.Reader) error {
	it.readBlockBuf = toputils.ResizeSlice(it.readBlockBuf, blockBuf.Len())
	_, err := blockBuf.Read(it.readBlockBuf)

	if err != nil {
		log.Errorf("decodeWALBlock: failed to read from blockBuf into blockBuf: %v", err)
		return err
	}

	it.readBuf, err = decoder.DecodeAll(it.readBlockBuf, it.readBuf[:0])

	if err != nil {
		log.Errorf("decodeWALBlock: decompression failed: %v", err)
		return err
	}

	rawReader := bytes.NewReader(it.readBuf)

	var N uint32
	err = binary.Read(rawReader, binary.LittleEndian, &N)
	if err != nil {
		log.Errorf("decodeWALBlock: failed to read datapoint count: %v", err)
		return err
	}

	it.readDps = toputils.ResizeSlice(it.readDps, int(N))

	for i := 0; i < int(N); i++ {
		err := binary.Read(rawReader, binary.LittleEndian, &it.readDps[i].Timestamp)
		if err != nil {
			log.Errorf("decodeWALBlock: failed to read timestamp at index %d: %v", i, err)
			return err
		}
	}

	for i := 0; i < int(N); i++ {
		err := binary.Read(rawReader, binary.LittleEndian, &it.readDps[i].DpVal)
		if err != nil {
			log.Errorf("decodeWALBlock: failed to read dpVal at index %d: %v", i, err)
			return err
		}
	}

	for i := 0; i < int(N); i++ {
		err := binary.Read(rawReader, binary.LittleEndian, &it.readDps[i].Tsid)
		if err != nil {
			log.Errorf("decodeWALBlock: failed to read tsid at index %d: %v", i, err)
			return err
		}
	}

	return nil
}

/*
Write-Ahead Logging (WAL) for Metrics Names
*/

type MNameWalIterator struct {
	fd               *os.File
	readBuf          []byte
	readMetricsNames []string
	currentIndex     int
}

func (it *MNameWalIterator) Close() error {
	if it.fd != nil {
		return it.fd.Close()
	}
	return errors.New("file descriptor is nil")
}

/*
This function appends a new block to the file.

File Format:
	Version:                1 Byte  // File format version

	// Repeating Blocks Structure
	[Block] {
		BlockLen:               4 Bytes  // Size of this block (excluding this field)
		Checksum:               4 Bytes  // CRC32 checksum for data integrity
		ZstdEncoded Block:
			Metrics Names:  	[] 		 // List of metric names stored in the block
				|-- [Metric Name] {
					StringLength:  2 Bytes   // Length of the metric name (uint16)
					StringData:    Variable  // Actual metric name (UTF-8 encoded)
				}
	}

	Multiple such blocks are appended continuously.
*/

func (w *WAL) AppendMName(mName []string) error {
	w.encodedBuf = w.encodedBuf[:0]
	err := w.compressMetricNames(mName)
	if err != nil {
		log.Errorf("AppendMName: Failed to compress metric names: %v", err)
		return err
	}
	checksum := crc32.ChecksumIEEE(w.encodedBuf)

	blockSize := uint32(len(w.encodedBuf) + UINT32_SIZE) // UINT32_SIZE : 4 bytes for CRC32 checksum
	_, err = w.fd.Write(utils.Uint32ToBytesLittleEndian(blockSize))
	if err != nil {
		log.Errorf("AppendMName : failed to write block size: %v", err)
		return err
	}

	binary.LittleEndian.PutUint32(w.checksumBuf, checksum)
	_, err = w.fd.Write(w.checksumBuf)
	if err != nil {
		log.Errorf("AppendMName: failed to write checksum: %v", err)
		return err
	}

	_, err = w.fd.Write(w.encodedBuf)
	if err != nil {
		log.Errorf("AppendMName: failed to write block content of size %d to WAL file: %v", len(w.encodedBuf), err)
		return err
	}

	w.totalDps += uint32(len(mName))
	w.encodedSize += uint64(UINT32_SIZE + blockSize) // Adding 4-byte UINT32 (blockSize field) size to encodedSize, excluded from blockSize.
	return nil
}

func NewMNameWalReader(filePath string) (*MNameWalIterator, error) {
	fd, err := os.Open(filePath)
	if err != nil {
		log.Errorf("NewMNameWalReader: failed to open WAL file at path %s: %v", filePath, err)
		return nil, err
	}

	versionBuf := make([]byte, 1)
	_, err = fd.Read(versionBuf)
	if err != nil {
		log.Errorf("NewMNameWalReader: failed to read WAL file version: %v", err)
		return nil, err
	}

	if versionBuf[0] != segutils.VERSION_WALFILE[0] {
		log.Errorf("NewMNameWalReader: Unexpected WAL file version: %+v", versionBuf[0])
		return nil, fmt.Errorf("unexpected WAL file version: %+v", versionBuf[0])
	}

	return &MNameWalIterator{
		fd:               fd,
		readBuf:          make([]byte, 0),
		readMetricsNames: make([]string, 0),
		currentIndex:     0,
	}, nil
}

func (it *MNameWalIterator) Next() (*string, error) {
	if it.currentIndex < len(it.readMetricsNames) {
		it.currentIndex++
		return &it.readMetricsNames[it.currentIndex-1], nil
	}

	var blockSize uint32
	err := binary.Read(it.fd, binary.LittleEndian, &blockSize)
	if errors.Is(err, io.EOF) {
		return nil, nil
	} else if err != nil {
		log.Printf("MNameWalIterator Next: failed to read block size from WAL file: %v", err)
		return nil, err
	}

	if blockSize < UINT32_SIZE { // Checking if block size is less than checksum size (4 bytes)
		log.Printf("MNameWalIterator Next: invalid block size (%d), less than checksum size", blockSize)
		return nil, errors.New("invalid block size")
	}

	var checksum uint32
	err = binary.Read(it.fd, binary.LittleEndian, &checksum)
	if err != nil {
		log.Printf("MNameWalIterator Next: failed to read checksum: %v", err)
		return nil, err
	}

	it.readBuf = toputils.ResizeSlice(it.readBuf, int(blockSize-UINT32_SIZE)) // remove checksum length and read the actual data block
	_, err = io.ReadFull(it.fd, it.readBuf)
	if err != nil {
		log.Printf("MNameWalIterator Next: failed to read block data of size %d: %v", blockSize, err)
		return nil, err
	}

	calculatedChecksum := crc32.ChecksumIEEE(it.readBuf)
	if calculatedChecksum != checksum {
		log.Printf("MNameWalIterator Next: checksum mismatch! Calculated: %v, Expected: %v", calculatedChecksum, checksum)
		return nil, errors.New("checksum mismatch")
	}

	it.readMetricsNames = it.readMetricsNames[:0]
	err = it.decompressMetricNames()
	if err != nil {
		log.Errorf("MNameWalIterator Next: Failed to decompress block: %v", err)
		return nil, err
	}
	it.currentIndex = 1
	if len(it.readMetricsNames) == 0 {
		log.Warnf("MNameWalIterator Next: No metrics found in decompressed data")
		return nil, nil
	}
	return &it.readMetricsNames[0], nil
}

func (w *WAL) compressMetricNames(mNames []string) error {
	var buf bytes.Buffer
	for _, str := range mNames {
		length := uint16(len(str))
		err := binary.Write(&buf, binary.LittleEndian, length)
		if err != nil {
			log.Errorf("compressMetricNames: Failed to write length of metric '%s': %v", str, err)
			return err
		}
		buf.WriteString(str)
	}
	encoderLock.Lock()
	w.encodedBuf = encoder.EncodeAll(buf.Bytes(), w.encodedBuf[:0])
	encoderLock.Unlock()
	return nil
}

func (it *MNameWalIterator) decompressMetricNames() error {
	var err error
	it.readBuf, err = decoder.DecodeAll(it.readBuf, nil)
	if err != nil {
		log.Errorf("decompressMetricNames: Failed to decompress data: %v", err)
		return err
	}

	buf := bytes.NewReader(it.readBuf)
	for buf.Len() > 0 {
		var length uint16
		err := binary.Read(buf, binary.LittleEndian, &length)
		if err != nil {
			log.Errorf("decompressMetricNames: Failed to read string length: %v", err)
			return err
		}
		strBytes := make([]byte, length)
		_, err = buf.Read(strBytes)
		if err != nil {
			log.Errorf("decompressMetricNames: Failed to read string data: %v", err)
			return err
		}
		it.readMetricsNames = append(it.readMetricsNames, string(strBytes))
	}

	return nil
}
