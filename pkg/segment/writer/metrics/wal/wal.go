package wal

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/siglens/siglens/pkg/segment/structs"

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

type Wal struct {
	fd          *os.File
	encodedBuf  []byte
	checksumBuf []byte
	filePath    string
	Encoder     walAppender
	encodedSize uint64
}

var encoder, _ = zstd.NewWriter(nil)
var decoder, _ = zstd.NewReader(nil)
var encoderLock sync.Mutex

const Uint32Size = 4

type walAppender interface {
	PrepareEncode(input any) ([]byte, error)
}

func (w *Wal) prepareEncodedBlock(input any) ([]byte, error) {
	return w.Encoder.PrepareEncode(input)
}

func NewWAL(filePath string, encoder walAppender) (*Wal, error) {
	dir := filepath.Dir(filePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		log.Errorf("NewWAL : Failed to create directories for path %s: %v", dir, err)
		return nil, err
	}

	fd, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		log.Errorf("NewWAL : Failed to open WAL file at path %s: %v", filePath, err)
		return nil, err
	}

	_, err = fd.Write(segutils.VERSION_WALFILE)
	if err != nil {
		log.Infof("NewDataPointWal: Could not write version byte to file %v. Err %v", filePath, err)
		return nil, err
	}

	return &Wal{
		fd:          fd,
		filePath:    filePath,
		Encoder:     encoder,
		checksumBuf: make([]byte, 4),
	}, nil
}

// Append appends a new encoded block to the existing WAL file.
func (w *Wal) Append(input any) error {
	var err error
	w.encodedBuf, err = w.prepareEncodedBlock(input)
	if err != nil {
		log.Errorf("Wal.Append: failed to prepare encoded block: %v", err)
		return err
	}
	return w.writeBlockToFile()
}

/*
Write truncates the WAL file and writes the new input.
  - Although WALs are usually append-only, in some cases (like segmeta.json meta entries),
  - we overwrite the file since old data is no longer needed.
*/
func (w *Wal) Write(input any) error {
	err := w.truncate()
	if err != nil {
		log.Errorf("Wal.Write: failed to truncate WAL file: %v", err)
		return err
	}
	w.encodedBuf, err = w.prepareEncodedBlock(input)
	if err != nil {
		log.Errorf("Wal.Write: failed to prepare encoded block: %v", err)
		return err
	}

	return w.writeBlockToFile()
}

func (w *Wal) writeBlockToFile() error {
	checksum := crc32.ChecksumIEEE(w.encodedBuf)
	blockSize := uint32(len(w.encodedBuf) + Uint32Size) // UINT32_SIZE : 4 bytes for CRC32 checksum

	_, err := w.fd.Write(utils.Uint32ToBytesLittleEndian(blockSize))
	if err != nil {
		log.Errorf("Wal.writeBlockToFile: failed to write block size: %v", err)
		return err
	}

	binary.LittleEndian.PutUint32(w.checksumBuf, checksum)
	_, err = w.fd.Write(w.checksumBuf)
	if err != nil {
		log.Errorf("Wal.writeBlockToFile: failed to write checksum: %v", err)
		return err
	}

	_, err = w.fd.Write(w.encodedBuf)
	if err != nil {
		log.Errorf("Wal.writeBlockToFile: failed to write encoded block: %v", err)
		return err
	}

	w.encodedSize += uint64(Uint32Size + blockSize) // Adding 4-byte UINT32 (blockSize field) size to encodedSize, excluded from blockSize.
	return err
}

func (w *Wal) truncate() error {
	err := w.fd.Truncate(0)
	if err != nil {
		log.Errorf("Wal.truncate: failed to truncate file: %v", err)
		return err
	}
	_, err = w.fd.Seek(0, 0)
	if err != nil {
		log.Errorf("Wal.truncate: failed to seek to beginning: %v", err)
		return err
	}

	_, err = w.fd.Write(segutils.VERSION_WALFILE)
	if err != nil {
		log.Errorf("Wal.truncate: failed to write WAL version: %v", err)
		return err
	}
	return err
}

type DataPointEncoder struct {
	rawBlockBuf *bytes.Buffer
	encodedBuf  []byte
}

func NewDataPointEncoder() *DataPointEncoder {
	return &DataPointEncoder{
		rawBlockBuf: new(bytes.Buffer),
	}
}

/*
Data Point WAL Format

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

func (dpe *DataPointEncoder) PrepareEncode(input any) ([]byte, error) {
	dps, ok := input.([]WalDatapoint)
	if !ok {
		return nil, errors.New("invalid type for DataPointEncoder")
	}

	dpe.rawBlockBuf.Reset()
	N := uint32(len(dps))
	if N == 0 {
		log.Warn("DataPointEncoder PrepareEncode : received empty data points slice")
		return nil, errors.New("empty data points")
	}

	err := binary.Write(dpe.rawBlockBuf, binary.LittleEndian, N)
	if err != nil {
		log.Errorf("DataPointEncoder PrepareEncode: failed to write datapoint count: %v", err)
		return nil, err
	}

	for _, dp := range dps {
		if err := binary.Write(dpe.rawBlockBuf, binary.LittleEndian, dp.Timestamp); err != nil {
			log.Errorf("DataPointEncoder.PrepareEncode: failed to write timestamp :  %v err : %v", dp.Timestamp, err)
			return nil, err
		}
	}
	for _, dp := range dps {
		if err := binary.Write(dpe.rawBlockBuf, binary.LittleEndian, dp.DpVal); err != nil {
			log.Errorf("DataPointEncoder.PrepareEncode: failed to write DpVal : %v err : %v", dp.DpVal, err)
			return nil, err
		}
	}
	for _, dp := range dps {
		if err := binary.Write(dpe.rawBlockBuf, binary.LittleEndian, dp.Tsid); err != nil {
			log.Errorf("DataPointEncoder.PrepareEncode: failed to write Tsid : %v err : %v", dp.Tsid, err)
			return nil, err
		}
	}

	encoderLock.Lock()
	dpe.encodedBuf = encoder.EncodeAll(dpe.rawBlockBuf.Bytes(), dpe.encodedBuf[:0])
	encoderLock.Unlock()
	return dpe.encodedBuf, nil
}

func (w *Wal) GetWALStats() uint64 {
	return w.encodedSize
}

type DPWalIterator struct {
	fd           *os.File
	currentIndex int
	readBuf      []byte
	readBlockBuf []byte
	readDps      []WalDatapoint
}

func NewWALReader(filePath string) (*DPWalIterator, error) {
	fd, err := openAndValidateWALFile(filePath)
	if err != nil {
		log.Errorf("NewWALReader: validation failed for WAL file %s: %v", filePath, err)
		return nil, err
	}

	return &DPWalIterator{
		fd:      fd,
		readBuf: make([]byte, 0),
		readDps: make([]WalDatapoint, 0),
	}, nil
}

func openAndValidateWALFile(filePath string) (*os.File, error) {
	f, err := os.Open(filePath)
	if err != nil {
		log.Errorf("openAndValidateWALFile: failed to open WAL file at path %s: %v", filePath, err)
		return nil, err
	}

	versionBuf := make([]byte, 1)
	_, err = f.Read(versionBuf)
	if err != nil {
		log.Errorf("openAndValidateWALFile: failed to read WAL file version: %v", err)
		return nil, err
	}

	if versionBuf[0] != segutils.VERSION_WALFILE[0] {
		log.Errorf("openAndValidateWALFile: Unexpected WAL file version: %+v", versionBuf[0])
		return nil, fmt.Errorf("unexpected WAL file version: %+v", versionBuf[0])
	}
	return f, nil
}

func (it *DPWalIterator) Next() (*WalDatapoint, error) {
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

	if blockSize < Uint32Size { // Checking if block size is less than checksum size (4 bytes)
		log.Errorf("WalIterator Next: invalid block size (%d), less than checksum size", blockSize)
		return nil, errors.New("invalid block size")
	}

	var checksum uint32
	err = binary.Read(it.fd, binary.LittleEndian, &checksum)
	if err != nil {
		log.Errorf("WalIterator Next: failed to read checksum: %v", err)
		return nil, err
	}

	it.readBuf = toputils.ResizeSlice(it.readBuf, int(blockSize-Uint32Size)) // remove checksum length and read the actual data block
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

func (it *DPWalIterator) Close() error {
	if it.fd != nil {
		return it.fd.Close()
	}
	return errors.New("file descriptor is nil")
}

func (it *DPWalIterator) decodeWALBlock(blockBuf *bytes.Reader) error {
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

type MetricNameEncoder struct {
	rawBlockBuf *bytes.Buffer
	encodedBuf  []byte
}

func NewMetricNameEncoder() *MetricNameEncoder {
	return &MetricNameEncoder{
		rawBlockBuf: new(bytes.Buffer),
	}
}

/*
Metrics Name wal format

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

func (mne *MetricNameEncoder) PrepareEncode(input any) ([]byte, error) {
	names, ok := input.([]string)
	if !ok {
		return nil, errors.New("invalid type for MetricNameEncoder")
	}

	mne.rawBlockBuf.Reset()
	for _, name := range names {
		err := binary.Write(mne.rawBlockBuf, binary.LittleEndian, uint16(len(name)))
		if err != nil {
			log.Errorf("compressMetricNames: Failed to write length of metric '%s': %v", name, err)
			return nil, err
		}
		mne.rawBlockBuf.WriteString(name)
	}

	encoderLock.Lock()
	mne.encodedBuf = encoder.EncodeAll(mne.rawBlockBuf.Bytes(), mne.encodedBuf[:0])
	encoderLock.Unlock()

	return mne.encodedBuf, nil
}

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

func NewMNameWalReader(filePath string) (*MNameWalIterator, error) {
	fd, err := openAndValidateWALFile(filePath)
	if err != nil {
		return nil, err
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
		log.Errorf("MNameWalIterator Next: failed to read block size from WAL file: %v", err)
		return nil, err
	}

	if blockSize < Uint32Size { // Checking if block size is less than checksum size (4 bytes)
		log.Errorf("MNameWalIterator Next: invalid block size (%d), less than checksum size", blockSize)
		return nil, errors.New("invalid block size")
	}

	var checksum uint32
	err = binary.Read(it.fd, binary.LittleEndian, &checksum)
	if err != nil {
		log.Errorf("MNameWalIterator Next: failed to read checksum: %v", err)
		return nil, err
	}

	it.readBuf = toputils.ResizeSlice(it.readBuf, int(blockSize-Uint32Size)) // remove checksum length and read the actual data block
	_, err = io.ReadFull(it.fd, it.readBuf)
	if err != nil {
		log.Errorf("MNameWalIterator Next: failed to read block data of size %d: %v", blockSize, err)
		return nil, err
	}

	calculatedChecksum := crc32.ChecksumIEEE(it.readBuf)
	if calculatedChecksum != checksum {
		log.Errorf("MNameWalIterator Next: checksum mismatch! Calculated: %v, Expected: %v", calculatedChecksum, checksum)
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

type MetricsMetaEncoder struct{}

/*
Metrics meta entry file format

File Format:
	Version:                1 Byte  // File format version

	[Block]
		BlockLen:               4 Bytes  // Size of this block (excluding this field)
		Checksum:               4 Bytes  // CRC32 checksum for data integrity
		[]metricsMetaEntriesForEverySegment: Array of metric meta entries for every segment
*/

func (e *MetricsMetaEncoder) PrepareEncode(input any) ([]byte, error) {
	meta, ok := input.([]*structs.MetricsMeta)
	if !ok {
		return nil, errors.New("invalid type for MetricsMetaEncoder")
	}
	return json.Marshal(meta)
}

type MMetaEntryIterator struct {
	fd      *os.File
	readBuf []byte
	entries []*structs.MetricsMeta
	current int
}

func (it *MMetaEntryIterator) Close() error {
	if it.fd != nil {
		return it.fd.Close()
	}
	return errors.New("file descriptor is nil")
}

func NewMetricsMetaEntryWalReader(filePath string) (*MMetaEntryIterator, error) {
	fd, err := openAndValidateWALFile(filePath)
	if err != nil {
		return nil, err
	}

	return &MMetaEntryIterator{
		fd:      fd,
		readBuf: make([]byte, 0),
		entries: make([]*structs.MetricsMeta, 0),
		current: 0,
	}, nil
}

func (it *MMetaEntryIterator) Next() (*structs.MetricsMeta, error) {
	if it.current < len(it.entries) {
		it.current++
		return it.entries[it.current-1], nil
	}

	var blockSize uint32
	err := binary.Read(it.fd, binary.LittleEndian, &blockSize)
	if errors.Is(err, io.EOF) {
		return nil, nil
	} else if err != nil {
		log.Errorf("MetricsMetaWalReader Next: failed to read block size: %v", err)
		return nil, err
	}

	if blockSize < Uint32Size {
		log.Errorf("MetricsMetaWalReader Next: invalid block size (%d), less than checksum size", blockSize)
		return nil, fmt.Errorf("invalid block size")
	}

	var checksum uint32
	err = binary.Read(it.fd, binary.LittleEndian, &checksum)
	if err != nil {
		log.Errorf("MetricsMetaWalReader Next: failed to read checksum: %v", err)
		return nil, err
	}

	dataSize := blockSize - Uint32Size
	it.readBuf = make([]byte, dataSize)
	_, err = io.ReadFull(it.fd, it.readBuf)
	if err != nil {
		log.Errorf("MetricsMetaWalReader Next: failed to read WAL block: %v", err)
		return nil, err
	}

	calculatedChecksum := crc32.ChecksumIEEE(it.readBuf)
	if calculatedChecksum != checksum {
		log.Errorf("MetricsMetaWalReader Next: checksum mismatch! Got %v, Expected %v", calculatedChecksum, checksum)
		return nil, fmt.Errorf("checksum mismatch")
	}

	it.entries = nil
	err = json.Unmarshal(it.readBuf, &it.entries)
	if err != nil {
		log.Errorf("MetricsMetaWalReader Next: JSON unmarshal failed: %v", err)
		return nil, err
	}

	if len(it.entries) == 0 {
		log.Warnf("MetricsMetaWalReader Next: no entries found in block")
		return nil, nil
	}

	it.current = 1
	return it.entries[0], nil
}

func (w *Wal) Close() error {
	if w.fd != nil {
		return w.fd.Close()
	}
	return errors.New("file descriptor is nil")
}

func (w *Wal) DeleteWAL() error {
	if w.fd != nil {
		_ = w.fd.Close()
	}

	if err := os.Remove(w.filePath); err != nil {
		log.Errorf("DeleteWAL : failed to delete WAL file: %v", err)
		return err
	}

	return nil

}
