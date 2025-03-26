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
	encodeBuf     []byte
	rawBlockBuf   *bytes.Buffer
	checksumBuf   []byte
}

var encoder, _ = zstd.NewWriter(nil)
var decoder, _ = zstd.NewReader(nil)

const UINT32_SIZE = 4

func NewWAL(baseDir, filename string) (*WAL, error) {
	filePath := filepath.Join(baseDir, filename)

	dir := filepath.Dir(baseDir)
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
		log.Infof("NewWAL: Could not write version byte to file %v. Err %v", filename, err)
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
This function appends the block to the file, a new block is added.

File Format:
version:					1 Bytes
	BlockLen:               4 Bytes
	Checksum:				4 Bytes
	ZstdEncoding of following block:
		NumOfDatapoints (N): 4 Bytes
		BinaryEncodedAllTimestamps
		BinaryEncodedAllDpVals
		BinaryEncodedAllTsid

Multiple such blocks are appended continuously.
*/

func (w *WAL) AppendToWAL(dps []WalDatapoint) error {
	err := w.encodeWALBlock(dps)
	if err != nil {
		log.Errorf("AppendToWAL: dataCompression failed: %v", err)
		return err
	}

	checksum := crc32.ChecksumIEEE(w.encodeBuf)

	blockSize := uint32(len(w.encodeBuf) + UINT32_SIZE) // UINT32_SIZE : 4 bytes for CRC32 checksum
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

	_, err = w.fd.Write(w.encodeBuf)
	if err != nil {
		log.Errorf("AppendToWAL: failed to write block content of size %d to WAL file: %v", len(w.encodeBuf), err)
		return err
	}

	w.totalDps += uint32(len(dps))
	w.encodedSize += uint64(UINT32_SIZE + blockSize) //UINT32_SIZE : 4 bytes for storing block size prefix

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

func NewReaderWAL(baseDir, filename string) (*WalIterator, error) {
	filePath := filepath.Join(baseDir, filename)
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

func (it *WalIterator) Next() (*WalDatapoint, bool, error) {
	if it.currentIndex < len(it.readDps) {
		it.currentIndex++
		return &it.readDps[it.currentIndex-1], true, nil
	}

	var blockSize uint32
	err := binary.Read(it.fd, binary.LittleEndian, &blockSize)
	if errors.Is(err, io.EOF) {
		return nil, false, nil
	} else if err != nil {
		log.Errorf("WalIterator Next: failed to read block size from WAL file: %v", it.fd.Name())
		return nil, false, err
	}

	if blockSize < UINT32_SIZE { // Checking if block size is less than checksum size (4 bytes)
		log.Errorf("WalIterator Next: invalid block size (%d), less than checksum size", blockSize)
		return nil, false, errors.New("invalid block size")
	}

	var checksum uint32
	err = binary.Read(it.fd, binary.LittleEndian, &checksum)
	if err != nil {
		log.Errorf("WalIterator Next: failed to read checksum: %v", err)
		return nil, false, err
	}

	it.readBuf = toputils.ResizeSlice(it.readBuf, int(blockSize-UINT32_SIZE)) // remove checksum length and read the actual data block
	_, err = io.ReadFull(it.fd, it.readBuf)
	if err != nil {
		log.Errorf("WalIterator Next: failed to read block data of size %d from file %s: %v", blockSize, it.fd.Name(), err)
		return nil, false, err
	}

	calculatedChecksum := crc32.ChecksumIEEE(it.readBuf)
	if calculatedChecksum != checksum {
		log.Errorf("WalIterator Next: checksum mismatch! Calculated: %v, Expected: %v", calculatedChecksum, checksum)
		return nil, false, errors.New("checksum mismatch")
	}

	blockBuf := bytes.NewReader(it.readBuf)
	err = it.decodeWALBlock(blockBuf)

	if err != nil {
		log.Errorf("WalIterator Next: dataDecompression failed: %v", err)
		return nil, false, err
	}
	it.currentIndex = 1
	return &it.readDps[0], true, nil
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

	w.encodeBuf = encoder.EncodeAll(w.rawBlockBuf.Bytes(), w.encodeBuf[:0])
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
