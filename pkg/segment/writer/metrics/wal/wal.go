package wal

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"path/filepath"

	"github.com/siglens/siglens/pkg/utils"

	"github.com/klauspost/compress/zstd"
	toputils "github.com/siglens/siglens/pkg/utils"
	log "github.com/sirupsen/logrus"
)

type walDatapoint struct {
	timestamp uint64
	dpVal     float64
	tsid      uint64
}

type WAL struct {
	fd            *os.File
	fNameWithPath string
	totalDps      uint32
	encodedSize   uint64
	encodeBuf     []byte
}

var encoder, _ = zstd.NewWriter(nil)
var decoder, _ = zstd.NewReader(nil)

func NewWAL(baseDir, filename string) (*WAL, error) {
	filePath := filepath.Join(baseDir, filename)
	f, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		log.Errorf("NewWAL : Failed to open WAL file at path %s: %v", filePath, err)
		return nil, err
	}

	return &WAL{
		fNameWithPath: filePath,
		totalDps:      0,
		encodedSize:   0,
		fd:            f,
	}, nil
}

/*
This function appends the block to the file, a new block is added.

File Format:
BlockLen:               4 Bytes
ZstdEncoding of following block:
    NumOfDatapoints (N): 4 Bytes
    BinaryEncodedAllTimestamps
    BinaryEncodedAllDpVals
    BinaryEncodedAllTsid

Multiple such blocks are appended continuously.
*/

func (w *WAL) AppendToWAL(dps []walDatapoint) error {
	blockBuf := &bytes.Buffer{}
	err := w.encodeWALBlock(dps, blockBuf)
	if err != nil {
		log.Errorf("AppendToWAL: dataCompression failed: %v", err)
		return err
	}

	blockSize := uint32(blockBuf.Len())
	_, err = w.fd.Write(utils.Uint32ToBytesLittleEndian(blockSize))
	if err != nil {
		log.Errorf("AppendToWAL : failed to write block size: %v", err)
		return err
	}

	_, err = w.fd.Write(blockBuf.Bytes())
	if err != nil {
		log.Errorf("AppendToWAL: failed to write block content of size %d to WAL file: %v", len(blockBuf.Bytes()), err)
		return err
	}

	w.totalDps += uint32(len(dps))
	w.encodedSize += uint64(4 + blockSize)

	return nil
}

func (w *WAL) Close() error {
	return w.fd.Close()
}

func (w *WAL) DeleteWAL() error {
	return os.Remove(w.fNameWithPath)
}

type WalIterator struct {
	fd           *os.File
	currentBlock []walDatapoint
	currentIndex int
	readBuf      []byte
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
	return &WalIterator{
		fd:      f,
		readBuf: make([]byte, 0),
	}, nil
}

func (it *WalIterator) Next() (*walDatapoint, bool, error) {
	if it.currentIndex < len(it.currentBlock) {
		dp := &it.currentBlock[it.currentIndex]
		it.currentIndex++
		return dp, true, nil
	}

	var blockSize uint32
	err := binary.Read(it.fd, binary.LittleEndian, &blockSize)
	if errors.Is(err, io.EOF) {
		return nil, false, nil
	} else if err != nil {
		log.Errorf("Next: failed to read block size from WAL file: %v", it.fd.Name())
		return nil, false, err
	}

	it.readBuf = toputils.ResizeSlice(it.readBuf, int(blockSize))
	_, err = io.ReadFull(it.fd, it.readBuf)
	if err != nil {
		log.Errorf("Next: failed to read block data of size %d from file %s: %v", blockSize, it.fd.Name(), err)
		return nil, false, err
	}

	blockBuf := bytes.NewReader(it.readBuf)
	newBlock, err := it.decodeWALBlock(blockBuf)

	if err != nil {
		log.Errorf("Next: dataDecompression failed: %v", err)
		return nil, false, err
	}

	it.currentBlock = newBlock
	it.currentIndex = 1

	return &newBlock[0], true, nil
}

func (it *WalIterator) Close() error {
	return it.fd.Close()
}

func (w *WAL) encodeWALBlock(dps []walDatapoint, compressedBlockBuf *bytes.Buffer) error {
	N := uint32(len(dps))
	if N == 0 {
		log.Warn("EncodeWALBlock: received empty data points slice")
		return errors.New("empty data points")
	}

	rawBlockBuf := &bytes.Buffer{}

	err := binary.Write(rawBlockBuf, binary.LittleEndian, N)
	if err != nil {
		log.Errorf("EncodeWALBlock: failed to write datapoint count: %v", err)
		return err
	}

	for _, dp := range dps {
		if err := binary.Write(rawBlockBuf, binary.LittleEndian, dp.timestamp); err != nil {
			log.Errorf("EncodeWALBlock: failed to write timestamp: %v", err)
			return err
		}
	}

	for _, dp := range dps {
		if err := binary.Write(rawBlockBuf, binary.LittleEndian, dp.dpVal); err != nil {
			log.Errorf("EncodeWALBlock: failed to write dpVal: %v", err)
			return err
		}
	}

	for _, dp := range dps {
		if err := binary.Write(rawBlockBuf, binary.LittleEndian, dp.tsid); err != nil {
			log.Errorf("EncodeWALBlock: failed to write tsid: %v", err)
			return err
		}
	}

	w.encodeBuf = encoder.EncodeAll(rawBlockBuf.Bytes(), w.encodeBuf[:0])

	_, err = compressedBlockBuf.Write(w.encodeBuf)
	if err != nil {
		log.Errorf("EncodeWALBlock: failed to write compressed data to buffer: %v", err)
		return err
	}
	return nil
}

func (it *WalIterator) decodeWALBlock(blockBuf *bytes.Reader) ([]walDatapoint, error) {
	compressedData, err := io.ReadAll(blockBuf)
	if err != nil {
		log.Errorf("decodeWALBlock: failed to read compressed data: %v", err)
		return nil, err
	}

	it.readBuf, err = decoder.DecodeAll(compressedData, it.readBuf[:0])

	if err != nil {
		log.Errorf("decodeWALBlock: decompression failed: %v", err)
		return nil, err
	}

	rawReader := bytes.NewReader(it.readBuf)

	var N uint32
	err = binary.Read(rawReader, binary.LittleEndian, &N)
	if err != nil {
		log.Errorf("decodeWALBlock: failed to read datapoint count: %v", err)
		return nil, err
	}

	timestamps := make([]uint64, N)
	dpVals := make([]float64, N)
	tsids := make([]uint64, N)

	for i := 0; i < int(N); i++ {
		err := binary.Read(rawReader, binary.LittleEndian, &timestamps[i])
		if err != nil {
			log.Errorf("decodeWALBlock: failed to read timestamp: %v", err)
			return nil, err
		}
	}

	for i := 0; i < int(N); i++ {
		err := binary.Read(rawReader, binary.LittleEndian, &dpVals[i])
		if err != nil {
			log.Errorf("decodeWALBlock: failed to read dpVal: %v", err)
			return nil, err
		}
	}

	for i := 0; i < int(N); i++ {
		err := binary.Read(rawReader, binary.LittleEndian, &tsids[i])
		if err != nil {
			log.Errorf("decodeWALBlock: failed to read tsid: %v", err)
			return nil, err
		}
	}

	dps := make([]walDatapoint, N)
	for i := 0; i < int(N); i++ {
		dps[i] = walDatapoint{
			timestamp: timestamps[i],
			dpVal:     dpVals[i],
			tsid:      tsids[i],
		}
	}

	return dps, nil
}
