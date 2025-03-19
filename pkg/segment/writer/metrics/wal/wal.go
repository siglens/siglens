package wal

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"path/filepath"

	"github.com/klauspost/compress/zstd"
	log "github.com/sirupsen/logrus"
)

type WALDatapoint struct {
	Timestamp uint64
	DpVal     float64
	Tsid      uint64
}

type WAL struct {
	file          *os.File
	fNameWithPath string
	totalDps      uint32
	encodedSize   uint64
}

func NewWAL(baseDir, filename string) (*WAL, error) {
	filePath := filepath.Join(baseDir, filename)
	f, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		log.Errorf("NewWAL : Failed to open WAL file at path %s: %v", filePath, err)
		return nil, err
	}

	return &WAL{
		fNameWithPath: filePath,
		totalDps:      0,
		encodedSize:   0,
		file:          f,
	}, nil
}

/*
This function appends the block to the file sequentially—each second, a new block is added.

File Format:
BlockLen:               4 Bytes
ZstdEncode following block:
    NumOfDatapoints (N): 4 Bytes
    BinaryEncodeAllTimestamps
    BinaryEncodeAllDpVals
    BinaryEncodeAllTsid

Multiple such blocks are appended continuously as time progresses (every second).
*/

func (w *WAL) AppendToWAL(dps []WALDatapoint) error {
	blockBuf := &bytes.Buffer{}
	err := encodeWALBlock(dps, blockBuf)
	if err != nil {
		log.Errorf("AppendToWAL: dataCompression failed: %v", err)
		return err
	}

	blockSize := uint32(blockBuf.Len())
	if err := binary.Write(w.file, binary.LittleEndian, blockSize); err != nil {
		log.Errorf("AppendToWAL: failed to write block size to WAL file: %v", err)
		return err
	}

	_, err = w.file.Write(blockBuf.Bytes())
	if err != nil {
		log.Errorf("FlushWal: failed to write block content of size %d to WAL file: %v", len(blockBuf.Bytes()), err)
		return err
	}

	w.totalDps += uint32(len(dps))
	w.encodedSize += uint64(blockSize)

	return nil
}

func (w *WAL) Close() error {
	return w.file.Close()
}

func DeleteWAL(baseDir, filename string) error {
	filePath := filepath.Join(baseDir, filename)
	return os.Remove(filePath)
}

type WalIterator struct {
	file         *os.File
	currentBlock []WALDatapoint
	currentIndex int
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
	return &WalIterator{file: f}, nil
}

func (it *WalIterator) Next() (*WALDatapoint, bool, error) {
	if it.currentIndex < len(it.currentBlock) {
		dp := &it.currentBlock[it.currentIndex]
		it.currentIndex++
		return dp, true, nil
	}

	var blockSize uint32
	err := binary.Read(it.file, binary.LittleEndian, &blockSize)
	if errors.Is(err, io.EOF) {
		log.Infof("Next: reached end of WAL file")
		return nil, false, nil
	} else if err != nil {
		log.Errorf("Next: failed to read block size from WAL file: %v", err)
		return nil, false, err
	}

	blockData := make([]byte, blockSize)
	_, err = io.ReadFull(it.file, blockData)
	if err != nil {
		log.Errorf("Next: failed to read block data of size %d: %v", blockSize, err)
		return nil, false, err
	}

	blockBuf := bytes.NewReader(blockData)
	newBlock, err := decodeWALBlock(blockBuf)

	if err != nil {
		log.Errorf("Next: dataDecompression failed: %v", err)
		return nil, false, err
	}

	it.currentBlock = newBlock
	it.currentIndex = 1

	return &newBlock[0], true, nil
}

func (it *WalIterator) Close() error {
	return it.file.Close()
}

func encodeWALBlock(dps []WALDatapoint, compressedBlockBuf *bytes.Buffer) error {
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

	timestamps := make([]uint64, N)
	dpVals := make([]float64, N)
	tsids := make([]uint64, N)
	for i, dp := range dps {
		timestamps[i] = dp.Timestamp
		dpVals[i] = dp.DpVal
		tsids[i] = dp.Tsid
	}

	for _, ts := range timestamps {
		err := binary.Write(rawBlockBuf, binary.LittleEndian, ts)
		if err != nil {
			log.Errorf("EncodeWALBlock: failed to write timestamp: %v", err)
			return err
		}
	}

	for _, val := range dpVals {
		err := binary.Write(rawBlockBuf, binary.LittleEndian, val)
		if err != nil {
			log.Errorf("EncodeWALBlock: failed to write dpVal: %v", err)
			return err
		}
	}

	for _, id := range tsids {
		err := binary.Write(rawBlockBuf, binary.LittleEndian, id)
		if err != nil {
			log.Errorf("EncodeWALBlock: failed to write tsid: %v", err)
			return err
		}
	}

	compressedData, err := compressDataWithZSTD(rawBlockBuf.Bytes())
	if err != nil {
		log.Errorf("EncodeWALBlock: zstd compression failed: %v", err)
		return err
	}

	_, err = compressedBlockBuf.Write(compressedData)
	if err != nil {
		log.Errorf("EncodeWALBlock: failed to write compressed data to buffer: %v", err)
		return err
	}
	return nil
}

func compressDataWithZSTD(data []byte) ([]byte, error) {
	encoder, err := zstd.NewWriter(nil)
	if err != nil {
		log.Errorf("zstdCompressBytes: failed to create zstd encoder: %v", err)
		return nil, err
	}
	defer encoder.Close()
	return encoder.EncodeAll(data, nil), nil
}

func decodeWALBlock(blockBuf *bytes.Reader) ([]WALDatapoint, error) {
	compressedData, err := io.ReadAll(blockBuf)
	if err != nil {
		log.Errorf("decodeWALBlock: failed to read compressed data: %v", err)
		return nil, err
	}

	decompressedData, err := decompressDataWithZSTD(compressedData)
	if err != nil {
		log.Errorf("decodeWALBlock: decompression failed: %v", err)
		return nil, err
	}

	rawReader := bytes.NewReader(decompressedData)

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

	dps := make([]WALDatapoint, N)
	for i := 0; i < int(N); i++ {
		dps[i] = WALDatapoint{
			Timestamp: timestamps[i],
			DpVal:     dpVals[i],
			Tsid:      tsids[i],
		}
	}

	return dps, nil
}

func decompressDataWithZSTD(data []byte) ([]byte, error) {
	decoder, err := zstd.NewReader(nil)
	if err != nil {
		log.Errorf("zstdDecompressBytes: failed to create zstd decoder: %v", err)
		return nil, err
	}
	defer decoder.Close()
	return decoder.DecodeAll(data, nil)
}
