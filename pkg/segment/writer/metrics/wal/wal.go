package wal

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/siglens/siglens/pkg/config"
	log "github.com/sirupsen/logrus"
)

type WALDatapoint struct {
	Timestamp uint64
	DpVal     float64
	Tsid      uint64
}

type WAL struct {
	file *os.File
}

func getBaseWalDir() (string, error) {
	var sb strings.Builder
	sb.WriteString(config.GetDataPath())
	sb.WriteString(config.GetHostID())
	sb.WriteString("/wal-ts/")
	dirPath := sb.String()
	err := os.MkdirAll(dirPath, 0755)
	return dirPath, err
}

func NewWAL(filename string) (*WAL, error) {
	dirPath, err := getBaseWalDir()
	if err != nil {
		log.Errorf("NewWAL : Failed to get base WAL directory: %v", err)
		return nil, err
	}

	filePath := filepath.Join(dirPath, filename+".wal")
	f, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		log.Errorf("NewWAL : Failed to open WAL file at path %s: %v", filePath, err)
		return nil, err
	}

	return &WAL{file: f}, nil
}

func (w *WAL) AppendToWAL(dps []WALDatapoint) error {
	blockBuf := &bytes.Buffer{}
	err := dataCompression(dps, blockBuf)
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
	return nil
}

func (w *WAL) Close() error {
	return w.file.Close()
}

func DeleteWAL(filename string) error {
	dirPath, err := getBaseWalDir()
	filePath := filepath.Join(dirPath, filename+".wal")
	if err != nil {
		log.Errorf("DeleteWAL: failed to get base WAL directory: %v", err)
		return err
	}
	return os.Remove(filePath)
}

type WalIterator struct {
	file         *os.File
	currentBlock []WALDatapoint
	currentIndex int
}

func NewReaderWAL(filename string) (*WalIterator, error) {
	dirPath, err := getBaseWalDir()
	filePath := filepath.Join(dirPath, filename+".wal")
	if err != nil {
		log.Errorf("NewReaderWAL: failed to get base WAL directory: %v", err)
		return nil, err
	}

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
	newBlock, err2 := dataDecompression(blockBuf)

	if err2 != nil {
		log.Errorf("Next: dataDecompression failed: %v", err2)
		return nil, false, err2
	}

	if len(newBlock) == 0 {
		log.Warnf("Next: decompressed block has zero datapoints")
		return nil, false, nil
	}

	it.currentBlock = newBlock
	it.currentIndex = 1

	return &newBlock[0], true, nil
}

func (it *WalIterator) Close() error {
	return it.file.Close()
}

func dataCompression(dps []WALDatapoint, blockBuf *bytes.Buffer) error {
	for i, dp := range dps {
		if err := binary.Write(blockBuf, binary.LittleEndian, dp.Timestamp); err != nil {
			log.Errorf("dataCompression: failed to write Timestamp at index %d: %v", i, err)
			return err
		}
		if err := binary.Write(blockBuf, binary.LittleEndian, dp.DpVal); err != nil {
			log.Errorf("dataCompression: failed to write DpVal at index %d: %v", i, err)
			return err
		}
		if err := binary.Write(blockBuf, binary.LittleEndian, dp.Tsid); err != nil {
			log.Errorf("dataCompression: failed to write Tsid at index %d: %v", i, err)
			return err
		}
	}
	return nil
}

func dataDecompression(blockBuf *bytes.Reader) ([]WALDatapoint, error) {
	var newBlock []WALDatapoint
	index := 0

	for blockBuf.Len() > 0 {
		var dp WALDatapoint
		if err := binary.Read(blockBuf, binary.LittleEndian, &dp.Timestamp); err != nil {
			log.Errorf("dataDecompression: failed to read Timestamp at index %d: %v", index, err)
			return nil, err
		}
		if err := binary.Read(blockBuf, binary.LittleEndian, &dp.DpVal); err != nil {
			log.Errorf("dataDecompression: failed to read DpVal at index %d: %v", index, err)

			return nil, err
		}
		if err := binary.Read(blockBuf, binary.LittleEndian, &dp.Tsid); err != nil {
			log.Errorf("dataDecompression: failed to read Tsid at index %d: %v", index, err)
			return nil, err
		}
		newBlock = append(newBlock, dp)
		index++
	}
	return newBlock, nil
}
