package wal

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/klauspost/compress/zstd"
	"github.com/siglens/siglens/pkg/config"
	"github.com/siglens/siglens/pkg/segment/writer/metrics/compress"

	log "github.com/sirupsen/logrus"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

var WalFilePaths []string

func getBaseWalDir(shardID, segID string) (string, error) {
	var sb strings.Builder
	sb.WriteString(config.GetDataPath())
	sb.WriteString(config.GetHostID())
	sb.WriteString("/wal-ts/")
	sb.WriteString("shardid-" + shardID + "-segid-" + segID + "/")
	dirPath := sb.String()
	err := os.MkdirAll(dirPath, 0755)
	return dirPath, err
}

type WALManager struct {
	dirPath     string
	file        *os.File
	fileSize    int64
	maxFileSize int64
	index       uint64
	shardID     string
	segID       string
	blockID     string
	entryCount  int
}

type TimeSeriesBlock struct {
	Tsids      []uint64          // Unique TSIDs
	Compressed map[uint64][]byte // Compressed data for each TSID
}

type ShardedTimeSeriesBlock struct {
	ShardID   string
	SegmentID string
	BlockNO   string
	Block     TimeSeriesBlock
}

var walManagerMap = make(map[string]*WALManager)
var walManagerMapLock sync.Mutex

func GetWALManager(shardID, segID, blockID string, index uint64) (*WALManager, error) {
	key := shardID + "_" + segID + "_" + blockID + "_" + strconv.FormatUint(index, 10)
	walManagerMapLock.Lock()
	defer walManagerMapLock.Unlock()

	if manager, exists := walManagerMap[key]; exists {
		return manager, nil
	}

	manager, err := NewWALManager(shardID, segID, blockID, index)
	if err != nil {
		return nil, err
	}

	walManagerMap[key] = manager
	return manager, nil
}

func NewWALManager(shardID, segID, blockID string, index uint64) (*WALManager, error) {
	dirPath, err := getBaseWalDir(shardID, segID)
	if err != nil {
		return nil, err
	}

	filePath := filepath.Join(dirPath, blockID+"_"+strconv.FormatUint(index, 10)+".wal")
	f, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}

	return &WALManager{
		dirPath:     dirPath,
		file:        f,
		fileSize:    0,
		maxFileSize: 10 * 1024 * 1024,
		index:       index,
		shardID:     shardID,
		segID:       segID,
		blockID:     blockID,
		entryCount:  0,
	}, nil
}

func compressZSTD(data []byte) []byte {
	encoder, _ := zstd.NewWriter(nil)
	return encoder.EncodeAll(data, make([]byte, 0, len(data)))
}

func decompressZSTD(data []byte) []byte {
	decoder, _ := zstd.NewReader(nil)
	result, _ := decoder.DecodeAll(data, nil)
	return result
}

func writeUint32(buf *bytes.Buffer, val uint32) {
	_ = binary.Write(buf, binary.LittleEndian, val)
}

func readUint32(f *os.File) uint32 {
	var val uint32
	_ = binary.Read(f, binary.LittleEndian, &val)
	return val
}

func (w *WALManager) WriteWALBlock(block TimeSeriesBlock) {
	tempBuf := &bytes.Buffer{}

	// Write TSID count
	writeUint32(tempBuf, uint32(len(block.Tsids)))

	// Compress TSID list
	tsidBuf := &bytes.Buffer{}
	for _, tsid := range block.Tsids {
		_ = binary.Write(tsidBuf, binary.LittleEndian, tsid)
	}
	compressedTsidList := compressZSTD(tsidBuf.Bytes())

	// Write compressed TSID list len
	writeUint32(tempBuf, uint32(len(compressedTsidList)))

	tempBuf.Write(compressedTsidList)

	// For each TSID, write data length + data
	for _, tsid := range block.Tsids {
		data := block.Compressed[tsid]
		writeUint32(tempBuf, uint32(len(data)))
		tempBuf.Write(data)
	}

	// Write Block Size first
	blockSize := uint32(tempBuf.Len())
	_ = binary.Write(w.file, binary.LittleEndian, blockSize)

	// Write block content
	w.file.Write(tempBuf.Bytes())
	fmt.Println("Block written with size:", blockSize)
}

func ReadWALBlocks(filePath string) {
	f, _ := os.Open(filePath)
	defer f.Close()

	for {
		// Read block size
		var blockSize uint32
		err := binary.Read(f, binary.LittleEndian, &blockSize)
		fmt.Println(blockSize)
		if err != nil {
			break
		}
		blockData := make([]byte, blockSize)
		_, err = f.Read(blockData)
		if err != nil {
			break
		}

		buf := bytes.NewReader(blockData)

		// Read TSID count
		var tsidCount uint32
		_ = binary.Read(buf, binary.LittleEndian, &tsidCount)

		fmt.Println(tsidCount)
		var compressedLen uint32
		_ = binary.Read(buf, binary.LittleEndian, &compressedLen)

		// Read compressed TSID list
		compressedTsidList := make([]byte, compressedLen)
		buf.Read(compressedTsidList)

		tsidListRaw := decompressZSTD(compressedTsidList)

		tsidBuf := bytes.NewReader(tsidListRaw)
		tsids := make([]uint64, tsidCount)
		for i := 0; i < int(tsidCount); i++ {
			_ = binary.Read(tsidBuf, binary.LittleEndian, &tsids[i])
		}

		// Read and decompress data for each TSID
		for _, tsid := range tsids {
			var dataLen uint32
			_ = binary.Read(buf, binary.LittleEndian, &dataLen)

			dataCompressed := make([]byte, dataLen)
			buf.Read(dataCompressed)
			rawSeries := bytes.NewReader(dataCompressed)
			tsitr, _ := compress.NewDecompressIterator(rawSeries)
			for tsitr.Next() {
				ts, dp := tsitr.At()
				fmt.Printf("TSID %d: Data: TS %s val %s\n", tsid, ts, dp)

			}

		}
	}
}

func (w *WALManager) rotateFile() error {
	w.file.Close()
	w.index++

	newDir, err := getBaseWalDir(w.shardID, w.segID)
	if err != nil {
		return err
	}

	filePath := filepath.Join(newDir, w.blockID+"_"+strconv.FormatUint(w.index, 10)+".wal")
	f, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return err
	}

	// Update WALManager
	w.file = f
	w.dirPath = newDir
	w.fileSize = 0
	return nil
}

func CollectWALFiles(baseDir string) error {
	// Walk through the directory recursively
	err := filepath.Walk(baseDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			log.Error("Something mistake %v", err)
			return err // Handle error during traversal
		}

		// Check if this is a file with .wal extension
		if !info.IsDir() && filepath.Ext(info.Name()) == ".wal" {
			WalFilePaths = append(WalFilePaths, path)
		}
		return nil
	})

	return err
}

func DeleteWALFiles(walFiles []string) error {
	for _, filePath := range walFiles {
		err := os.Remove(filePath)
		if err != nil {
			fmt.Printf("Failed to delete %s: %v\n", filePath, err)
			return err // Stop if any file fails to delete
		} else {
			fmt.Printf("Deleted WAL file: %s\n", filePath)
		}
	}
	return nil
}
