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

type DataPoint struct {
	Timestamp uint32
	Value     float64
}

type TimeSeriesBlock struct {
	Tsids      []uint64
	Compressed map[uint64][]byte
}

var walManagerMap = make(map[string]*WALManager)
var walManagerMapLock sync.Mutex

func CreateWal(shardID, segID, blockID string, index uint64) (*WALManager, error) {
	key := shardID + "_" + segID + "_" + blockID + "_" + strconv.FormatUint(index, 10)
	walManagerMapLock.Lock()
	defer walManagerMapLock.Unlock()

	if manager, exists := walManagerMap[key]; exists {
		return manager, nil
	}

	manager, err := newWALManager(shardID, segID, blockID, index)
	if err != nil {
		log.Errorf("CreateWal : Failed to create WALManager for shardID=%s, segID=%s, blockID=%s, index=%d: %v", shardID, segID, blockID, index, err)
		return nil, err
	}

	walManagerMap[key] = manager
	return manager, nil
}

func newWALManager(shardID, segID, blockID string, index uint64) (*WALManager, error) {
	dirPath, err := getBaseWalDir(shardID, segID)
	if err != nil {
		log.Errorf("NewWALManager : Failed to get WAL base directory for shardID=%s, segID=%s: %v", shardID, segID, err)
		return nil, err
	}

	filePath := filepath.Join(dirPath, blockID+"_"+strconv.FormatUint(index, 10)+".wal")
	f, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		log.Errorf("NewWALManager : Failed to open WAL file at path=%s: %v", filePath, err)
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

/*
FlushWal writes a block of time series data to the WAL file for one second.
This function appends the block to the file, so for each second, a new block is written sequentially.

File Format:
[4 bytes] Block Size
[4 bytes] TSID Count
[4 bytes] Compressed TSID List Length
[N bytes] Compressed TSID List (TSIDs like 1001, 1002...)
For each TSID:

	[4 bytes] Compressed Data Length
	[M bytes] Compressed Data

Multiple such blocks are appended continuously as time progresses (every second).
*/
func (w *WALManager) FlushWal(block TimeSeriesBlock) {
	tempBuf := &bytes.Buffer{}
	writeUint32(tempBuf, uint32(len(block.Tsids)))

	tsidBuf := &bytes.Buffer{}
	for _, tsid := range block.Tsids {
		err := binary.Write(tsidBuf, binary.LittleEndian, tsid)
		if err != nil {
			log.Errorf("FlushWal: failed to write TSID %d: %v", tsid, err)
			return
		}
	}
	compressedTsidList := compressZSTD(tsidBuf.Bytes())

	writeUint32(tempBuf, uint32(len(compressedTsidList)))

	tempBuf.Write(compressedTsidList)

	for _, tsid := range block.Tsids {
		data := block.Compressed[tsid]
		writeUint32(tempBuf, uint32(len(data)))
		tempBuf.Write(data)
	}

	blockSize := uint32(tempBuf.Len())
	err := binary.Write(w.file, binary.LittleEndian, blockSize)
	if err != nil {
		log.Errorf("FlushWal: failed to write block size %d to file: %v", blockSize, err)
		return
	}

	_, err = w.file.Write(tempBuf.Bytes())
	if err != nil {
		log.Errorf("FlushWal: failed to write block content of size %d to WAL file: %v", len(tempBuf.Bytes()), err)
		return
	}
	w.fileSize += int64(4 + blockSize)

	if w.fileSize >= w.maxFileSize {
		err := w.rotateFile()
		if err != nil {
			log.Errorf("FlushWal: failed to rotate WAL file after reaching max size (%d bytes): %v", w.maxFileSize, err)
		}
	}

}

func (w *WALManager) ReadWal() map[uint64][]DataPoint {
	tsMap := make(map[uint64][]DataPoint)

	walFilePaths := CollectWALFiles(w.dirPath)
	for _, filePath := range walFilePaths {

		f, err := os.Open(filePath)
		if err != nil {
			log.Errorf("ReadWal: failed to open WAL file %s: %v", filePath, err)
			continue
		}
		defer f.Close()

		for {
			var blockSize uint32
			err = binary.Read(f, binary.LittleEndian, &blockSize)
			if err != nil {
				break
			}

			blockData := make([]byte, blockSize)
			_, err = f.Read(blockData)
			if err != nil {
				log.Errorf("ReadWal: failed to read block data from %s: %v", filePath, err)
				break
			}

			buf := bytes.NewReader(blockData)

			var tsidCount uint32
			err = binary.Read(buf, binary.LittleEndian, &tsidCount)

			if err != nil {
				log.Errorf("ReadWal: failed to read TSID count from %s: %v", filePath, err)
				break
			}

			var compressedLen uint32
			err = binary.Read(buf, binary.LittleEndian, &compressedLen)
			if err != nil {
				log.Errorf("ReadWal: failed to read compressed TSID list length from %s: %v", filePath, err)
				break
			}

			compressedTsidList := make([]byte, compressedLen)
			_, err = buf.Read(compressedTsidList)
			if err != nil {
				log.Errorf("ReadWal: failed to read compressed TSID list from %s: %v", filePath, err)
				break
			}

			tsidListRaw := decompressZSTD(compressedTsidList)

			tsidBuf := bytes.NewReader(tsidListRaw)
			tsids := make([]uint64, tsidCount)
			for i := 0; i < int(tsidCount); i++ {
				err = binary.Read(tsidBuf, binary.LittleEndian, &tsids[i])
				if err != nil {
					log.Errorf("ReadWal: failed to read TSID at index %d from %s: %v", i, filePath, err)
					break
				}
			}

			for _, tsid := range tsids {
				var dataLen uint32
				err := binary.Read(buf, binary.LittleEndian, &dataLen)
				if err != nil {
					log.Errorf("ReadWal: failed to read data length for TSID %d from %s: %v", tsid, filePath, err)
					break
				}

				dataCompressed := make([]byte, dataLen)
				_, err = buf.Read(dataCompressed)
				if err != nil {
					log.Errorf("ReadWal: failed to read compressed data for TSID %d from %s: %v", tsid, filePath, err)
					break
				}
				rawSeries := bytes.NewReader(dataCompressed)
				tsitr, err := compress.NewDecompressIterator(rawSeries)
				if err != nil {
					log.Errorf("ReadWal: failed to create decompress iterator for TSID %d from %s: %v", tsid, filePath, err)
					break
				}
				for tsitr.Next() {
					ts, dp := tsitr.At()
					tsMap[tsid] = append(tsMap[tsid], DataPoint{
						Timestamp: ts,
						Value:     dp,
					})
					fmt.Printf("tsid: %v timestamp: %v val: %v\n", tsid, ts, dp)
				}
			}
		}
	}

	return tsMap

}

func (w *WALManager) rotateFile() error {
	err := w.file.Close()
	if err != nil {
		log.Errorf("rotateFile: failed to close current WAL file: %v", err)
		return err
	}
	w.index++

	newDir, _ := getBaseWalDir(w.shardID, w.segID)

	filePath := filepath.Join(newDir, w.blockID+"_"+strconv.FormatUint(w.index, 10)+".wal")
	f, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		log.Errorf("rotateFile: failed to open new WAL file %s: %v", filePath, err)
		return err
	}

	w.file = f
	w.dirPath = newDir
	w.fileSize = 0
	log.Infof("rotateFile: rotated to new WAL file ")
	return nil
}

func CollectWALFiles(baseDir string) []string {
	var WalFilePaths []string
	err := filepath.Walk(baseDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			log.Errorf("CollectWALFiles: error accessing path %s: %v", path, err)
			return err
		}

		if !info.IsDir() && filepath.Ext(info.Name()) == ".wal" {
			WalFilePaths = append(WalFilePaths, path)
		}
		return nil
	})

	if err != nil {
		log.Errorf("CollectWALFiles: filepath.Walk failed in baseDir %s: %v", baseDir, err)
	}

	return WalFilePaths
}

func DeleteWALFiles(walFiles []string) error {
	for _, filePath := range walFiles {
		err := os.Remove(filePath)
		if err != nil {
			log.Errorf("DeleteWALFiles: failed to delete %s: %v", filePath, err)
			return err
		} else {
			log.Infof("DeleteWALFiles: deleted WAL file %s", filePath)
		}
	}
	return nil
}
