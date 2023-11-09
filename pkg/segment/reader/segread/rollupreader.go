/*
Copyright 2023.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package segread

import (
	"fmt"
	"io"
	"os"
	"path"

	"github.com/bits-and-blooms/bitset"
	"github.com/cespare/xxhash"
	"github.com/siglens/siglens/pkg/blob"
	"github.com/siglens/siglens/pkg/segment/pqmr"
	segutils "github.com/siglens/siglens/pkg/segment/utils"
	"github.com/siglens/siglens/pkg/segment/writer"
	"github.com/siglens/siglens/pkg/utils"

	log "github.com/sirupsen/logrus"
)

type RollupReader struct {
	minRupFd  *os.File
	hourRupFd *os.File
	dayRupFd  *os.File

	// map[blkNum] ==> map[tsBuckKey] ==> RolledRecsForMatchedRecNums
	allBlocksTomRollup map[uint16]map[uint64]*writer.RolledRecs
	allBlocksTohRollup map[uint16]map[uint64]*writer.RolledRecs
	allBlocksTodRollup map[uint16]map[uint64]*writer.RolledRecs

	allBlocksTomLoaded bool
	allBlocksTohLoaded bool
	allBlocksTodLoaded bool
	qid                uint64
	allInUseFiles      []string
}

func InitNewRollupReader(segKey string, tsKey string, qid uint64) (*RollupReader, error) {
	allInUseFiles := make([]string, 0)
	fName := fmt.Sprintf("%v/rups/%v.crup", path.Dir(segKey), xxhash.Sum64String(tsKey+"m"))
	err := blob.DownloadSegmentBlob(fName, true)
	if err != nil {
		log.Errorf("qid=%d, InitNewRollupReader failed to download min rollup file. %+v, err=%v", qid, fName, err)
		return nil, err
	}
	minRupFd, err := os.OpenFile(fName, os.O_RDONLY, 0644)
	if err != nil {
		log.Errorf("qid=%d, InitNewRollupReader: failed to open min rollup file %s. Error: %+v", qid, fName, err)
		return &RollupReader{}, err
	}
	allInUseFiles = append(allInUseFiles, fName)

	fName = fmt.Sprintf("%v/rups/%v.crup", path.Dir(segKey), xxhash.Sum64String(tsKey+"h"))
	err = blob.DownloadSegmentBlob(fName, true)
	if err != nil {
		log.Errorf("qid=%d, InitNewRollupReader failed to download hour rollup file. %+v, err=%v", qid, fName, err)
		return nil, err
	}
	hourRupFd, err := os.OpenFile(fName, os.O_RDONLY, 0644)
	if err != nil {
		log.Errorf("qid=%d, InitNewRollupReader: failed to open hour rollup file %s. Error: %+v", qid, fName, err)
		return &RollupReader{}, err
	}
	allInUseFiles = append(allInUseFiles, fName)

	fName = fmt.Sprintf("%v/rups/%v.crup", path.Dir(segKey), xxhash.Sum64String(tsKey+"d"))
	err = blob.DownloadSegmentBlob(fName, true)
	if err != nil {
		log.Errorf("qid=%d, InitNewRollupReader failed to download day rollup file. %+v, err=%v", qid, fName, err)
		return nil, err
	}
	dayRupFd, err := os.OpenFile(fName, os.O_RDONLY, 0644)
	if err != nil {
		log.Errorf("qid=%d, InitNewRollupReader: failed to open day rollup file %s. Error: %+v", qid, fName, err)
		return &RollupReader{}, err
	}
	allInUseFiles = append(allInUseFiles, fName)

	allBlocksTomRollup := make(map[uint16]map[uint64]*writer.RolledRecs)
	allBlocksTohRollup := make(map[uint16]map[uint64]*writer.RolledRecs)
	allBlocksTodRollup := make(map[uint16]map[uint64]*writer.RolledRecs)

	return &RollupReader{
		minRupFd:           minRupFd,
		hourRupFd:          hourRupFd,
		dayRupFd:           dayRupFd,
		allBlocksTomRollup: allBlocksTomRollup,
		allBlocksTohRollup: allBlocksTohRollup,
		allBlocksTodRollup: allBlocksTodRollup,
		qid:                qid,
		allInUseFiles:      allInUseFiles,
	}, nil
}

func (rur *RollupReader) Close() {
	if rur.minRupFd != nil {
		rur.minRupFd.Close()
	}
	if rur.hourRupFd != nil {
		rur.hourRupFd.Close()
	}
	if rur.dayRupFd != nil {
		rur.dayRupFd.Close()
	}
	err := blob.SetSegSetFilesAsNotInUse(rur.allInUseFiles)
	if err != nil {
		log.Errorf("Failed to release needed segment files from local storage %+v!  Err: %+v", rur.allInUseFiles, err)
	}
}

func (rur *RollupReader) GetMinRollups() (map[uint16]map[uint64]*writer.RolledRecs, error) {
	if rur.allBlocksTomLoaded {
		return rur.allBlocksTomRollup, nil
	}
	err := readRollupFile(rur.minRupFd, rur.allBlocksTomRollup, rur.qid)
	if err != nil {
		log.Errorf("qid=%d, GetMinRollups: failed to read min rollups: err=%v", rur.qid, err)
		return nil, err
	}
	rur.allBlocksTomLoaded = true
	return rur.allBlocksTomRollup, err
}

func (rur *RollupReader) GetHourRollups() (map[uint16]map[uint64]*writer.RolledRecs, error) {
	if rur.allBlocksTohLoaded {
		return rur.allBlocksTohRollup, nil
	}
	err := readRollupFile(rur.hourRupFd, rur.allBlocksTohRollup, rur.qid)
	if err != nil {
		log.Errorf("qid=%d, GetHourRollups: failed to read hour rollups: err=%v", rur.qid, err)
		return nil, err
	}
	rur.allBlocksTohLoaded = true
	return rur.allBlocksTohRollup, err
}

func (rur *RollupReader) GetDayRollups() (map[uint16]map[uint64]*writer.RolledRecs, error) {
	if rur.allBlocksTodLoaded {
		return rur.allBlocksTodRollup, nil
	}
	err := readRollupFile(rur.dayRupFd, rur.allBlocksTodRollup, rur.qid)
	if err != nil {
		log.Errorf("qid=%d, GetDayRollups: failed to read min rollups: err=%v", rur.qid, err)
		return nil, err
	}
	rur.allBlocksTodLoaded = true
	return rur.allBlocksTodRollup, err
}

func readRollupFile(fd *os.File,
	allBlks map[uint16]map[uint64]*writer.RolledRecs, qid uint64) error {

	offset := int64(0)
	var blkNum uint16
	bbBlkNum := make([]byte, 2) // blkNum (2)
	var bKey uint64
	bbBKey := make([]byte, 8) // for bucket key timestamp
	var numBucks uint16
	bbNumBucks := make([]byte, 2) // for num of buckets
	var mrSize uint16
	bbMrSize := make([]byte, 2) // for bitset match result size

	bsBlk := make([]byte, segutils.WIP_NUM_RECS/8)

	for {
		// read blkNum
		_, err := fd.ReadAt(bbBlkNum, offset)
		if err != nil {
			if err != io.EOF {
				log.Errorf("qid=%d, readRollupFile: failed to read blkNum len err=[%+v]", qid, err)
				return err
			}
			break
		}
		offset += 2
		blkNum = utils.BytesToUint16LittleEndian(bbBlkNum[:])
		toxRollup := make(map[uint64]*writer.RolledRecs)
		allBlks[blkNum] = toxRollup

		// read num of buckets
		_, err = fd.ReadAt(bbNumBucks, offset)
		if err != nil {
			if err != io.EOF {
				log.Errorf("qid=%d, readRollupFile: failed to read blkNum len err=[%+v]", qid, err)
				return err
			}
			break
		}
		offset += 2
		numBucks = utils.BytesToUint16LittleEndian(bbNumBucks[:])

		for i := uint16(0); i < numBucks; i++ {
			// read bucketKey timestamp
			_, err = fd.ReadAt(bbBKey, offset)
			if err != nil {
				log.Errorf("qid=%d, readRollupFile: failed to read bKey for blkNum=%v, i=%v, err=[%+v]",
					qid, blkNum, i, err)
				return err
			}
			offset += 8
			bKey = utils.BytesToUint64LittleEndian(bbBKey[:])

			// skip forward for RR_ENC_BITSET, since thats the only type we support today
			offset += 1

			// read matched result bitset size
			_, err = fd.ReadAt(bbMrSize, offset)
			if err != nil {
				log.Errorf("qid=%d, readRollupFile: failed to read mrsize for blkNum=%v, i=%v, err=[%+v]",
					qid, blkNum, i, err)
				return err
			}
			offset += 2
			mrSize = utils.BytesToUint16LittleEndian(bbMrSize[:])

			if currBufSize := len(bsBlk); int(mrSize) > currBufSize {
				toAdd := int(mrSize) - currBufSize
				newSlice := make([]byte, toAdd)
				bsBlk = append(bsBlk, newSlice...)
			}

			// read the actual bitset
			_, err = fd.ReadAt(bsBlk[:mrSize], offset)
			if err != nil {
				if err != io.EOF {
					log.Errorf("qid=%d, readRollupFile: failed to read bitset err=[%+v]", qid, err)
					return err
				}
				break
			}
			offset += int64(mrSize)

			bs := bitset.New(0)
			err = bs.UnmarshalBinary(bsBlk[:mrSize])
			if err != nil {
				log.Errorf("qid=%d, readRollupFile: failed to unmarshall bitset err=[%+v]", qid, err)
				return err
			}

			mr := pqmr.CreatePQMatchResultsFromBs(bs)
			rr := &writer.RolledRecs{MatchedRes: mr}
			toxRollup[bKey] = rr
		}
	}
	return nil
}
