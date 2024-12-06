// Copyright (c) 2021-2024 SigScalr, Inc.
//
// This file is part of SigLens Observability Solution
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

func checkAndDownload(fName string) error {
	_, err := os.Stat(fName)
	if err != nil {
		if os.IsNotExist(err) {
			downloadErr := blob.DownloadSegmentBlob(fName, true)
			if downloadErr != nil {
				return fmt.Errorf("checkAndDownload: failed to download min rollup file: %+v, downloadErr: %v", fName, downloadErr)
			}
		}
		return fmt.Errorf("checkAndDownload: failed to stat file: %+v, err: %v", fName, err)
	}
	return nil
}

func InitNewRollupReader(segKey string, tsKey string, qid uint64) (*RollupReader, error) {
	allInUseFiles := make([]string, 0)
	fName := fmt.Sprintf("%v/rups/%v.crup", path.Dir(segKey), xxhash.Sum64String(tsKey+"m"))
	err := checkAndDownload(fName)
	if err != nil {
		return nil, utils.TeeErrorf("qid=%d, InitNewRollupReader: failed to download min rollup file, err: %v", qid, err)
	}

	minRupFd, err := os.OpenFile(fName, os.O_RDONLY, 0644)
	if err != nil {
		log.Errorf("qid=%d, InitNewRollupReader: failed to open min rollup file: %s, err: %+v", qid, fName, err)
		return &RollupReader{}, err
	}
	allInUseFiles = append(allInUseFiles, fName)

	fName = fmt.Sprintf("%v/rups/%v.crup", path.Dir(segKey), xxhash.Sum64String(tsKey+"h"))
	err = checkAndDownload(fName)
	if err != nil {
		return nil, utils.TeeErrorf("qid=%d, InitNewRollupReader: failed to download hour rollup file, err: %v", qid, err)
	}

	hourRupFd, err := os.OpenFile(fName, os.O_RDONLY, 0644)
	if err != nil {
		log.Errorf("qid=%d, InitNewRollupReader: failed to open hour rollup file: %s, err: %+v", qid, fName, err)
		return &RollupReader{}, err
	}
	allInUseFiles = append(allInUseFiles, fName)

	fName = fmt.Sprintf("%v/rups/%v.crup", path.Dir(segKey), xxhash.Sum64String(tsKey+"d"))
	err = checkAndDownload(fName)
	if err != nil {
		return nil, utils.TeeErrorf("qid=%d, InitNewRollupReader: failed to download day rollup file, err: %v", qid, err)
	}
	dayRupFd, err := os.OpenFile(fName, os.O_RDONLY, 0644)
	if err != nil {
		log.Errorf("qid=%d, InitNewRollupReader: failed to open day rollup file: %s, err: %+v", qid, fName, err)
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
		log.Errorf("RollupReader.Close: Failed to release needed segment files from local storage: %+v!  err: %+v", rur.allInUseFiles, err)
	}
}

func (rur *RollupReader) GetMinRollups() (map[uint16]map[uint64]*writer.RolledRecs, error) {
	if rur.allBlocksTomLoaded {
		return rur.allBlocksTomRollup, nil
	}
	err := readRollupFile(rur.minRupFd, rur.allBlocksTomRollup, rur.qid)
	if err != nil {
		log.Errorf("qid=%d, RollupReader.GetMinRollups: failed to read min rollups, err: %v", rur.qid, err)
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
		log.Errorf("qid=%d, RollupReader.GetHourRollups: failed to read hour rollups, err: %v", rur.qid, err)
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
		log.Errorf("qid=%d, RollupReader.GetDayRollups: failed to read day rollups, err: %v", rur.qid, err)
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
				log.Errorf("qid=%d, readRollupFile: failed to read blkNum at offset: %v, err: %+v", qid, offset, err)
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
				log.Errorf("qid=%d, readRollupFile: failed to read num of buckets at offset: %v, err: %+v", qid, offset, err)
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
				log.Errorf("qid=%d, readRollupFile: failed to read bKey for blkNum: %v, i: %v, offset: %v, err: %+v",
					qid, blkNum, i, offset, err)
				return err
			}
			offset += 8
			bKey = utils.BytesToUint64LittleEndian(bbBKey[:])

			// skip forward for RR_ENC_BITSET, since thats the only type we support today
			offset += 1

			// read matched result bitset size
			_, err = fd.ReadAt(bbMrSize, offset)
			if err != nil {
				log.Errorf("qid=%d, readRollupFile: failed to read mrsize for blkNum: %v, i: %v, offset: %v, err: %+v",
					qid, blkNum, i, offset, err)
				return err
			}
			offset += 2
			mrSize = utils.BytesToUint16LittleEndian(bbMrSize[:])

			bsBlk = utils.ResizeSlice(bsBlk, int(mrSize))

			// read the actual bitset
			_, err = fd.ReadAt(bsBlk[:mrSize], offset)
			if err != nil {
				if err != io.EOF {
					log.Errorf("qid=%d, readRollupFile: failed to read bitset at offset: %v, err: %+v", qid, offset, err)
					return err
				}
				break
			}
			offset += int64(mrSize)

			bs := bitset.New(0)
			err = bs.UnmarshalBinary(bsBlk[:mrSize])
			if err != nil {
				log.Errorf("qid=%d, readRollupFile: failed to unmarshall bitset, err: %+v", qid, err)
				return err
			}

			mr := pqmr.CreatePQMatchResultsFromBs(bs)
			rr := &writer.RolledRecs{MatchedRes: mr}
			toxRollup[bKey] = rr
		}
	}
	return nil
}
