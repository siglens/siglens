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

package utils

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strings"

	"github.com/cespare/xxhash"
	"github.com/rogpeppe/fastuuid"
	log "github.com/sirupsen/logrus"
)

const UnescapeStackBufSize = 64
const SegmentValidityFname = "segment-validity.json"

var UUID_GENERATOR *fastuuid.Generator

var MAX_SHARDS = int(1)

var single_whitespace = []byte(" ")

func init() {
	var err error
	UUID_GENERATOR, err = fastuuid.NewGenerator()
	if err != nil {
		panic(err)
	}
}

func EncodeGOB(node any) ([]byte, error) {
	var buf bytes.Buffer
	e := gob.NewEncoder(&buf)
	err := e.Encode(node)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func DecodeGOB(raw []byte, node any) error {
	buf := bytes.NewBuffer(raw)
	d := gob.NewDecoder(buf)
	err := d.Decode(node)
	if err != nil {
		return err
	}
	return nil
}

func CreateStreamId(indexName string, orgId int64) string {
	// todo this still has a issue of having 50 shards per index, we need to cap it somehow
	return fmt.Sprintf("%d-%v-%v", rand.Intn(MAX_SHARDS), orgId, xxhash.Sum64String(indexName))
}

func CreateStreamIdForMetrics(mname *string) string {
	return fmt.Sprintf("%d", xxhash.Sum64String(*mname)%uint64(MAX_SHARDS))
}

func CreateUniqueIndentifier() string {
	return UUID_GENERATOR.Hex128()
}

func Max[T ~int | ~uint64](x, y T) T {
	if x < y {
		return y
	}
	return x
}

func Min[T ~int | ~uint64](x, y T) T {
	if x > y {
		return y
	}
	return x
}

// TODO: delete these functions (and all similar in the codebase). Use the
// generic min/max functions instead.
func MaxInt64(x, y int64) int64 {
	if x < y {
		return y
	}
	return x
}

func MinInt64(x, y int64) int64 {
	if x > y {
		return y
	}
	return x
}

func HashString(x string) string {
	return fmt.Sprintf("%d", xxhash.Sum64String(x))
}

// we are assumung that needleLen and haystackLen are both non zero
func IsSubWordPresent(haystack []byte, needle []byte, isCaseInsensitive bool) bool {
	needleLen := len(needle)
	haystackLen := len(haystack)

	if needleLen > haystackLen {
		return false
	}

	for i := 0; i <= haystackLen-needleLen; i++ {
		haystackSlice := haystack[i : i+needleLen]

		if PerformBytesEqualityCheck(isCaseInsensitive, haystackSlice, needle) {
			// haystack[i:i+needleLen-1] was matched
			// we need to check if haystack[i - 1] is a whitespace and if haystack[i + needleLen] is a whitespace
			if (i == 0 || haystack[i-1] == single_whitespace[0]) && (i+needleLen == haystackLen || haystack[i+needleLen] == single_whitespace[0]) {
				return true
			}
		}
	}

	return false
}

func IsFileForRotatedSegment(filename string) bool {
	segBaseDir, err := GetSegBaseDirFromFilename(filename)
	if err != nil {
		log.Errorf("IsFileForRotatedSegment: cannot get segBaseDir from filename=%v; err=%v", filename, err)
		return false
	}

	_, err = os.Stat(filepath.Join(segBaseDir, SegmentValidityFname))
	return err == nil
}

func GetSegBaseDirFromFilename(filename string) (string, error) {
	// Note: this is coupled to getBaseSegDir. If getBaseSegDir changes, this
	// should change too.
	// getBaseSegDir looks like path/to/data/hostid/final/index/streamid/suffix
	// where path/to/data is the base data directory.
	depthAfterFinal := 3

	const finalStr = "/final/"
	pos := strings.Index(filename, finalStr)
	if pos == -1 {
		return "", TeeErrorf("getSegBaseDirFromFilename: cannot find /final/ in %v", filename)
	}
	pos += len(finalStr)

	curDepth := 0
	for curDepth < depthAfterFinal {
		nextPos := strings.Index(filename[pos:], "/")
		if nextPos == -1 {
			return "", TeeErrorf("getSegBaseDirFromFilename: cannot find %v parts in %v after /final/",
				depthAfterFinal, filename)
		}

		pos += nextPos + 1
		curDepth += 1
	}

	return filename[:pos], nil
}

func WriteValidityFile(segBaseDir string) error {
	err := os.MkdirAll(segBaseDir, 0755)
	if err != nil {
		log.Errorf("WriteValidityFile: cannot create dir=%v; err=%v", segBaseDir, err)
		return err
	}

	f, err := os.Create(filepath.Join(segBaseDir, SegmentValidityFname))
	if err != nil {
		return err
	}
	f.Close()

	return nil
}
