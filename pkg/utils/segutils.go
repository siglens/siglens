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

	"github.com/cespare/xxhash"
	"github.com/rogpeppe/fastuuid"
)

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

func CreateStreamId(indexName string, orgId uint64) string {
	// todo this still has a issue of having 50 shards per index, we need to cap it somehow
	return fmt.Sprintf("%d-%v-%v", rand.Intn(MAX_SHARDS), orgId, xxhash.Sum64String(indexName))
}

func CreateStreamIdForMetrics(mname *string) string {
	return fmt.Sprintf("%d", xxhash.Sum64String(*mname)%uint64(MAX_SHARDS))
}

func CreateUniqueIndentifier() string {
	return UUID_GENERATOR.Hex128()
}

func Max(x, y uint64) uint64 {
	if x < y {
		return y
	}
	return x
}

func Min(x, y uint64) uint64 {
	if x > y {
		return y
	}
	return x
}

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
func IsSubWordPresent(haystack []byte, needle []byte) bool {
	needleLen := len(needle)
	haystackLen := len(haystack)

	if needleLen > haystackLen {
		return false
	} else if needleLen == haystackLen {
		return bytes.Equal(needle, haystack)
	}

	for i := 0; i < haystackLen-needleLen+1; i += 1 {
		if haystack[i] == needle[0] {
			for j := needleLen - 1; j >= 1; j -= 1 {
				if haystack[i+j] != needle[j] {
					break
				}
				if j == 1 {
					// haystack[i:i+needleLen-1] was matched
					// we need to check if haystack[i - 1] is a whitespace and if haystack[i + needleLen] is a whitespace
					if i-1 >= 0 && haystack[i-1] != single_whitespace[0] {
						break
					}
					if i+needleLen < haystackLen && haystack[i+needleLen] != single_whitespace[0] {
						break
					}
					return true
				}
			}
		}
	}

	return false
}
