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
