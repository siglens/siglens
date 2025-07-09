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
	"container/list"
	"encoding/gob"
	"regexp"

	"github.com/caio/go-tdigest/v4"
	"github.com/siglens/go-hll"
	log "github.com/sirupsen/logrus"
)

func init() {
	gob.Register(&GobbableRegex{})
	gob.Register(&GobbableList{})
	gob.Register(&GobbableTDigest{})
	gob.Register(&GobbableHll{})
}

type GobbableRegex struct {
	rawRegex      string
	compiledRegex *regexp.Regexp
}

func (self *GobbableRegex) GetCompiledRegex() *regexp.Regexp {
	return self.compiledRegex
}

func (self *GobbableRegex) GetRawRegex() string {
	return self.rawRegex
}

func (self *GobbableRegex) SetRegex(raw string) error {
	compiled, err := regexp.Compile(raw)
	if err != nil {
		log.Errorf("SerizalizableRegex: failed to compile regex \"%v\", err=%v", raw, err)
		return err
	}

	self.rawRegex = raw
	self.compiledRegex = compiled

	return nil
}

// Implement https://pkg.go.dev/encoding/gob#GobEncoder
func (self *GobbableRegex) GobEncode() ([]byte, error) {
	return []byte(self.rawRegex), nil
}

// Implement https://pkg.go.dev/encoding/gob#GobDecoder
func (self *GobbableRegex) GobDecode(data []byte) error {
	self.compiledRegex = nil
	self.rawRegex = ""

	if len(data) == 0 {
		return nil
	}

	return self.SetRegex(string(data))
}

type GobbableList struct {
	list.List // Embedding list.List lets us use all of its methods.
}

// Implement https://pkg.go.dev/encoding/gob#GobEncoder
func (self *GobbableList) GobEncode() ([]byte, error) {
	var buffer bytes.Buffer
	encoder := gob.NewEncoder(&buffer)
	listValues := make([]interface{}, 0, self.Len())

	for element := self.Front(); element != nil; element = element.Next() {
		listValues = append(listValues, element.Value)
	}

	if err := encoder.Encode(listValues); err != nil {
		log.Errorf("GobbableList.GobEncode: failed to encode; err=%v", err)
		return nil, err
	}

	return buffer.Bytes(), nil
}

// Implement https://pkg.go.dev/encoding/gob#GobDecoder
func (self *GobbableList) GobDecode(data []byte) error {
	decoder := gob.NewDecoder(bytes.NewReader(data))

	var elements []interface{}
	if err := decoder.Decode(&elements); err != nil {
		log.Errorf("GobbableList.GobDecode: failed to decode; err=%v", err)
		return err
	}

	self.Init()
	for _, element := range elements {
		self.PushBack(element)
	}

	return nil
}

type GobbableTDigest struct {
	*tdigest.TDigest
}

// splunk uses nearest rank for less than 1000 cardinality and tdigest for > 1000 cardinality
// https://docs.splunk.com/Documentation/Splunk/9.1.1/SearchReference/Aggregatefunctions#Usage_12
const TDIGEST_COMPRESSION float64 = 1000

func CreateNewTDigest() (*GobbableTDigest, error) {
	ntd, err := tdigest.New(tdigest.Compression(TDIGEST_COMPRESSION))
	return &GobbableTDigest{TDigest: ntd}, err
}

func (td *GobbableTDigest) InsertIntoTDigest(val float64) error {
	err := td.Add(val)
	return err
}

func (td *GobbableTDigest) MergeTDigest(toMerge *GobbableTDigest) error {
	err := td.Merge(toMerge.TDigest)
	return err
}

func (td *GobbableTDigest) GetQuantile(quantVal float64) float64 {
	val := td.Quantile(quantVal)
	return val
}

func (self *GobbableTDigest) GobEncode() ([]byte, error) {
	var buffer bytes.Buffer
	encoder := gob.NewEncoder(&buffer)
	bytes := self.ToBytes([]byte{})

	if err := encoder.Encode(bytes); err != nil {
		log.Errorf("GobbableTDigest.GobEncode: failed to encode; err=%v", err)
		return nil, err
	}

	return buffer.Bytes(), nil
}

func (self *GobbableTDigest) GobDecode(data []byte) error {
	var err error
	decoder := gob.NewDecoder(bytes.NewReader(data))

	var bytes []byte
	if err = decoder.Decode(&bytes); err != nil {
		log.Errorf("GobbableTDigest.GobDecode: failed to decode; err=%v", err)
		return err
	}

	err = self.TDigest.FromBytes(bytes)
	if err != nil {
		log.Errorf("GobbableTDigest.GobDecode: failed to create TDigest struct from bytes; err=%v", err)
	}

	return nil
}

type GobbableHll struct {
	hll.Hll
}

func (self *GobbableHll) GobEncode() ([]byte, error) {
	var buffer bytes.Buffer
	encoder := gob.NewEncoder(&buffer)
	bytes := self.ToBytes()

	if err := encoder.Encode(bytes); err != nil {
		log.Errorf("GobbableHll.GobEncode: failed to encode; err=%v", err)
		return nil, err
	}

	return buffer.Bytes(), nil
}

func (self *GobbableHll) GobDecode(data []byte) error {
	var err error
	decoder := gob.NewDecoder(bytes.NewReader(data))

	var bytes []byte
	if err = decoder.Decode(&bytes); err != nil {
		log.Errorf("GobbableHll.GobDecode: failed to decode; err=%v", err)
		return err
	}

	self.Hll, err = hll.FromBytes(bytes)
	if err != nil {
		log.Errorf("GobbableHll.GobDecode: failed to create Hll from bytes; err=%v", err)
		return err
	}

	return nil
}
