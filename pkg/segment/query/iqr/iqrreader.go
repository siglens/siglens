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

package iqr

import (
	"fmt"
	"reflect"
	"sync"

	"github.com/siglens/siglens/pkg/segment/reader/record"
	"github.com/siglens/siglens/pkg/segment/utils"
	log "github.com/sirupsen/logrus"
)

type ReaderMode uint8

const (
	ReaderModeSingleReader ReaderMode = iota
	ReaderModeMultiReader
)

type IQRReader struct {
	readerMode            ReaderMode
	readerIdToReader      map[utils.T_SegReaderId]record.RRCsReaderI
	encodingToReaderIndex map[utils.T_SegEncoding]utils.T_SegReaderId
	reader                record.RRCsReaderI
}

type rrcWithIndex struct {
	rrc   *utils.RecordResultContainer
	index int
}

type readerResult struct {
	readerId    utils.T_SegReaderId
	knownValues map[string][]utils.CValueEnclosure
	err         error
}

func NewIQRReader(reader record.RRCsReaderI) *IQRReader {
	iqrRdr := &IQRReader{
		readerMode:            ReaderModeSingleReader,
		readerIdToReader:      make(map[utils.T_SegReaderId]record.RRCsReaderI),
		encodingToReaderIndex: make(map[utils.T_SegEncoding]utils.T_SegReaderId),
		reader:                reader,
	}

	return iqrRdr
}

func NewMultiIQRReader() *IQRReader {
	iqrRdr := &IQRReader{
		readerMode:            ReaderModeMultiReader,
		readerIdToReader:      make(map[utils.T_SegReaderId]record.RRCsReaderI),
		encodingToReaderIndex: make(map[utils.T_SegEncoding]utils.T_SegReaderId),
	}

	return iqrRdr
}

func (iqrRdr *IQRReader) IsSingleReader() bool {
	return iqrRdr.readerMode == ReaderModeSingleReader
}

func (iqrRdr *IQRReader) IsMultiReader() bool {
	return iqrRdr.readerMode == ReaderModeMultiReader
}

func (iqrRdr *IQRReader) AddReader(readerId utils.T_SegReaderId, reader record.RRCsReaderI) error {
	if !iqrRdr.IsMultiReader() {
		return fmt.Errorf("iqrReader.AddReader: reader mode is not multi reader: %v", iqrRdr.readerMode)
	}

	if reader, ok := iqrRdr.readerIdToReader[readerId]; ok {
		if reflect.TypeOf(reader) != reflect.TypeOf(reader) {
			return fmt.Errorf("iqrReader.AddReader: readerId=%v already exists", readerId)
		}

		return nil
	}

	iqrRdr.readerIdToReader[readerId] = reader

	return nil
}

func (iqrRdr *IQRReader) AddSegEncodingToReader(segEnc utils.T_SegEncoding, readerId utils.T_SegReaderId) error {
	if !iqrRdr.IsMultiReader() {
		return fmt.Errorf("iqrReader.AddSegEncodingToReader: reader mode is not multi reader: %v", iqrRdr.readerMode)
	}

	if _, ok := iqrRdr.readerIdToReader[readerId]; !ok {
		return fmt.Errorf("iqrReader.AddSegEncodingToReader: readerId=%v not found", readerId)
	}

	if ri, ok := iqrRdr.encodingToReaderIndex[segEnc]; ok {
		if ri != readerId {
			return fmt.Errorf("iqrReader.AddSegEncodingToReader: segEnc=%v already exists for readerId=%v", segEnc, ri)
		}

		return nil
	}

	iqrRdr.encodingToReaderIndex[segEnc] = readerId

	return nil
}

func (iqrRdr *IQRReader) AddSingleIQRReader(iqr *IQR) error {
	if !iqrRdr.IsMultiReader() {
		return fmt.Errorf("iqrReader.AddSingleIQRReader: reader mode is not multi reader: %v", iqrRdr.readerMode)
	}

	otherRdr := iqr.reader
	if !otherRdr.IsSingleReader() {
		return fmt.Errorf("iqrReader.AddSingleIQRReader: other reader is not single reader: %v", otherRdr.readerMode)
	}

	if len(iqr.encodingToSegKey) == 0 {
		return nil
	}

	otherReaderId := otherRdr.reader.GetReaderId()

	err := iqrRdr.AddReader(otherReaderId, otherRdr.reader)
	if err != nil {
		return fmt.Errorf("iqrReader.AddSingleIQRReader: cannot add reader to iqr; err=%v", err)
	}

	for segEnc := range iqr.encodingToSegKey {
		if err := iqrRdr.AddSegEncodingToReader(segEnc, otherReaderId); err != nil {
			return fmt.Errorf("iqrReader.AddSingleIQRReader: cannot add seg encoding to reader; err=%v", err)
		}
	}

	return nil
}

func (iqr *IQR) mergeIQRReaders(otherIQR *IQR) error {
	iqrRdr := iqr.reader
	otherRdr := otherIQR.reader

	if iqrRdr.IsSingleReader() && otherRdr.IsSingleReader() {
		if reflect.TypeOf(iqrRdr) == reflect.TypeOf(otherRdr) {
			return nil
		}

		resultRdr := NewMultiIQRReader()

		err := resultRdr.AddSingleIQRReader(iqr)
		if err != nil {
			return fmt.Errorf("iqrReader.MergeIQRReaders: cannot add iqr to resultRdr; err=%v", err)
		}

		err = resultRdr.AddSingleIQRReader(otherIQR)
		if err != nil {
			return fmt.Errorf("iqrReader.MergeIQRReaders: cannot add otherIQR to resultRdr; err=%v", err)
		}

		iqr.reader = resultRdr
	}

	if iqrRdr.IsSingleReader() {
		iqrRdr.readerMode = ReaderModeMultiReader
		if err := iqrRdr.AddSingleIQRReader(otherIQR); err != nil {
			return fmt.Errorf("iqrReader.MergeIQRReaders: cannot add otherIQR to iqr; err=%v", err)
		}
	}

	if otherRdr.IsSingleReader() {
		if err := iqrRdr.AddSingleIQRReader(otherIQR); err != nil {
			return fmt.Errorf("iqrReader.MergeIQRReaders: cannot add otherIQR to iqr; err=%v", err)
		}
	} else {
		for readerId, reader := range otherRdr.readerIdToReader {
			if err := iqrRdr.AddReader(readerId, reader); err != nil {
				return fmt.Errorf("iqrReader.MergeIQRReaders: cannot add reader to iqr; err=%v", err)
			}
		}

		for segEnc, readerId := range otherRdr.encodingToReaderIndex {
			if err := iqrRdr.AddSegEncodingToReader(segEnc, readerId); err != nil {
				return fmt.Errorf("iqrReader.MergeIQRReaders: cannot add seg encoding to reader; err=%v", err)
			}
		}
	}

	return nil
}

func (iqrRdr *IQRReader) validate() error {
	if iqrRdr.IsSingleReader() {
		if iqrRdr.reader == nil {
			return fmt.Errorf("iqrReader.validate: reader is nil in single reader mode")
		}
		return nil
	}

	for segEnc, readerId := range iqrRdr.encodingToReaderIndex {
		if _, ok := iqrRdr.readerIdToReader[readerId]; !ok {
			return fmt.Errorf("iqrReader.validate: readerId=%v not found for segEnc=%v", readerId, segEnc)
		}
	}

	return nil
}

func (iqrRdr *IQRReader) readColumnsForRRCs(segKey string, vTable string, rrcs []*utils.RecordResultContainer,
	qid uint64, ignoredCols map[string]struct{}, columnName *string) (map[string][]utils.CValueEnclosure, error) {
	if len(rrcs) == 0 {
		return nil, nil
	}

	if iqrRdr.IsSingleReader() {
		return nil, fmt.Errorf("iqrReader.ReadAllColsForRRCs: single reader mode not supported")
	}

	rrcReaderGroups := make(map[utils.T_SegReaderId][]*rrcWithIndex)

	for readerId := range iqrRdr.readerIdToReader {
		rrcReaderGroups[readerId] = make([]*rrcWithIndex, 0, len(rrcs))
	}

	for _, rrc := range rrcs {
		rrcReaderGroups[rrc.SegKeyInfo.ReaderId] = append(rrcReaderGroups[rrc.SegKeyInfo.ReaderId], &rrcWithIndex{rrc: rrc, index: 0})
	}

	resultChan := make(chan readerResult, len(rrcReaderGroups))

	wg := sync.WaitGroup{}

	for readerId, rrcsWithIndex := range rrcReaderGroups {
		reader := iqrRdr.readerIdToReader[readerId]

		wg.Add(1)
		go func(readerId utils.T_SegReaderId, rrcsWithIndex []*rrcWithIndex, reader record.RRCsReaderI) {
			defer wg.Done()

			rrcs := make([]*utils.RecordResultContainer, len(rrcsWithIndex))

			for i, rrcWithIndex := range rrcsWithIndex {
				rrcs[i] = rrcWithIndex.rrc
			}

			var knownValues map[string][]utils.CValueEnclosure
			var err error

			if columnName != nil {
				var values []utils.CValueEnclosure
				values, err = reader.ReadColForRRCs(segKey, rrcs, *columnName, qid)
				if err == nil {
					knownValues = map[string][]utils.CValueEnclosure{
						*columnName: values,
					}
				}
			} else {
				knownValues, err = reader.ReadAllColsForRRCs(segKey, vTable, rrcs, qid, ignoredCols)
			}

			resultChan <- readerResult{
				readerId:    readerId,
				knownValues: knownValues,
				err:         fmt.Errorf("readerId=%v; err=%v", readerId, err),
			}

		}(readerId, rrcsWithIndex, reader)
	}

	go func() {
		wg.Wait()
		close(resultChan)
	}()

	readerResultGroups := make(map[utils.T_SegReaderId]map[string][]utils.CValueEnclosure)
	var errors []error

	defer func() {
		if len(errors) > 0 {
			log.Errorf("iqrReader.ReadAllColsForRRCs: errors=%v", errors)
		}
	}()

	for result := range resultChan {
		if result.err != nil {
			errors = append(errors, result.err)
			continue
		}

		readerResultGroups[result.readerId] = result.knownValues
	}

	finalKnownValues := make(map[string][]utils.CValueEnclosure)

	for _, knownValues := range readerResultGroups {
		for cname := range knownValues {
			if _, ok := finalKnownValues[cname]; !ok {
				finalKnownValues[cname] = make([]utils.CValueEnclosure, len(rrcs))
			}
		}
	}

	for readerId, rrcsWithIndex := range rrcReaderGroups {
		knownValues := readerResultGroups[readerId]

		if len(knownValues) != len(rrcsWithIndex) {
			return nil, fmt.Errorf("iqrReader.ReadAllColsForRRCs: readerId=%v; len(knownValues)=%v != len(rrcsWithIndex)=%v",
				readerId, len(knownValues), len(rrcsWithIndex))
		}

		for i, indexedRRC := range rrcsWithIndex {
			for cname, values := range knownValues {
				finalKnownValues[cname][indexedRRC.index] = values[i]
			}
		}
	}

	return finalKnownValues, nil
}

func (iqrRdr *IQRReader) ReadAllColsForRRCs(segKey string, vTable string, rrcs []*utils.RecordResultContainer,
	qid uint64, ignoredCols map[string]struct{}) (map[string][]utils.CValueEnclosure, error) {

	if iqrRdr.IsSingleReader() {
		return iqrRdr.reader.ReadAllColsForRRCs(segKey, vTable, rrcs, qid, ignoredCols)
	}

	knownValues, err := iqrRdr.readColumnsForRRCs(segKey, vTable, rrcs, qid, ignoredCols, nil)
	if err != nil {
		return nil, fmt.Errorf("iqrReader.ReadAllColsForRRCs: cannot read columns for segKey=%v; err=%v", segKey, err)
	}

	return knownValues, nil
}

func (iqrRdr *IQRReader) getColumnsForSegKey(segKey string, vTable string, segEnc utils.T_SegEncoding) (map[string]struct{}, error) {
	if iqrRdr.IsSingleReader() {
		return iqrRdr.reader.GetColsForSegKey(segKey, vTable)
	}

	readerId, ok := iqrRdr.encodingToReaderIndex[segEnc]
	if !ok {
		return nil, fmt.Errorf("iqrReader.GetColsForSegKey: readerID not found for segKey=%v", segKey)
	}

	reader, ok := iqrRdr.readerIdToReader[readerId]
	if !ok {
		return nil, fmt.Errorf("iqrReader.GetColsForSegKey: reader not found for readerID=%v, segkey=%v", readerId, segKey)
	}

	return reader.GetColsForSegKey(segKey, vTable)
}

func (iqrRdr *IQRReader) GetColsForSegKey(segKey string, vTable string) (map[string]struct{}, error) {
	if iqrRdr.IsSingleReader() {
		return iqrRdr.reader.GetColsForSegKey(segKey, vTable)
	} else {
		return nil, fmt.Errorf("iqrReader.GetColsForSegKey: not supported in multi-reader mode")
	}
}

func (iqrRdr *IQRReader) ReadColForRRCs(segKey string, rrcs []*utils.RecordResultContainer, cname string, qid uint64) ([]utils.CValueEnclosure, error) {
	if iqrRdr.IsSingleReader() {
		return iqrRdr.reader.ReadColForRRCs(segKey, rrcs, cname, qid)
	}

	knownValues, err := iqrRdr.readColumnsForRRCs(segKey, "", rrcs, qid, nil, &cname)
	if err != nil {
		return nil, fmt.Errorf("iqrReader.ReadColForRRCs: cannot read column %v for segKey=%v; err=%v", cname, segKey, err)
	}

	if values, ok := knownValues[cname]; ok {
		return values, nil
	}

	return nil, fmt.Errorf("iqrReader.ReadColForRRCs: column %v not found for segKey=%v", cname, segKey)
}
