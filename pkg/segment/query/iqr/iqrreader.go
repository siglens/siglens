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

	"github.com/siglens/siglens/pkg/segment/reader/record"
	segutils "github.com/siglens/siglens/pkg/segment/utils"
)

type ReaderMode uint8

const (
	ReaderModeSingleReader ReaderMode = iota
	ReaderModeMultiReader
)

type IQRReader struct {
	readerMode         ReaderMode
	readerIdToReader   map[segutils.T_SegReaderId]record.RRCsReaderI
	encodingToReaderId map[segutils.T_SegEncoding]segutils.T_SegReaderId
	reader             record.RRCsReaderI
}

func NewIQRReader(reader record.RRCsReaderI) *IQRReader {
	iqrRdr := &IQRReader{
		readerMode:         ReaderModeSingleReader,
		readerIdToReader:   make(map[segutils.T_SegReaderId]record.RRCsReaderI),
		encodingToReaderId: make(map[segutils.T_SegEncoding]segutils.T_SegReaderId),
		reader:             reader,
	}

	return iqrRdr
}

func NewMultiIQRReader() *IQRReader {
	iqrRdr := &IQRReader{
		readerMode:         ReaderModeMultiReader,
		readerIdToReader:   make(map[segutils.T_SegReaderId]record.RRCsReaderI),
		encodingToReaderId: make(map[segutils.T_SegEncoding]segutils.T_SegReaderId),
	}

	return iqrRdr
}

func (iqrRdr *IQRReader) IsSingleReader() bool {
	return iqrRdr.readerMode == ReaderModeSingleReader
}

func (iqrRdr *IQRReader) IsMultiReader() bool {
	return iqrRdr.readerMode == ReaderModeMultiReader
}

func (rdrMode ReaderMode) String() string {
	switch rdrMode {
	case ReaderModeSingleReader:
		return "SingleReader"
	case ReaderModeMultiReader:
		return "MultiReader"
	default:
		return fmt.Sprintf("UnknownReaderMode(%d)", rdrMode)
	}
}

func (iqrRdr *IQRReader) AddReader(readerId segutils.T_SegReaderId, reader record.RRCsReaderI) error {
	if !iqrRdr.IsMultiReader() {
		return fmt.Errorf("iqrReader.AddReader: reader mode is not multi reader: %v", iqrRdr.readerMode)
	}

	if rdr, ok := iqrRdr.readerIdToReader[readerId]; ok {
		if readerId != rdr.GetReaderId() {
			return fmt.Errorf("iqrReader.AddReader: readerId=%v already exists", readerId)
		}

		return nil
	}

	iqrRdr.readerIdToReader[readerId] = reader

	return nil
}

func (iqrRdr *IQRReader) AddSegEncodingToReader(segEnc segutils.T_SegEncoding, readerId segutils.T_SegReaderId) error {
	if !iqrRdr.IsMultiReader() {
		return fmt.Errorf("iqrReader.AddSegEncodingToReader: reader mode is not multi reader: %v", iqrRdr.readerMode)
	}

	if _, ok := iqrRdr.readerIdToReader[readerId]; !ok {
		return fmt.Errorf("iqrReader.AddSegEncodingToReader: readerId=%v not found", readerId)
	}

	if ri, ok := iqrRdr.encodingToReaderId[segEnc]; ok {
		if ri != readerId {
			return fmt.Errorf("iqrReader.AddSegEncodingToReader: segEnc=%v already exists for readerId=%v", segEnc, ri)
		}

		return nil
	}

	iqrRdr.encodingToReaderId[segEnc] = readerId

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
		if reflect.DeepEqual(iqrRdr, otherRdr) {
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

		return nil
	}

	if iqrRdr.IsSingleReader() {
		// IQR is single Reader and otherIQR is multiReader
		err := otherRdr.AddSingleIQRReader(iqr)
		if err != nil {
			return fmt.Errorf("iqrReader.MergeIQRReaders: cannot add iqr to otherIQR; err=%v", err)
		}
		iqr.reader = otherRdr
	} else if otherRdr.IsSingleReader() {
		// IQR is multiReader and otherIQR is singleReader
		if err := iqrRdr.AddSingleIQRReader(otherIQR); err != nil {
			return fmt.Errorf("iqrReader.MergeIQRReaders: cannot add otherIQR to iqr; err=%v", err)
		}
	} else {
		// IQR is multiReader and otherIQR is multiReader
		for readerId, reader := range otherRdr.readerIdToReader {
			if err := iqrRdr.AddReader(readerId, reader); err != nil {
				return fmt.Errorf("iqrReader.MergeIQRReaders: cannot add reader to iqr; err=%v", err)
			}
		}

		for segEnc, readerId := range otherRdr.encodingToReaderId {
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

	for segEnc, readerId := range iqrRdr.encodingToReaderId {
		if _, ok := iqrRdr.readerIdToReader[readerId]; !ok {
			return fmt.Errorf("iqrReader.validate: readerId=%v not found for segEnc=%v", readerId, segEnc)
		}
	}

	return nil
}

func (iqrRdr *IQRReader) readColumnsForRRCs(segKey string, vTable string, rrcs []*segutils.RecordResultContainer,
	qid uint64, ignoredCols map[string]struct{}, columnName *string) (map[string][]segutils.CValueEnclosure, error) {
	if len(rrcs) == 0 {
		return nil, nil
	}

	if iqrRdr.IsSingleReader() {
		return nil, fmt.Errorf("iqrReader.ReadAllColsForRRCs: single reader mode not supported")
	}

	segEnc := rrcs[0].SegKeyInfo.SegKeyEnc
	readerId, ok := iqrRdr.encodingToReaderId[segEnc]
	if !ok {
		return nil, fmt.Errorf("iqrReader.ReadAllColsForRRCs: readerId not found for segKey=%v, segEnc=%v", segKey, segEnc)
	}

	reader, ok := iqrRdr.readerIdToReader[readerId]
	if !ok {
		return nil, fmt.Errorf("iqrReader.ReadAllColsForRRCs: reader not found for readerID=%v, segkey=%v, segEnc=%v", readerId, segKey, segEnc)
	}

	var knownValues map[string][]segutils.CValueEnclosure
	var err error

	if columnName != nil {
		var values []segutils.CValueEnclosure
		values, err = reader.ReadColForRRCs(segKey, rrcs, *columnName, qid, true)
		if err == nil {
			knownValues = map[string][]segutils.CValueEnclosure{
				*columnName: values,
			}
		}
	} else {
		knownValues, err = reader.ReadAllColsForRRCs(segKey, vTable, rrcs, qid, ignoredCols)
	}

	if err != nil {
		return nil, fmt.Errorf("iqrReader.ReadAllColsForRRCs: cannot read columns for segKey=%v; err=%w", segKey, err)
	}

	return knownValues, nil
}

func (iqrRdr *IQRReader) ReadAllColsForRRCs(segKey string, vTable string, rrcs []*segutils.RecordResultContainer,
	qid uint64, ignoredCols map[string]struct{}) (map[string][]segutils.CValueEnclosure, error) {
	if iqrRdr.IsSingleReader() {
		return iqrRdr.reader.ReadAllColsForRRCs(segKey, vTable, rrcs, qid, ignoredCols)
	}

	knownValues, err := iqrRdr.readColumnsForRRCs(segKey, vTable, rrcs, qid, ignoredCols, nil)
	if err != nil {
		return nil, fmt.Errorf("iqrReader.ReadAllColsForRRCs: cannot read columns for segKey=%v; err=%v", segKey, err)
	}

	return knownValues, nil
}

func (iqrRdr *IQRReader) getColumnsForSegKey(segKey string, vTable string, segEnc segutils.T_SegEncoding) (map[string]struct{}, error) {
	if iqrRdr.IsSingleReader() {
		return iqrRdr.reader.GetColsForSegKey(segKey, vTable)
	}

	readerId, ok := iqrRdr.encodingToReaderId[segEnc]
	if !ok {
		return nil, fmt.Errorf("iqrReader.GetColsForSegKey: readerID not found for segKey=%v, segEnc=%v", segKey, segEnc)
	}

	reader, ok := iqrRdr.readerIdToReader[readerId]
	if !ok {
		return nil, fmt.Errorf("iqrReader.GetColsForSegKey: reader not found for readerID=%v, segkey=%v, segEnc=%v", readerId, segKey, segEnc)
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

func (iqrRdr *IQRReader) ReadColForRRCs(segKey string, rrcs []*segutils.RecordResultContainer, cname string, qid uint64) ([]segutils.CValueEnclosure, error) {
	if iqrRdr.IsSingleReader() {
		return iqrRdr.reader.ReadColForRRCs(segKey, rrcs, cname, qid, true)
	}

	knownValues, err := iqrRdr.readColumnsForRRCs(segKey, "", rrcs, qid, nil, &cname)
	if err != nil {
		return nil, fmt.Errorf("iqrReader.ReadColForRRCs: cannot read column %v for segKey=%v; err=%w", cname, segKey, err)
	}

	if values, ok := knownValues[cname]; ok {
		return values, nil
	}

	return nil, fmt.Errorf("iqrReader.ReadColForRRCs: column %v not found for segKey=%v", cname, segKey)
}
