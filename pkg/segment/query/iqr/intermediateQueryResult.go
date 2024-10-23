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
	"sort"

	"github.com/siglens/siglens/pkg/segment/reader/record"
	"github.com/siglens/siglens/pkg/segment/structs"
	"github.com/siglens/siglens/pkg/segment/utils"
	toputils "github.com/siglens/siglens/pkg/utils"
	log "github.com/sirupsen/logrus"
)

var backfillCVal = &utils.CValueEnclosure{
	CVal:  nil,
	Dtype: utils.SS_DT_BACKFILL,
}

type iqrMode int

const (
	invalidMode iqrMode = iota
	notSet
	withRRCs
	withoutRRCs
)

type IQR struct {
	mode iqrMode

	// Used if and only if the mode is withRRCs.
	rrcs             []*utils.RecordResultContainer
	encodingToSegKey map[uint16]string

	// Used in both modes.
	qid            uint64
	knownValues    map[string][]utils.CValueEnclosure // column name -> value for every row
	deletedColumns map[string]struct{}
	renamedColumns map[string]string // old name -> new name
	columnIndex    map[string]int

	// Used only if the mode is withoutRRCs. Sometimes not used in that mode.
	groupbyColumns []string
	measureColumns []string
}

func NewIQR(qid uint64) *IQR {
	return &IQR{
		mode:             notSet,
		qid:              qid,
		rrcs:             make([]*utils.RecordResultContainer, 0),
		encodingToSegKey: make(map[uint16]string),
		knownValues:      make(map[string][]utils.CValueEnclosure),
		deletedColumns:   make(map[string]struct{}),
		renamedColumns:   make(map[string]string),
		groupbyColumns:   make([]string, 0),
		measureColumns:   make([]string, 0),
		columnIndex:      make(map[string]int),
	}
}

func (iqr *IQR) GetQID() uint64 {
	return iqr.qid
}

func (iqr *IQR) validate() error {
	if iqr == nil {
		return fmt.Errorf("IQR is nil")
	}

	if iqr.mode == invalidMode {
		return fmt.Errorf("IQR.mode is invalid")
	}

	for cname, values := range iqr.knownValues {
		if len(values) != len(iqr.rrcs) && len(iqr.rrcs) != 0 {
			if _, ok := iqr.deletedColumns[cname]; !ok {
				return fmt.Errorf("knownValues for column %s has %v values, but there are %v RRCs",
					cname, len(values), len(iqr.rrcs))
			}
		}
	}

	return nil
}

func (iqr *IQR) AppendRRCs(rrcs []*utils.RecordResultContainer, segEncToKey map[uint16]string) error {
	if len(rrcs) == 0 {
		return nil
	}

	if err := iqr.validate(); err != nil {
		log.Errorf("qid=%v, IQR.AppendRRCs: validation failed: %v", iqr.qid, err)
		return err
	}

	switch iqr.mode {
	case notSet:
		iqr.mode = withRRCs
	case withRRCs:
		// Do nothing.
	case withoutRRCs:
		return fmt.Errorf("IQR.AppendRRCs: mode is withoutRRCs")
	default:
		return fmt.Errorf("IQR.AppendRRCs: unexpected mode %v", iqr.mode)
	}

	err := iqr.mergeEncodings(segEncToKey)
	if err != nil {
		log.Errorf("qid=%v, IQR.AppendRRCs: error merging encodings: %v", iqr.qid, err)
		return err
	}

	iqr.rrcs = append(iqr.rrcs, rrcs...)

	return nil
}

func (iqr *IQR) AppendKnownValues(knownValues map[string][]utils.CValueEnclosure) error {
	if err := iqr.validate(); err != nil {
		log.Errorf("qid=%v, IQR.AppendKnownValues: validation failed: %v", iqr.qid, err)
		return err
	}

	if iqr.mode == notSet {
		// We have no RRCs, so these values don't correspond to RRCs.
		iqr.mode = withoutRRCs
	}

	numExistingRecords := iqr.NumberOfRecords()

	for cname, values := range knownValues {
		if _, ok := iqr.deletedColumns[cname]; ok {
			return toputils.TeeErrorf("IQR.AppendKnownValues: column %s is deleted", cname)
		}

		if numExistingRecords != 0 && len(values) != numExistingRecords {
			return toputils.TeeErrorf("IQR.AppendKnownValues: expected %v records, but got %v for column %v",
				numExistingRecords, len(values), cname)
		}

		iqr.knownValues[cname] = values
	}

	return nil
}

func (iqr *IQR) NumberOfRecords() int {
	if err := iqr.validate(); err != nil {
		log.Errorf("qid=%v, IQR.NumberOfRecords: validation failed: %v", iqr.qid, err)
		return 0
	}

	switch iqr.mode {
	case notSet:
		return 0
	case withRRCs:
		return len(iqr.rrcs)
	case withoutRRCs:
		for _, values := range iqr.knownValues {
			return len(values)
		}

		return 0
	default:
		log.Errorf("qid=%v, IQR.NumberOfRecords: unexpected mode %v", iqr.qid, iqr.mode)
		return 0
	}
}

func (iqr *IQR) mergeEncodings(segEncToKey map[uint16]string) error {
	// Verify the new encodings don't conflict with the existing ones.
	for encoding, newSegKey := range segEncToKey {
		if existingSegKey, ok := iqr.encodingToSegKey[encoding]; ok && existingSegKey != newSegKey {
			return toputils.TeeErrorf("IQR.mergeEncodings: same encoding used for %v and %v",
				newSegKey, existingSegKey)
		}
	}

	// Add the new encodings to the existing ones.
	iqr.encodingToSegKey = toputils.MergeMaps(iqr.encodingToSegKey, segEncToKey)

	return nil
}

func (iqr *IQR) ReadAllColumns() (map[string][]utils.CValueEnclosure, error) {
	if err := iqr.validate(); err != nil {
		log.Errorf("qid=%v, IQR.ReadAllColumns: validation failed: %v", iqr.qid, err)
		return nil, err
	}

	switch iqr.mode {
	case notSet:
		// There's no data.
		return nil, nil
	case withRRCs:
		return iqr.readAllColumnsWithRRCs()
	case withoutRRCs:
		return iqr.knownValues, nil
	default:
		return nil, fmt.Errorf("IQR.ReadAllColumns: unexpected mode %v", iqr.mode)
	}
}

func (iqr *IQR) ReadColumn(cname string) ([]utils.CValueEnclosure, error) {
	if err := iqr.validate(); err != nil {
		log.Errorf("qid=%v, IQR.ReadColumn: validation failed: %v", iqr.qid, err)
		return nil, err
	}

	if iqr.mode == notSet {
		return nil, fmt.Errorf("IQR.ReadColumn: mode not set")
	}

	if _, ok := iqr.deletedColumns[cname]; ok {
		return nil, fmt.Errorf("IQR.ReadColumn: column %s is deleted", cname)
	}

	if values, ok := iqr.knownValues[cname]; ok {
		return values, nil
	}

	switch iqr.mode {
	case withRRCs:
		return iqr.readColumnWithRRCs(cname)
	case withoutRRCs:
		// We don't have RRCs, so we can't read the column. Since we got here
		// and didn't already return results from knownValues, we don't know
		// about this column.
		return nil, toputils.TeeErrorf("IQR.ReadColumn: invalid column %v", cname)
	default:
		return nil, fmt.Errorf("IQR.ReadColumn: unexpected mode %v", iqr.mode)
	}
}

func (iqr *IQR) readAllColumnsWithRRCs() (map[string][]utils.CValueEnclosure, error) {
	// Prepare to call BatchProcessToMap().
	getBatchKey := func(rrc *utils.RecordResultContainer) uint16 {
		return rrc.SegKeyInfo.SegKeyEnc
	}
	batchKeyLess := toputils.NewUnsetOption[func(uint16, uint16) bool]()
	batchOperation := func(rrcs []*utils.RecordResultContainer) map[string][]utils.CValueEnclosure {
		if len(rrcs) == 0 {
			return nil
		}

		segKey, ok := iqr.encodingToSegKey[rrcs[0].SegKeyInfo.SegKeyEnc]
		if !ok {
			log.Errorf("qid=%v, IQR.readAllColumnsWithRRCs: unknown encoding %v", iqr.qid, rrcs[0].SegKeyInfo.SegKeyEnc)
			return nil
		}

		vTable := rrcs[0].VirtualTableName
		colToValues, err := record.ReadAllColsForRRCs(segKey, vTable, rrcs, iqr.qid, iqr.deletedColumns)
		if err != nil {
			log.Errorf("qid=%v, IQR.readAllColumnsWithRRCs: error reading all columns for segKey %v; err=%v",
				iqr.qid, segKey, err)
			return nil
		}

		return colToValues
	}

	results := toputils.BatchProcessToMap(iqr.rrcs, getBatchKey, batchKeyLess, batchOperation)

	for _, values := range results {
		if len(values) != len(iqr.rrcs) {
			// This will happen if we got an error in the batch operation.
			return nil, toputils.TeeErrorf("IQR.readAllColumnsWithRRCs: expected %v results, got %v",
				len(iqr.rrcs), len(values))
		}
	}

	for oldName := range iqr.renamedColumns {
		// TODO: don't read these columns from the RRCs, instead of reading and
		// then deleting them.
		delete(results, oldName)
	}

	for cname, values := range iqr.knownValues {
		results[cname] = values
	}

	return results, nil
}

func (iqr *IQR) readColumnWithRRCs(cname string) ([]utils.CValueEnclosure, error) {
	// Prepare to call BatchProcess().
	getBatchKey := func(rrc *utils.RecordResultContainer) uint16 {
		return rrc.SegKeyInfo.SegKeyEnc
	}
	batchKeyLess := toputils.NewUnsetOption[func(uint16, uint16) bool]()
	batchOperation := func(rrcs []*utils.RecordResultContainer) ([]utils.CValueEnclosure, error) {
		if len(rrcs) == 0 {
			return nil, nil
		}

		segKey, ok := iqr.encodingToSegKey[rrcs[0].SegKeyInfo.SegKeyEnc]
		if !ok {
			return nil, toputils.TeeErrorf("IQR.readColumnWithRRCs: unknown encoding %v", rrcs[0].SegKeyInfo.SegKeyEnc)
		}

		values, err := record.ReadColForRRCs(segKey, rrcs, cname, iqr.qid)
		if err != nil {
			return nil, toputils.TeeErrorf("IQR.readColumnWithRRCs: error reading column %s: %v", cname, err)
		}

		return values, nil
	}

	results, err := toputils.BatchProcess(iqr.rrcs, getBatchKey, batchKeyLess, batchOperation)
	if err != nil {
		return nil, toputils.TeeErrorf("IQR.readColumnWithRRCs: error in batch operation: %v", err)
	}

	if len(results) != len(iqr.rrcs) {
		// This will happen if we got an error in the batch operation.
		return nil, toputils.TeeErrorf("IQR.readColumnWithRRCs: expected %v results, got %v",
			len(iqr.rrcs), len(results))
	}

	// TODO: should we have an option to disable this caching?
	iqr.knownValues[cname] = results

	return results, nil
}

func (iqr *IQR) Append(other *IQR) error {
	if err := iqr.validate(); err != nil {
		log.Errorf("qid=%v, IQR.Append: validation failed on self: %v", iqr.qid, err)
		return err
	}

	if other == nil {
		return nil
	}

	if err := other.validate(); err != nil {
		log.Errorf("qid=%v, IQR.Append: validation failed on other: %v", iqr.qid, err)
		return err
	}

	mergedIQR, err := mergeMetadata([]*IQR{iqr, other})
	if err != nil {
		log.Errorf("qid=%v, IQR.Append: error merging metadata: %v", iqr.qid, err)
		return err
	}

	iqr.mode = mergedIQR.mode
	iqr.encodingToSegKey = mergedIQR.encodingToSegKey

	numInitialRecords := iqr.NumberOfRecords()
	numAddedRecords := other.NumberOfRecords()
	numFinalRecords := numInitialRecords + numAddedRecords

	if iqr.mode == withRRCs {
		iqr.rrcs = append(iqr.rrcs, other.rrcs...)
	}

	for cname, values := range other.knownValues {
		if _, ok := iqr.knownValues[cname]; !ok {
			iqr.knownValues[cname] = make([]utils.CValueEnclosure, numInitialRecords+len(values))
			for i := 0; i < numInitialRecords; i++ {
				iqr.knownValues[cname][i] = *backfillCVal
			}

			copy(iqr.knownValues[cname][numInitialRecords:], values)
		} else {
			iqr.knownValues[cname] = append(iqr.knownValues[cname], values...)
		}
	}

	for cname, values := range iqr.knownValues {
		if _, ok := other.knownValues[cname]; !ok {
			iqr.knownValues[cname] = toputils.ResizeSliceWithDefault(values, numFinalRecords, *backfillCVal)
		}
	}

	return nil
}

func (iqr *IQR) getSegKeyToVirtualTableMapFromRRCs() map[string]string {
	// segKey -> virtual table name
	segKeyVTableMap := make(map[string]string)
	for _, rrc := range iqr.rrcs {
		segKey, ok := iqr.encodingToSegKey[rrc.SegKeyInfo.SegKeyEnc]
		if !ok {
			// This should never happen.
			log.Errorf("qid=%v, IQR.getSegKeysFromRRCs: unknown encoding %v", iqr.qid, rrc.SegKeyInfo.SegKeyEnc)
			continue
		}

		if _, ok := segKeyVTableMap[segKey]; ok {
			continue
		}
		segKeyVTableMap[segKey] = rrc.VirtualTableName
	}

	return segKeyVTableMap
}

func (iqr *IQR) GetColumns() (map[string]struct{}, error) {
	if err := iqr.validate(); err != nil {
		return nil, toputils.TeeErrorf("IQR.GetColumns: validation failed: %v", err)
	}

	segKeyVTableMap := iqr.getSegKeyToVirtualTableMapFromRRCs()

	allColumns := make(map[string]struct{})

	for segkey, vTable := range segKeyVTableMap {
		columns, err := record.GetColsForSegKey(segkey, vTable)
		if err != nil {
			log.Errorf("qid=%v, IQR.GetColumns: error getting columns for segKey %v: %v and Virtual Table name: %v", iqr.qid, segkey, vTable, err)
		}

		for column := range columns {
			if _, ok := iqr.deletedColumns[column]; !ok {
				allColumns[column] = struct{}{}
			}
		}
	}

	for column := range iqr.knownValues {
		if _, ok := iqr.deletedColumns[column]; !ok {
			allColumns[column] = struct{}{}
		}
	}

	return allColumns, nil
}

func (iqr *IQR) GetRecord(index int) *Record {
	return &Record{iqr: iqr, index: index}
}

func (iqr *IQR) Sort(less func(*Record, *Record) bool) error {
	if err := iqr.validate(); err != nil {
		log.Errorf("qid=%v, IQR.Sort: validation failed: %v", iqr.qid, err)
		return err
	}

	if less == nil {
		return toputils.TeeErrorf("qid=%v, IQR.Sort: the less function is nil", iqr.qid)
	}

	if iqr.mode == notSet {
		return nil
	}

	records := make([]*Record, iqr.NumberOfRecords())
	for i := 0; i < iqr.NumberOfRecords(); i++ {
		records[i] = &Record{iqr: iqr, index: i}
	}

	sort.Slice(records, func(i, j int) bool {
		return less(records[i], records[j])
	})

	if iqr.mode == withRRCs {
		newRRCs := make([]*utils.RecordResultContainer, iqr.NumberOfRecords())
		for i, record := range records {
			newRRCs[i] = iqr.rrcs[record.index]
		}

		iqr.rrcs = newRRCs
	}

	for cname, values := range iqr.knownValues {
		newValues := make([]utils.CValueEnclosure, iqr.NumberOfRecords())
		for i, record := range records {
			newValues[i] = values[record.index]
		}

		iqr.knownValues[cname] = newValues
	}

	return nil
}

// This merges multiple IQRs into one. It stops when one of the IQRs runs out
// of records, and returns the index of the IQR that ran out of records.
//
// Each record taken from an input IQR is discarded from that IQR.
//
// Each input IQR must already be sorted according to the given less function.
func MergeIQRs(iqrs []*IQR, less func(*Record, *Record) bool) (*IQR, int, error) {
	if len(iqrs) == 0 {
		return nil, 0, toputils.TeeErrorf("MergeIQRs: no IQRs to merge")
	}

	if less == nil {
		return nil, 0, toputils.TeeErrorf("MergeIQRs: the less function is nil")
	}

	iqr, err := mergeMetadata(iqrs)
	if err != nil {
		log.Errorf("qid=%v, MergeIQRs: error merging metadata: %v", iqr.qid, err)
		return nil, 0, err
	}

	nextRecords := make([]*Record, len(iqrs))
	numRecordsTaken := make([]int, len(iqrs))
	for i, iqr := range iqrs {
		nextRecords[i] = &Record{iqr: iqr, index: 0}
		numRecordsTaken[i] = 0
	}

	for {
		iqrIndex := toputils.IndexOfMin(nextRecords, less)
		numRecordsTaken[iqrIndex]++
		record := nextRecords[iqrIndex]

		// Append the record.
		if iqr.mode == withRRCs {
			iqr.rrcs = append(iqr.rrcs, record.iqr.rrcs[record.index])
		}
		for cname, values := range record.iqr.knownValues {
			if _, ok := iqr.knownValues[cname]; !ok {
				err := toputils.TeeErrorf("MergeIQRs: column %v is missing from destination IQR", cname)
				return nil, 0, err
			}
			iqr.knownValues[cname] = append(iqr.knownValues[cname], values[record.index])
		}

		// Prepare for the next iteration.
		record.index++

		// Check if this IQR is out of records.
		if iqrs[iqrIndex].NumberOfRecords() <= nextRecords[iqrIndex].index {
			// Discard all the records that were merged.
			for i, numTaken := range numRecordsTaken {
				err := iqrs[i].discard(numTaken)
				if err != nil {
					log.Errorf("qid=%v, MergeIQRs: error discarding records: %v", iqr.qid, err)
					return nil, 0, err
				}
			}

			return iqr, iqrIndex, nil
		}
	}
}

func mergeMetadata(iqrs []*IQR) (*IQR, error) {
	if len(iqrs) == 0 {
		return nil, fmt.Errorf("mergeMetadata: no IQRs to merge")
	}

	result := NewIQR(iqrs[0].qid)
	result.mode = iqrs[0].mode

	for encoding, segKey := range iqrs[0].encodingToSegKey {
		result.encodingToSegKey[encoding] = segKey
	}

	for cname := range iqrs[0].knownValues {
		result.knownValues[cname] = make([]utils.CValueEnclosure, 0)
	}

	for _, iqrToMerge := range iqrs {
		result.AddColumnsToDelete(iqrToMerge.deletedColumns)
		result.AddColumnIndex(iqrToMerge.columnIndex)
	}

	for oldName, newName := range iqrs[0].renamedColumns {
		result.renamedColumns[oldName] = newName
	}

	result.groupbyColumns = append(result.groupbyColumns, iqrs[0].groupbyColumns...)
	result.measureColumns = append(result.measureColumns, iqrs[0].measureColumns...)

	for _, iqr := range iqrs {
		err := result.mergeEncodings(iqr.encodingToSegKey)
		if err != nil {
			return nil, fmt.Errorf("mergeMetadata: error merging encodings: %v", err)
		}

		for cname := range iqr.knownValues {
			if _, ok := result.knownValues[cname]; !ok {
				result.knownValues[cname] = make([]utils.CValueEnclosure, 0)
			}
		}

		if iqr.qid != result.qid {
			return nil, fmt.Errorf("mergeMetadata: inconsistent qids (%v and %v)", iqr.qid, result.qid)
		}

		if iqr.mode != result.mode {
			if result.mode == notSet {
				result.mode = iqr.mode
			} else if iqr.mode == notSet {
				// Do nothing.
			} else {
				return nil, fmt.Errorf("qid=%v, mergeMetadata: inconsistent modes (%v and %v)",
					iqr.qid, iqr.mode, result.mode)
			}
		}

		if !reflect.DeepEqual(iqr.renamedColumns, result.renamedColumns) {
			return nil, fmt.Errorf("qid=%v, mergeMetadata: inconsistent renamed columns (%v and %v)",
				iqr.qid, iqr.renamedColumns, result.renamedColumns)
		}

		if !reflect.DeepEqual(iqr.groupbyColumns, result.groupbyColumns) {
			return nil, fmt.Errorf("qid=%v, mergeMetadata: inconsistent groupby columns (%v and %v)",
				iqr.qid, iqr.groupbyColumns, result.groupbyColumns)
		}

		if !reflect.DeepEqual(iqr.measureColumns, result.measureColumns) {
			return nil, fmt.Errorf("qid=%v, mergeMetadata: inconsistent measure columns (%v and %v)",
				iqr.qid, iqr.measureColumns, result.measureColumns)
		}
	}

	return result, nil
}

func (iqr *IQR) AddColumnsToDelete(cnames map[string]struct{}) {
	for cname := range cnames {
		iqr.deletedColumns[cname] = struct{}{}
		delete(iqr.knownValues, cname)
	}
}

func (iqr *IQR) AddColumnIndex(cnamesToIndex map[string]int) {
	for cname, index := range cnamesToIndex {
		iqr.columnIndex[cname] = index
	}
}

func (iqr *IQR) discard(numRecords int) error {
	if err := iqr.validate(); err != nil {
		log.Errorf("qid=%v, IQR.discard: validation failed: %v", iqr.qid, err)
		return err
	}

	if iqr.mode == notSet {
		return nil
	} else if iqr.mode == withRRCs {
		if numRecords > len(iqr.rrcs) {
			return fmt.Errorf("IQR.discard: trying to discard %v records, but there are only %v RRCs",
				numRecords, len(iqr.rrcs))
		}

		iqr.rrcs = iqr.rrcs[numRecords:]
	}

	for cname, values := range iqr.knownValues {
		if len(values) < numRecords {
			return fmt.Errorf("IQR.discard: trying to discard %v records, but there are only %v values for column %v",
				numRecords, len(values), cname)
		}

		iqr.knownValues[cname] = values[numRecords:]
	}

	return nil
}

func (iqr *IQR) DiscardAfter(numRecords uint64) error {
	if err := iqr.validate(); err != nil {
		log.Errorf("qid=%v, IQR.DiscardAfter: validation failed: %v", iqr.qid, err)
		return err
	}

	if numRecords > uint64(iqr.NumberOfRecords()) {
		return nil
	}

	if iqr.mode == notSet {
		return nil
	} else if iqr.mode == withRRCs {
		iqr.rrcs = iqr.rrcs[:numRecords]
	}

	for cname, values := range iqr.knownValues {
		if len(values) < int(numRecords) {
			return fmt.Errorf("IQR.DiscardAfter: trying to discard %v after records, but there are only %v values for column %v",
				numRecords, len(values), cname)
		}

		iqr.knownValues[cname] = values[:numRecords]
	}

	return nil
}

func (iqr *IQR) DiscardRows(rowsToDiscard []int) error {
	if err := iqr.validate(); err != nil {
		log.Errorf("qid=%v, IQR.DiscardRows: validation failed: %v", iqr.qid, err)
		return err
	}

	if iqr.mode == notSet {
		return nil
	}

	if iqr.mode == withRRCs {
		newRRCs, err := toputils.RemoveSortedIndices(iqr.rrcs, rowsToDiscard)
		if err != nil {
			return toputils.TeeErrorf("qid=%v, IQR.DiscardRows: error discarding rows for RRCs: %v",
				iqr.qid, err)
		}

		iqr.rrcs = newRRCs
	}

	for cname, values := range iqr.knownValues {
		newValues, err := toputils.RemoveSortedIndices(values, rowsToDiscard)
		if err != nil {
			return toputils.TeeErrorf("qid=%v, IQR.DiscardRows: error discarding rows for column %v: %v",
				iqr.qid, cname, err)
		}

		iqr.knownValues[cname] = newValues
	}

	return nil
}

func (iqr *IQR) RenameColumn(oldName, newName string) error {
	if err := iqr.validate(); err != nil {
		log.Errorf("qid=%v, IQR.RenameColumn: validation failed: %v", iqr.qid, err)
		return err
	}

	iqr.renamedColumns[oldName] = newName
	if values, ok := iqr.knownValues[oldName]; ok {
		iqr.knownValues[newName] = values
		delete(iqr.knownValues, oldName)
	}

	return nil
}

func (iqr *IQR) GetColumnsOrder(allCnames []string) []string {
	indexToCols := make(map[int][]string)
	withoutIndexCols := []string{}
	finalColumnOrder := []string{}

	// For each column find its group based on the index.
	for _, cname := range allCnames {
		if idx, isIndexAvailable := iqr.columnIndex[cname]; isIndexAvailable {
			indexToCols[idx] = append(indexToCols[idx], cname)
		} else {
			withoutIndexCols = append(withoutIndexCols, cname)
		}
	}

	// Sort the value of indexes to get the order for each group.
	allIndexes := toputils.GetKeysOfMap(indexToCols)
	sort.Ints(allIndexes)

	// Since multiple columns can have same index we will sort them lexicographically within each group.
	// Then all of these groups will be appended in the order of their index.
	for _, idx := range allIndexes {
		allColsAtIdx := indexToCols[idx]
		sort.Strings(allColsAtIdx)
		finalColumnOrder = append(finalColumnOrder, allColsAtIdx...)
	}

	// Columns without any index would be sorted lexicographically and added at the end.
	sort.Strings(withoutIndexCols)
	finalColumnOrder = append(finalColumnOrder, withoutIndexCols...)

	return finalColumnOrder
}

// TODO: Add option/method to return the result for a websocket query.
// TODO: Add option/method to return the result for an ES/kibana query.
func (iqr *IQR) AsResult(qType structs.QueryType) (*structs.PipeSearchResponseOuter, error) {
	if err := iqr.validate(); err != nil {
		log.Errorf("qid=%v, IQR.AsResult: validation failed: %v", iqr.qid, err)
		return nil, err
	}

	var records map[string][]utils.CValueEnclosure
	var err error
	switch iqr.mode {
	case notSet:
		// There's no data.
	case withRRCs:
		records, err = iqr.readAllColumnsWithRRCs()
		if err != nil {
			log.Errorf("qid=%v, IQR.AsResult: error reading all columns: %v", iqr.qid, err)
			return nil, err
		}
	case withoutRRCs:
		records = iqr.knownValues
	default:
		return nil, fmt.Errorf("IQR.AsResult: unexpected mode %v", iqr.mode)
	}

	cValRecords := toputils.TransposeMapOfSlices(records)
	recordsAsAny := make([]map[string]interface{}, len(cValRecords))
	for i, record := range cValRecords {
		recordsAsAny[i] = make(map[string]interface{})
		for key, value := range record {
			recordsAsAny[i][key] = value.CVal
		}
	}

	allCNames := toputils.GetKeysOfMap(records)

	var response *structs.PipeSearchResponseOuter
	switch qType {
	case structs.RRCCmd:
		response = &structs.PipeSearchResponseOuter{
			Hits: structs.PipeSearchResponse{
				TotalMatched: toputils.HitsCount{
					Value:    uint64(iqr.NumberOfRecords()),
					Relation: "eq",
				},
				Hits: recordsAsAny,
			},
			AllPossibleColumns: allCNames,
			Errors:             nil,
			Qtype:              qType.String(),
			CanScrollMore:      false,
			ColumnsOrder:       iqr.GetColumnsOrder(allCNames),
		}
	case structs.GroupByCmd, structs.SegmentStatsCmd:
		bucketHolderArr, aggGroupByCols, measureFuncs, bucketCount, err := iqr.getFinalStatsResults()
		if err != nil {
			return nil, toputils.TeeErrorf("qid=%v, IQR.AsResult: error getting final result for GroupBy: %v", iqr.qid, err)
		}

		response = &structs.PipeSearchResponseOuter{
			Hits: structs.PipeSearchResponse{
				TotalMatched: 0, // TODO: get the total matched records
				Hits:         nil,
			},
			AllPossibleColumns: allCNames,
			Errors:             nil,
			Qtype:              qType.String(),
			CanScrollMore:      false,
			ColumnsOrder:       iqr.GetColumnsOrder(allCNames),
			BucketCount:        bucketCount,
			MeasureFunctions:   measureFuncs,
			MeasureResults:     bucketHolderArr,
			GroupByCols:        aggGroupByCols,
			// TODO: add IsTimechart flag
		}
	default:
		return nil, fmt.Errorf("IQR.AsResult: unexpected query type %v", qType)
	}

	return response, nil
}

func (iqr *IQR) AppendStatsResults(bucketHolderArr []*structs.BucketHolder, measureFuncs []string, aggGroupByCols []string, bucketCount int) error {
	if err := iqr.validate(); err != nil {
		log.Errorf("qid=%v, IQR.AppendStatsResults: validation failed: %v", iqr.qid, err)
		return err
	}

	iqr.mode = withoutRRCs

	knownValues := make(map[string][]utils.CValueEnclosure)

	for _, aggGroupByCol := range aggGroupByCols {
		knownValues[aggGroupByCol] = make([]utils.CValueEnclosure, bucketCount)
	}
	for _, measureFunction := range measureFuncs {
		knownValues[measureFunction] = make([]utils.CValueEnclosure, bucketCount)
	}

	conversionErrors := make([]string, utils.MAX_SIMILAR_ERRORS_TO_LOG)
	errIndex := 0

	for i, bucketHolder := range bucketHolderArr {
		for idx, aggGroupByCol := range aggGroupByCols {
			colValue := bucketHolder.GroupByValues[idx]
			err := knownValues[aggGroupByCol][i].ConvertValue(colValue)
			if err != nil && errIndex < utils.MAX_SIMILAR_ERRORS_TO_LOG {
				conversionErrors[errIndex] = fmt.Sprintf("BucketHolderIndex=%v, groupByCol=%v, ColumnValue=%v. Error=%v", i, aggGroupByCol, colValue, err)
				errIndex++
			}
		}

		for _, measureFunc := range measureFuncs {
			value := bucketHolder.MeasureVal[measureFunc]
			err := knownValues[measureFunc][i].ConvertValue(value)
			if err != nil && errIndex < utils.MAX_SIMILAR_ERRORS_TO_LOG {
				conversionErrors[errIndex] = fmt.Sprintf("BucketHolderIndex=%v, measureFunc=%v, ColumnValue=%v. Error=%v", i, measureFunc, value, err)
				errIndex++
			}
		}
	}

	err := iqr.AppendKnownValues(knownValues)
	if err != nil {
		return err
	}

	iqr.groupbyColumns = append(iqr.groupbyColumns, aggGroupByCols...)
	iqr.measureColumns = append(iqr.measureColumns, measureFuncs...)

	if errIndex > 0 {
		log.Errorf("qid=%v, IQR.AppendStatsResults: conversion errors: %v", iqr.qid, conversionErrors)
	}

	return nil
}

func (iqr *IQR) getFinalStatsResults() ([]*structs.BucketHolder, []string, []string, int, error) {
	knownValues := iqr.knownValues

	if len(knownValues) == 0 {
		return nil, nil, nil, 0, fmt.Errorf("IQR.getFinalStatsResults: knownValues is empty")
	}

	if len(iqr.measureColumns) == 0 {
		return nil, nil, nil, 0, fmt.Errorf("IQR.getFinalStatsResults: measureColumns is empty")
	}

	// The bucket count is the number of rows in the final result. So we can use the length of any column.
	bucketCount := len(knownValues[iqr.measureColumns[0]])
	if bucketCount == 0 {
		return nil, nil, nil, 0, fmt.Errorf("IQR.getFinalStatsResults: bucketCount is 0")
	}

	bucketHolderArr := make([]*structs.BucketHolder, bucketCount)

	// Rename groupbyColumns and measureColumns based on the renamedColumns map
	for i, groupColName := range iqr.groupbyColumns {
		if newColName, ok := iqr.renamedColumns[groupColName]; ok {
			iqr.groupbyColumns[i] = newColName
		}
	}

	for i, measureColName := range iqr.measureColumns {
		if newColName, ok := iqr.renamedColumns[measureColName]; ok {
			iqr.measureColumns[i] = newColName
		}
	}

	groupByColLen := len(iqr.groupbyColumns)

	// Fill in the bucketHolderArr with values from knownValues
	for i := 0; i < bucketCount; i++ {

		bucketHolderArr[i] = &structs.BucketHolder{
			GroupByValues: make([]string, groupByColLen),
			MeasureVal:    make(map[string]interface{}),
		}

		for idx, aggGroupByCol := range iqr.groupbyColumns {
			colValue := knownValues[aggGroupByCol][i]
			convertedValue, err := colValue.GetString()
			if err != nil {
				return nil, nil, nil, 0, fmt.Errorf("IQR.getFinalStatsResults: conversion error for aggGroupByCol %v with value:%v. Error=%v", aggGroupByCol, colValue, err)
			}
			bucketHolderArr[i].GroupByValues[idx] = convertedValue
		}

		for _, measureFunc := range iqr.measureColumns {
			bucketHolderArr[i].MeasureVal[measureFunc] = knownValues[measureFunc][i].CVal
		}
	}

	return bucketHolderArr, iqr.groupbyColumns, iqr.measureColumns, bucketCount, nil
}
