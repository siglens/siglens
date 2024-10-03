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

package structs

import (
	"fmt"

	"github.com/siglens/siglens/pkg/ast/pipesearch"
	"github.com/siglens/siglens/pkg/segment/reader/record"
	"github.com/siglens/siglens/pkg/segment/utils"
	toputils "github.com/siglens/siglens/pkg/utils"
	log "github.com/sirupsen/logrus"
)

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
	knownValues    map[string][]utils.CValueEnclosure
	deletedColumns map[string]struct{}
	renamedColumns map[string]string

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
	}
}

func (iqr *IQR) validate() error {
	if iqr == nil {
		return fmt.Errorf("IQR is nil")
	}

	if iqr.mode == invalidMode {
		return fmt.Errorf("IQR.mode is invalid")
	}

	for cname := range iqr.knownValues {
		if len(iqr.knownValues[cname]) != len(iqr.rrcs) && len(iqr.rrcs) != 0 {
			if _, ok := iqr.deletedColumns[cname]; !ok {
				return fmt.Errorf("knownValues for column %s has %v values, but there are %v RRCs",
					cname, len(iqr.knownValues[cname]), len(iqr.rrcs))
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
		log.Errorf("IQR.AppendRRCs: invalid state: %v", err)
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
		log.Errorf("IQR.AppendRRCs: error merging encodings: %v", err)
		return err
	}

	iqr.rrcs = append(iqr.rrcs, rrcs...)

	return nil
}

func (iqr *IQR) AppendKnownValues(knownValues map[string][]utils.CValueEnclosure) error {
	if err := iqr.validate(); err != nil {
		log.Errorf("IQR.AppendKnownValues: invalid state: %v", err)
		return err
	}

	if iqr.mode == notSet {
		// We have no RRCs, so these values don't correspond to RRCs.
		iqr.mode = withoutRRCs
	}

	numExistingRecords := iqr.numberOfRecords()

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

func (iqr *IQR) numberOfRecords() int {
	if err := iqr.validate(); err != nil {
		log.Errorf("IQR.numberOfRecords: invalid state: %v", err)
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
		log.Errorf("IQR.numberOfRecords: unexpected mode %v", iqr.mode)
		return 0
	}
}

func (iqr *IQR) mergeEncodings(segEncToKey map[uint16]string) error {
	// Verify the new encodings don't conflict with the existing ones.
	for encoding, newSegKey := range segEncToKey {
		if existingSegKey, ok := iqr.encodingToSegKey[encoding]; ok {
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
		log.Errorf("IQR.ReadAllColumns: invalid state: %v", err)
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
		log.Errorf("IQR.ReadColumn: invalid state: %v", err)
		return nil, err
	}

	if iqr.mode == notSet {
		return nil, fmt.Errorf("IQR.ReadColumn: mode not set")
	}

	if _, ok := iqr.deletedColumns[cname]; ok {
		return nil, fmt.Errorf("IQR.ReadColumn: column %s is deleted", cname)
	}

	if newCname, ok := iqr.renamedColumns[cname]; ok {
		cname = newCname
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
			log.Errorf("IQR.readAllColumnsWithRRCs: unknown encoding %v", rrcs[0].SegKeyInfo.SegKeyEnc)
			return nil
		}

		vTable := iqr.rrcs[0].VirtualTableName
		colToValues, err := record.ReadAllColsForRRCs(segKey, vTable, rrcs, iqr.qid)
		if err != nil {
			log.Errorf("IQR.readAllColumnsWithRRCs: error reading all columns for segKey %v; err=%v",
				segKey, err)
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

	return results, nil
}

func (iqr *IQR) readColumnWithRRCs(cname string) ([]utils.CValueEnclosure, error) {
	// Prepare to call BatchProcess().
	getBatchKey := func(rrc *utils.RecordResultContainer) uint16 {
		return rrc.SegKeyInfo.SegKeyEnc
	}
	batchKeyLess := toputils.NewUnsetOption[func(uint16, uint16) bool]()
	batchOperation := func(rrcs []*utils.RecordResultContainer) []utils.CValueEnclosure {
		if len(rrcs) == 0 {
			return nil
		}

		segKey, ok := iqr.encodingToSegKey[rrcs[0].SegKeyInfo.SegKeyEnc]
		if !ok {
			log.Errorf("IQR.readColumnWithRRCs: unknown encoding %v", rrcs[0].SegKeyInfo.SegKeyEnc)
			return nil
		}

		values, err := record.ReadColForRRCs(segKey, rrcs, cname, iqr.qid)
		if err != nil {
			log.Errorf("IQR.readColumnWithRRCs: error reading column %s: %v", cname, err)
			return nil
		}

		return values
	}

	results := toputils.BatchProcess(iqr.rrcs, getBatchKey, batchKeyLess, batchOperation)

	if len(results) != len(iqr.rrcs) {
		// This will happen if we got an error in the batch operation.
		return nil, toputils.TeeErrorf("IQR.readColumnWithRRCs: expected %v results, got %v",
			len(iqr.rrcs), len(results))
	}

	// TODO: should we have an option to disable this caching?
	iqr.knownValues[cname] = results

	return results, nil
}

func (iqr *IQR) AsResult() (*pipesearch.PipeSearchResponseOuter, error) {
	if err := iqr.validate(); err != nil {
		log.Errorf("IQR.AsResult: invalid state: %v", err)
		return nil, err
	}

	var records map[string][]utils.CValueEnclosure
	var err error
	switch iqr.mode {
	case notSet:
		// There's no data.
		return nil, nil
	case withRRCs:
		records, err = iqr.readAllColumnsWithRRCs()
		if err != nil {
			log.Errorf("IQR.AsResult: error reading all columns: %v", err)
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

	response := &pipesearch.PipeSearchResponseOuter{
		Hits: pipesearch.PipeSearchResponse{
			TotalMatched: iqr.numberOfRecords(),
			Hits:         recordsAsAny,
		},
		AllPossibleColumns: toputils.GetKeysOfMap(records),
		Errors:             nil,
		Qtype:              "logs-query", // TODO: handle stats queries
		CanScrollMore:      false,
		ColumnsOrder:       toputils.GetSortedStringKeys(records),
	}

	return response, nil
}
