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

package processor

import (
	"compress/gzip"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/siglens/siglens/pkg/config"
	"github.com/siglens/siglens/pkg/segment/query/iqr"
	"github.com/siglens/siglens/pkg/segment/structs"
	segutils "github.com/siglens/siglens/pkg/segment/utils"
)

type inputlookupProcessor struct {
	options      *structs.InputLookup
	eof          bool
	qid          uint64
	start        uint64
	numprocessed uint64
	limit        uint64
}

func isCSVFormat(filename string) bool {
	return strings.HasSuffix(filename, ".csv") || strings.HasSuffix(filename, ".csv.gz")
}

func createRecord(columnNames []string, record []string) (map[string]segutils.CValueEnclosure, error) {
	if len(columnNames) != len(record) {
		return nil, fmt.Errorf("createRecord: columnNames and record lengths are not equal, len(columnNames): %v, len(record): %v",
			len(columnNames), len(record))
	}
	recordMap := make(map[string]segutils.CValueEnclosure)
	for i, cname := range columnNames {
		floatVal, floatErr := strconv.ParseFloat(record[i], 64)
		if floatErr != nil {
			recordMap[cname] = segutils.CValueEnclosure{Dtype: segutils.SS_DT_STRING, CVal: record[i]}
		} else {
			recordMap[cname] = segutils.CValueEnclosure{Dtype: segutils.SS_DT_FLOAT, CVal: floatVal}
		}
	}
	return recordMap, nil
}

func (p *inputlookupProcessor) skipToStartPos(reader *csv.Reader) (uint64, error) {
	curr := uint64(0)
	for curr < p.start {
		_, err := reader.Read()
		if err != nil {
			if err == io.EOF {
				p.eof = true
				return curr, err
			}
			return 0, fmt.Errorf("skipToStartPos: Error skipping rows, err: %v", err)
		}
		curr++
	}

	return curr, nil
}

func (p *inputlookupProcessor) Process(inpIqr *iqr.IQR) (*iqr.IQR, error) {
	if inpIqr != nil {
		p.qid = inpIqr.GetQID()
		return inpIqr, nil
	}
	if p.eof {
		return nil, io.EOF
	}
	if p.limit == 0 {
		p.limit = segutils.QUERY_EARLY_EXIT_LIMIT // TODO: Find a better way to deal with scroll and set limit for later inputlookups
	}

	if p.options == nil {
		return nil, fmt.Errorf("inputlookupProcessor.Process: InputLookup is nil")
	}
	filename := p.options.Filename

	if !isCSVFormat(filename) {
		return nil, fmt.Errorf("inputlookupProcessor.Process: Only .csv and .csv.gz formats are currently supported")
	}

	filePath := filepath.Join(config.GetLookupPath(), filename)

	fd, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("inputlookupProcessor.Process: Error while opening file %v, err: %v", filePath, err)
	}
	defer fd.Close()

	var reader *csv.Reader
	if strings.HasSuffix(filename, ".csv.gz") {
		gzipReader, err := gzip.NewReader(fd)
		if err != nil {
			return nil, fmt.Errorf("inputlookupProcessor.Process: Error while creating gzip reader, err: %v", err)
		}
		defer gzipReader.Close()
		reader = csv.NewReader(gzipReader)
	} else {
		reader = csv.NewReader(fd)
	}

	// read columns from first row of csv file
	columnNames, err := reader.Read()
	if err != nil {
		return nil, fmt.Errorf("inputlookupProcessor.Process: Error reading column names, err: %v", err)
	}

	curr, err := p.skipToStartPos(reader)
	if err != nil && err != io.EOF {
		return nil, fmt.Errorf("inputlookupProcessor.Process: Error skipping rows, err: %v", err)
	}

	count := uint64(0)
	records := map[string][]segutils.CValueEnclosure{}

	for !p.eof && count < min(p.options.Max, p.limit) {
		count++
		curr++
		csvRecord, err := reader.Read()
		if err != nil {
			// Check if we've reached the end of the file
			if err == io.EOF {
				p.eof = true
				break
			}
			return nil, fmt.Errorf("inputlookupProcessor.Process: Error reading record, err: %v", err)
		}

		record, err := createRecord(columnNames, csvRecord)
		if err != nil {
			return nil, fmt.Errorf("inputlookupProcessor.Process: Error creating record, err: %v", err)
		}
		if p.options.WhereExpr != nil {
			conditionPassed, err := p.options.WhereExpr.EvaluateForInputLookup(record)
			if err != nil {
				return nil, fmt.Errorf("inputlookupProcessor.Process: Error evaluating where expression, err: %v", err)
			}
			if !conditionPassed {
				continue
			}
		}
		for field, CValEnc := range record {
			records[field] = append(records[field], CValEnc)
		}
	}

	if count >= p.options.Max {
		p.eof = true
	}

	p.start = curr
	p.numprocessed += count

	newIQR := iqr.NewIQR(p.qid)
	err = newIQR.AppendKnownValues(records)
	if err != nil {
		return nil, fmt.Errorf("inputlookupProcessor.Process: Error appending known values, err: %v", err)
	}

	return newIQR, nil
}

func (p *inputlookupProcessor) Rewind() {
	p.eof = false
	p.start = p.options.Start
	p.numprocessed = 0
}

func (p *inputlookupProcessor) Cleanup() {
	// Nothing is stored in memory, so nothing to cleanup
}

func (p *inputlookupProcessor) GetFinalResultIfExists() (*iqr.IQR, bool) {
	return nil, false
}

func (p *inputlookupProcessor) IsEOF() bool {
	return p.eof
}
