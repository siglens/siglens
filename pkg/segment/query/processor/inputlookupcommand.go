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
	"strings"

	"github.com/siglens/siglens/pkg/config"
	"github.com/siglens/siglens/pkg/segment/query/iqr"
	"github.com/siglens/siglens/pkg/segment/structs"
	"github.com/siglens/siglens/pkg/segment/utils"
	putils "github.com/siglens/siglens/pkg/utils"
)

type inputlookupProcessor struct {
	options   *structs.InputLookup
	eof       bool
	qid       uint64
	start     uint64
	processed uint64
}

func checkCSVFormat(filename string) bool {
	return strings.HasSuffix(filename, ".csv") || strings.HasSuffix(filename, ".csv.gz")
}

func createRecord(columnNames []string, record []string) (map[string]utils.CValueEnclosure, error) {
	if len(columnNames) != len(record) {
		return nil, fmt.Errorf("createRecord: columnNames and record lengths are not equal, len(columnNames): %v, len(record): %v",
			len(columnNames), len(record))
	}
	recordMap := make(map[string]utils.CValueEnclosure)
	for i, cname := range columnNames {
		recordMap[cname] = utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: record[i]}
	}
	return recordMap, nil
}

func (p *inputlookupProcessor) Process(inpIqr *iqr.IQR) (*iqr.IQR, error) {
	if inpIqr != nil && !inpIqr.EOF {
		p.qid = inpIqr.GetQid()
		return inpIqr, nil
	}
	if p.eof {
		return nil, io.EOF
	}

	if p.options == nil {
		return nil, fmt.Errorf("inputlookupProcessor.Process: InputLookup is nil")
	}
	filename := p.options.Filename

	if !checkCSVFormat(filename) {
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

	curr := uint64(0)
	for curr < p.start {
		_, err := reader.Read()
		if err != nil {
			if err.Error() == "EOF" {
				p.eof = true
				break
			}
			return nil, fmt.Errorf("inputlookupProcessor.Process: Error skipping rows, err: %v", err)
		}
		curr++
	}

	count := p.processed
	records := map[string][]utils.CValueEnclosure{}

	for !p.eof && count < putils.MinUint64(p.options.Max, utils.QUERY_EARLY_EXIT_LIMIT) {
		count++
		curr++
		csvRecord, err := reader.Read()
		if err != nil {
			// Check if we've reached the end of the file
			if err.Error() == "EOF" {
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
	p.processed = count

	nIQR := iqr.NewIQR(p.qid)
	err = nIQR.AppendKnownValues(records)
	if err != nil {
		return nil, fmt.Errorf("inputlookupProcessor.Process: Error appending known values, err: %v", err)
	}

	if inpIqr != nil && inpIqr.NumberOfRecords() > 0 {
		err = inpIqr.Append(nIQR)
		if err != nil {
			return nil, fmt.Errorf("inputlookupProcessor.Process: Error appending iqr, err: %v", err)
		}
		return inpIqr, nil
	}

	return nIQR, nil
}

func (p *inputlookupProcessor) Rewind() {
	p.eof = false
	p.start = 0
	p.processed = 0
}

func (p *inputlookupProcessor) Cleanup() {
	// Nothing is stored in memory, so nothing to cleanup
}
