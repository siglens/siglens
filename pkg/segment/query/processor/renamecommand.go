// Copyright (c) 2026 The SigScalr Authors.
// SPDX-License-Identifier: Apache-2.0

package processor

import (
	"fmt"
	"io"

	"github.com/siglens/siglens/pkg/segment/query/iqr"
	"github.com/siglens/siglens/pkg/segment/structs"
)

type renameProcessor struct {
	options *structs.RenameExp
}

func (p *renameProcessor) Process(iqr *iqr.IQR) (*iqr.IQR, error) {
	if p.options == nil {
		return nil, fmt.Errorf("renameProcessor.Process: options is nil")
	}
	if iqr == nil {
		return nil, io.EOF
	}

	switch p.options.RenameExprMode {
	case structs.REMPhrase, structs.REMOverride:
		for oldName, newName := range p.options.RenameColumns {
			err := iqr.RenameColumn(oldName, newName)
			if err != nil {
				return nil, fmt.Errorf("renameProcessor.Process: Error while renaming %v to %v %v", oldName, newName, err)
			}
		}
	case structs.REMRegex:
		if len(p.options.RenameColumns) != 1 {
			return nil, fmt.Errorf("renameProcessor.Process: RenameColumns should have one entry for REMRegex mode")
		}
		origPattern := ""
		newPattern := ""
		for oldName, newName := range p.options.RenameColumns {
			origPattern = oldName
			newPattern = newName
		}
		allCnames, err := iqr.GetColumns()
		if err != nil {
			return nil, fmt.Errorf("renameProcessor.Process: Error while getting column names %v", err)
		}
		for cname := range allCnames {
			newName, err := structs.ProcessRenameRegexExp(origPattern, newPattern, cname)
			if err != nil {
				return nil, fmt.Errorf("renameProcessor.Process: Error while processing regex %v", err)
			}
			if newName == "" {
				continue // No match so continue
			}
			err = iqr.RenameColumn(cname, newName)
			if err != nil {
				return nil, fmt.Errorf("renameProcessor.Process: Error while renaming %v to %v for origPattern: %v newPattern: %v", cname, newName, origPattern, newPattern)
			}
		}
	}
	return iqr, nil
}

func (p *renameProcessor) Rewind() {
	// Nothing to do
}

func (p *renameProcessor) Cleanup() {
	// Nothing to do
}

func (p *renameProcessor) GetFinalResultIfExists() (*iqr.IQR, bool) {
	return nil, false
}
