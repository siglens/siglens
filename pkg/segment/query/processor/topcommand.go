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
	"io"

	"github.com/siglens/siglens/pkg/segment/query/iqr"
	"github.com/siglens/siglens/pkg/segment/structs"
	toputils "github.com/siglens/siglens/pkg/utils"
)

type topProcessor struct {
	options *structs.StatisticExpr
}

func (p *topProcessor) Process(iqr *iqr.IQR) (*iqr.IQR, error) {
	if p.options == nil {
		return nil, toputils.TeeErrorf("topProcessor.Process: options is nil")
	}

	// countIsGroupByCol := toputils.SliceContainsString(p.options.GetFields(), p.options.StatisticOptions.CountField)
	// percentIsGroupByCol := toputils.SliceContainsString(p.options.GetFields(), p.options.StatisticOptions.PercentField)

	// resultCount := iqr.NumberOfRecords()

	// groupByCols := p.options.GetFields()

	return iqr, io.EOF
}

func (p *topProcessor) Rewind() {
	// Nothing to do
}

func (p *topProcessor) Cleanup() {
	// Nothing to do
}

func (p *topProcessor) GetFinalResultIfExists() (*iqr.IQR, bool) {
	return nil, false
}
