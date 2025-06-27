// Copyright (c) 2021-2025 SigScalr, Inc.
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
)

type mergeProcessor struct {
	mergeSettings  mergeSettings
	currentResults *iqr.IQR
}

func (p *mergeProcessor) Process(nextIQR *iqr.IQR) (*iqr.IQR, error) {
	if !p.mergeSettings.mergingStats {
		// Basically a no-op. The merging happens in the DataProcessor.Fetch()
		if nextIQR == nil {
			return nil, io.EOF
		}

		return nextIQR, nil
	}

	if nextIQR == nil {
		return p.currentResults, io.EOF
	}

	if p.currentResults == nil {
		p.currentResults = nextIQR

		// The stats don't get output correctly if there's only one non-nil
		// IQR and MergeIQRStatsResults is called on it, so call it now.
		_, err := p.currentResults.MergeIQRStatsResults([]*iqr.IQR{p.currentResults})
		return nil, err
	}

	// The IQR that MergeIQRStatsResults is called on isn't automatically
	// included in the merge, so we need to include it explicitly.
	_, err := p.currentResults.MergeIQRStatsResults([]*iqr.IQR{p.currentResults, nextIQR})
	if err != nil {
		return nil, err
	}

	return nil, nil
}

func (p *mergeProcessor) Rewind() {
	p.currentResults = nil
}

func (p *mergeProcessor) Cleanup() {
	p.currentResults = nil
}

func (p *mergeProcessor) GetFinalResultIfExists() (*iqr.IQR, bool) {
	return nil, false
}
