package processor

import (
	"io"

	"github.com/siglens/siglens/pkg/segment/query/iqr"
)

type mergeProcessor struct {
	currentResults *iqr.IQR
}

func (p *mergeProcessor) Process(nextIQR *iqr.IQR) (*iqr.IQR, error) {
	if nextIQR == nil {
		return p.currentResults, io.EOF
	}

	if p.currentResults == nil {
		p.currentResults = nextIQR
		return nil, nil
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
