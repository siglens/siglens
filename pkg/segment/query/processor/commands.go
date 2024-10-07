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

import "github.com/siglens/siglens/pkg/segment/query/iqr"

type binProcessor struct{}

func (p *binProcessor) Process(iqr *iqr.IQR) (*iqr.IQR, error) {
	panic("not implemented")
}

func (p *binProcessor) Rewind() {} // TODO

type dedupProcessor struct{}

func (p *dedupProcessor) Process(iqr *iqr.IQR) (*iqr.IQR, error) {
	panic("not implemented")
}

func (p *dedupProcessor) Rewind() {}

type evalProcessor struct{}

func (p *evalProcessor) Process(iqr *iqr.IQR) (*iqr.IQR, error) {
	panic("not implemented")
}

func (p *evalProcessor) Rewind() {}

type fieldsProcessor struct{}

func (p *fieldsProcessor) Process(iqr *iqr.IQR) (*iqr.IQR, error) {
	panic("not implemented")
}

func (p *fieldsProcessor) Rewind() {}

type fillnullProcessor struct{}

func (p *fillnullProcessor) Process(iqr *iqr.IQR) (*iqr.IQR, error) {
	panic("not implemented")
}

func (p *fillnullProcessor) Rewind() {} // TODO

type gentimesProcessor struct{}

func (p *gentimesProcessor) Process(iqr *iqr.IQR) (*iqr.IQR, error) {
	panic("not implemented")
}

func (p *gentimesProcessor) Rewind() {}

type headProcessor struct{}

func (p *headProcessor) Process(iqr *iqr.IQR) (*iqr.IQR, error) {
	panic("not implemented")
}

func (p *headProcessor) Rewind() {}

type tailProcessor struct{}

func (p *tailProcessor) Process(iqr *iqr.IQR) (*iqr.IQR, error) {
	panic("not implemented")
}

func (p *tailProcessor) Rewind() {}

type makemvProcessor struct{}

func (p *makemvProcessor) Process(iqr *iqr.IQR) (*iqr.IQR, error) {
	panic("not implemented")
}

func (p *makemvProcessor) Rewind() {}

type regexProcessor struct{}

func (p *regexProcessor) Process(iqr *iqr.IQR) (*iqr.IQR, error) {
	panic("not implemented")
}

func (p *regexProcessor) Rewind() {}

type rexProcessor struct{}

func (p *rexProcessor) Process(iqr *iqr.IQR) (*iqr.IQR, error) {
	panic("not implemented")
}

func (p *rexProcessor) Rewind() {}

type searchProcessor struct{}

func (p *searchProcessor) Process(iqr *iqr.IQR) (*iqr.IQR, error) {
	panic("not implemented")
}

func (p *searchProcessor) Rewind() {}

type whereProcessor struct{}

func (p *whereProcessor) Process(iqr *iqr.IQR) (*iqr.IQR, error) {
	panic("not implemented")
}

func (p *whereProcessor) Rewind() {}

type streamstatsProcessor struct{}

func (p *streamstatsProcessor) Process(iqr *iqr.IQR) (*iqr.IQR, error) {
	panic("not implemented")
}

func (p *streamstatsProcessor) Rewind() {}

type timechartProcessor struct{}

func (p *timechartProcessor) Process(iqr *iqr.IQR) (*iqr.IQR, error) {
	panic("not implemented")
}

func (p *timechartProcessor) Rewind() {}

type statsProcessor struct{}

func (p *statsProcessor) Process(iqr *iqr.IQR) (*iqr.IQR, error) {
	panic("not implemented")
}

func (p *statsProcessor) Rewind() {}

type topProcessor struct{}

func (p *topProcessor) Process(iqr *iqr.IQR) (*iqr.IQR, error) {
	panic("not implemented")
}

func (p *topProcessor) Rewind() {}

type rareProcessor struct{}

func (p *rareProcessor) Process(iqr *iqr.IQR) (*iqr.IQR, error) {
	panic("not implemented")
}

func (p *rareProcessor) Rewind() {}

type transactionProcessor struct{}

func (p *transactionProcessor) Process(iqr *iqr.IQR) (*iqr.IQR, error) {
	panic("not implemented")
}

func (p *transactionProcessor) Rewind() {}

type sortProcessor struct{}

func (p *sortProcessor) Process(iqr *iqr.IQR) (*iqr.IQR, error) {
	panic("not implemented")
}

func (p *sortProcessor) Rewind() {}

func NewBinDP() *DataProcessor {
	return &DataProcessor{
		streams:           make([]*cachedStream, 0),
		processor:         &binProcessor{},
		inputOrderMatters: false,
		isPermutingCmd:    false,
		isBottleneckCmd:   false, // TODO: depends on whether the span option was set
		isTwoPassCmd:      false, // TODO: depends on whether the span option was set
	}
}

func NewDedupDP() *DataProcessor {
	return &DataProcessor{
		streams:           make([]*cachedStream, 0),
		processor:         &dedupProcessor{},
		inputOrderMatters: true,
		isPermutingCmd:    false,
		isBottleneckCmd:   false,
		isTwoPassCmd:      false,
	}
}

func NewEvalDP() *DataProcessor {
	return &DataProcessor{
		streams:           make([]*cachedStream, 0),
		processor:         &evalProcessor{},
		inputOrderMatters: false,
		isPermutingCmd:    false,
		isBottleneckCmd:   false,
		isTwoPassCmd:      false,
	}
}

func NewFieldsDP() *DataProcessor {
	return &DataProcessor{
		streams:           make([]*cachedStream, 0),
		processor:         &fieldsProcessor{},
		inputOrderMatters: false,
		isPermutingCmd:    false,
		isBottleneckCmd:   false,
		isTwoPassCmd:      false,
	}
}

func NewFillnullDP() *DataProcessor {
	return &DataProcessor{
		streams:           make([]*cachedStream, 0),
		processor:         &fillnullProcessor{},
		inputOrderMatters: false,
		isPermutingCmd:    false,
		isBottleneckCmd:   false, // TODO: depends on whether the fieldlist option was set
		isTwoPassCmd:      false, // TODO: depends on whether the fieldlist option was set
	}
}

func NewGentimesDP() *DataProcessor {
	return &DataProcessor{
		streams:           make([]*cachedStream, 0),
		processor:         &gentimesProcessor{},
		inputOrderMatters: false,
		isPermutingCmd:    false,
		isBottleneckCmd:   false,
		isTwoPassCmd:      false,
	}
}

func NewHeadDP() *DataProcessor {
	return &DataProcessor{
		streams:           make([]*cachedStream, 0),
		processor:         &headProcessor{},
		inputOrderMatters: true,
		isPermutingCmd:    false,
		isBottleneckCmd:   false,
		isTwoPassCmd:      false,
	}
}

func NewTailDP() *DataProcessor {
	return &DataProcessor{
		streams:           make([]*cachedStream, 0),
		processor:         &tailProcessor{},
		inputOrderMatters: true,
		isPermutingCmd:    true,
		isBottleneckCmd:   true, // TODO: depends on the previous DPs in the chain.
		isTwoPassCmd:      false,
	}
}

func NewMakemvDP() *DataProcessor {
	return &DataProcessor{
		streams:           make([]*cachedStream, 0),
		processor:         &makemvProcessor{},
		inputOrderMatters: false,
		isPermutingCmd:    false,
		isBottleneckCmd:   false,
		isTwoPassCmd:      false,
	}
}

func NewRegexDP() *DataProcessor {
	return &DataProcessor{
		streams:           make([]*cachedStream, 0),
		processor:         &regexProcessor{},
		inputOrderMatters: false,
		isPermutingCmd:    false,
		isBottleneckCmd:   false,
		isTwoPassCmd:      false,
	}
}

func NewRexDP() *DataProcessor {
	return &DataProcessor{
		streams:           make([]*cachedStream, 0),
		processor:         &rexProcessor{},
		inputOrderMatters: false,
		isPermutingCmd:    false,
		isBottleneckCmd:   false,
		isTwoPassCmd:      false,
	}
}

func NewSearchDP() *DataProcessor {
	return &DataProcessor{
		streams:           make([]*cachedStream, 0),
		processor:         &searchProcessor{},
		inputOrderMatters: false,
		isPermutingCmd:    false,
		isBottleneckCmd:   false,
		isTwoPassCmd:      false,
	}
}

func NewWhereDP() *DataProcessor {
	return &DataProcessor{
		streams:           make([]*cachedStream, 0),
		processor:         &whereProcessor{},
		inputOrderMatters: false,
		isPermutingCmd:    false,
		isBottleneckCmd:   false,
		isTwoPassCmd:      false,
	}
}

func NewStreamstatsDP() *DataProcessor {
	return &DataProcessor{
		streams:           make([]*cachedStream, 0),
		processor:         &streamstatsProcessor{},
		inputOrderMatters: true,
		isPermutingCmd:    false,
		isBottleneckCmd:   false,
		isTwoPassCmd:      false,
	}
}

func NewTimechartDP() *DataProcessor {
	return &DataProcessor{
		streams:           make([]*cachedStream, 0),
		processor:         &timechartProcessor{},
		inputOrderMatters: false,
		isPermutingCmd:    false,
		isBottleneckCmd:   true,
		isTwoPassCmd:      false,
	}
}

func NewStatsDP() *DataProcessor {
	return &DataProcessor{
		streams:           make([]*cachedStream, 0),
		processor:         &statsProcessor{},
		inputOrderMatters: false,
		isPermutingCmd:    false,
		isBottleneckCmd:   true,
		isTwoPassCmd:      false,
	}
}

func NewTopDP() *DataProcessor {
	return &DataProcessor{
		streams:           make([]*cachedStream, 0),
		processor:         &topProcessor{},
		inputOrderMatters: false,
		isPermutingCmd:    true,
		isBottleneckCmd:   true,
		isTwoPassCmd:      false,
	}
}

func NewRareDP() *DataProcessor {
	return &DataProcessor{
		streams:           make([]*cachedStream, 0),
		processor:         &rareProcessor{},
		inputOrderMatters: false,
		isPermutingCmd:    true,
		isBottleneckCmd:   true,
		isTwoPassCmd:      false,
	}
}

func NewTransactionDP() *DataProcessor {
	return &DataProcessor{
		streams:           make([]*cachedStream, 0),
		processor:         &transactionProcessor{},
		inputOrderMatters: true,
		isPermutingCmd:    false,
		isBottleneckCmd:   false,
		isTwoPassCmd:      false,
	}
}

func NewSortDP() *DataProcessor {
	return &DataProcessor{
		streams:           make([]*cachedStream, 0),
		processor:         &sortProcessor{},
		inputOrderMatters: false,
		isPermutingCmd:    true,
		isBottleneckCmd:   true,
		isTwoPassCmd:      false,
	}
}
