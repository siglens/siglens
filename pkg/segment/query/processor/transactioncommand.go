// Copyright (c) 2026 The SigScalr Authors.
// SPDX-License-Identifier: Apache-2.0

package processor

import (
	"github.com/siglens/siglens/pkg/segment/query/iqr"
	"github.com/siglens/siglens/pkg/segment/structs"
)

type transactionProcessor struct {
	options *structs.TransactionArguments
}

func (p *transactionProcessor) Process(iqr *iqr.IQR) (*iqr.IQR, error) {
	panic("not implemented")
}

func (p *transactionProcessor) Rewind() {
	panic("not implemented")
}

func (p *transactionProcessor) Cleanup() {
	panic("not implemented")
}

func (p *transactionProcessor) GetFinalResultIfExists() (*iqr.IQR, bool) {
	return nil, false
}
