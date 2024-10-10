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

// In the two-pass version of bin, Rewind() should remember the span it
// calculated in the first pass.
func (p *binProcessor) Rewind() {
	panic("not implemented")
}
