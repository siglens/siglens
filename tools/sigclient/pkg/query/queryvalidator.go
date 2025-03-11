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

package query

import "fmt"

type queryValidator interface {
	HandleLog(map[string]interface{}) error
	GetQuery() (string, uint64, uint64) // Query, start epoch, end epoch.
	MatchesResult([]byte) error
}

type basicValidator struct {
	startEpoch uint64
	endEpoch   uint64
	query      string
}

func (b *basicValidator) HandleLog(log map[string]interface{}) error {
	return fmt.Errorf("basicValidator.HandleLog: not implemented")
}

func (b *basicValidator) GetQuery() (string, uint64, uint64) {
	return b.query, b.startEpoch, b.endEpoch
}

func (b *basicValidator) MatchesResult(result []byte) error {
	return fmt.Errorf("basicValidator.MatchesResult: not implemented")
}
