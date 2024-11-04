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

package record

import (
	"testing"

	"github.com/siglens/siglens/pkg/segment/utils"
	"github.com/stretchr/testify/assert"
)

func Test_GetColumnNames(t *testing.T) {
	mocker := &MockRRCsReader{
		FieldToValues: map[string][]utils.CValueEnclosure{
			"col1": {},
			"col2": {},
		},
	}

	columns, err := mocker.GetColsForSegKey("segKey", "vTable")
	assert.NoError(t, err)

	expected := map[string]struct{}{
		"col1": {},
		"col2": {},
	}
	assert.Equal(t, expected, columns)
}
