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

package metautils

import (
	"testing"

	"github.com/siglens/siglens/pkg/segment/structs"
	"github.com/siglens/siglens/pkg/segment/utils"
	"github.com/stretchr/testify/assert"
)

func Test_CheckRangeIndex(t *testing.T) {
	testRange := make(map[string]*structs.Numbers)
	testRange["test1"] = &structs.Numbers{
		Min_uint64: 10,
		Max_uint64: 20,
		NumType:    utils.RNT_UNSIGNED_INT,
	}

	filter := make(map[string]string)
	filter["test1"] = "15"
	pass := CheckRangeIndex(filter, testRange, utils.Equals, 1)
	assert.True(t, pass)

	pass = CheckRangeIndex(filter, testRange, utils.NotEquals, 1)
	assert.True(t, pass)

	pass = CheckRangeIndex(filter, testRange, utils.LessThan, 1)
	assert.True(t, pass)

	pass = CheckRangeIndex(filter, testRange, utils.GreaterThan, 1)
	assert.True(t, pass)

	filter["test1"] = "8"

	pass = CheckRangeIndex(filter, testRange, utils.LessThan, 1)
	assert.False(t, pass)

	pass = CheckRangeIndex(filter, testRange, utils.GreaterThan, 1)
	assert.True(t, pass)
}
