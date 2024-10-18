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

package utils

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_CVal_Equal(t *testing.T) {
	c1 := &CValueEnclosure{Dtype: SS_DT_STRING, CVal: "hello"}
	c2 := &CValueEnclosure{Dtype: SS_DT_STRING, CVal: "hello"}
	assert.True(t, c1.Equal(c2))

	c1 = &CValueEnclosure{Dtype: SS_DT_SIGNED_NUM, CVal: int64(123)}
	c2 = &CValueEnclosure{Dtype: SS_DT_SIGNED_NUM, CVal: int64(123)}
	assert.True(t, c1.Equal(c2))

	c1 = &CValueEnclosure{Dtype: SS_DT_SIGNED_NUM, CVal: uint64(123)}
	c2 = &CValueEnclosure{Dtype: SS_DT_UNSIGNED_NUM, CVal: uint64(123)}
	assert.False(t, c1.Equal(c2))
}
