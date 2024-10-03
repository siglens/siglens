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
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_Getters(t *testing.T) {
	dp := &DataProcessor{
		inputOrderMatters: true,
		isPermutingCmd:    true,
		isBottleneckCmd:   true,
		isTwoPassCmd:      true,
	}

	assert.True(t, dp.DoesInputOrderMatter())
	assert.True(t, dp.IsPermutingCmd())
	assert.True(t, dp.IsBottleneckCmd())
	assert.True(t, dp.IsTwoPassCmd())

	dp = &DataProcessor{
		inputOrderMatters: false,
		isPermutingCmd:    false,
		isBottleneckCmd:   false,
		isTwoPassCmd:      false,
	}

	assert.False(t, dp.DoesInputOrderMatter())
	assert.False(t, dp.IsPermutingCmd())
	assert.False(t, dp.IsBottleneckCmd())
	assert.False(t, dp.IsTwoPassCmd())
}
