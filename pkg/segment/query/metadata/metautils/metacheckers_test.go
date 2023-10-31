/*
Copyright 2023.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

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
