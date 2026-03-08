// Copyright (c) 2026 The SigScalr Authors.
// SPDX-License-Identifier: Apache-2.0

package metautils

import (
	"testing"

	"github.com/siglens/siglens/pkg/segment/structs"
	sutils "github.com/siglens/siglens/pkg/segment/utils"
	"github.com/stretchr/testify/assert"
)

func Test_CheckRangeIndex(t *testing.T) {
	testRange := make(map[string]*structs.Numbers)
	testRange["test1"] = &structs.Numbers{
		Min_uint64: 10,
		Max_uint64: 20,
		NumType:    sutils.RNT_UNSIGNED_INT,
	}

	filter := make(map[string]string)
	filter["test1"] = "15"
	pass := CheckRangeIndex(filter, testRange, sutils.Equals, 1)
	assert.True(t, pass)

	pass = CheckRangeIndex(filter, testRange, sutils.NotEquals, 1)
	assert.True(t, pass)

	pass = CheckRangeIndex(filter, testRange, sutils.LessThan, 1)
	assert.True(t, pass)

	pass = CheckRangeIndex(filter, testRange, sutils.GreaterThan, 1)
	assert.True(t, pass)

	filter["test1"] = "8"

	pass = CheckRangeIndex(filter, testRange, sutils.LessThan, 1)
	assert.False(t, pass)

	pass = CheckRangeIndex(filter, testRange, sutils.GreaterThan, 1)
	assert.True(t, pass)
}
