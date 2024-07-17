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

func Test_GetNilRegex(t *testing.T) {
	regex := SerializableRegex{}
	assert.Nil(t, regex.GetCompiledRegex())
}

func TestGetNonNilRegex(t *testing.T) {
	regex := SerializableRegex{}
	err := regex.SetRegex(".*")
	assert.NoError(t, err)
	assert.NotNil(t, regex.GetCompiledRegex())
}

func Test_SetInvalidRegex(t *testing.T) {
	regex := SerializableRegex{}
	err := regex.SetRegex("a(")
	assert.Error(t, err)
}

func Test_SetValidRegex(t *testing.T) {
	regex := SerializableRegex{}
	err := regex.SetRegex("a.*z")
	assert.NoError(t, err)

	compiledRegex := regex.GetCompiledRegex()
	assert.NotNil(t, compiledRegex)
	assert.Equal(t, "a.*z", compiledRegex.String())
}

func Test_SetInvalidRegexKeepsLastValidRegex(t *testing.T) {
	regex := SerializableRegex{}
	err := regex.SetRegex("a.*z")
	assert.NoError(t, err)

	err = regex.SetRegex("a(")
	assert.Error(t, err)

	compiledRegex := regex.GetCompiledRegex()
	assert.NotNil(t, compiledRegex)
	assert.Equal(t, "a.*z", compiledRegex.String())
}
