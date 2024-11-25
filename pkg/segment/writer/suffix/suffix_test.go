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

package suffix

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_getNextSuffix(t *testing.T) {
	dir := t.TempDir()
	fileName := filepath.Join(dir, "suffix.json")

	// Test non-existent file.
	suffix, err := getAndIncrementSuffixFromFile(fileName, nil)
	assert.NoError(t, err)
	assert.Equal(t, uint64(0), suffix)

	// Now the file exists.
	suffix, err = getAndIncrementSuffixFromFile(fileName, nil)
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), suffix)
}
