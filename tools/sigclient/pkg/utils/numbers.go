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

package utils

import (
	"fmt"
	"strconv"
)

func AsUint64(x interface{}) (uint64, bool) {
	s := fmt.Sprintf("%v", x)
	result, err := strconv.ParseUint(s, 10, 64)
	if err == nil {
		return result, true
	}

	// Try to parse as float.
	f, ok := AsFloat64(x)
	if !ok {
		return 0, false
	}

	return uint64(f), true
}

func AsFloat64(x interface{}) (float64, bool) {
	s := fmt.Sprintf("%v", x)
	result, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, false
	}

	return result, true
}
