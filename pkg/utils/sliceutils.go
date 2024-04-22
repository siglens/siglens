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

import log "github.com/sirupsen/logrus"

func SliceContainsString(slice []string, s string) bool {
	for _, v := range slice {
		if v == s {
			return true
		}
	}

	return false
}

func SliceContainsInt(slice []int, x int) bool {
	for _, v := range slice {
		if v == x {
			return true
		}
	}

	return false
}

func SelectIndicesFromSlice(slice []string, indices []int) []string {
	var result []string
	for _, v := range indices {
		if v < 0 || v >= len(slice) {
			log.Errorf("SelectIndicesFromSlice: index %d out of range for slice of length %v", v, len(slice))
			continue
		}

		result = append(result, slice[v])
	}

	return result
}
