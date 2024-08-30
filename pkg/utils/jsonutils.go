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
	"fmt"
	"strings"

	"github.com/buger/jsonparser"
)

// Flatten takes a map and returns a new one where nested maps are replaced
// by dot-delimited keys.
func Flatten(m map[string]interface{}) map[string]interface{} {
	for k, v := range m {
		switch child := v.(type) {
		case map[string]interface{}:
			nm := Flatten(child)
			for nk, nv := range nm {
				m[k+"."+nk] = nv
			}
			delete(m, k)
			// todo if it is a json array it needs to be formatted correctly
		case []interface{}:
			for idx, val := range child {
				FlattenSingleValue(fmt.Sprintf("%v.%v", k, idx), m, val)
			}
			delete(m, k)
		}
	}
	return m
}

func FlattenSingleValue(key string, m map[string]interface{}, child interface{}) {
	switch child := child.(type) {
	case map[string]interface{}:
		nm := Flatten(child)
		for nk, nv := range nm {
			m[key+"."+nk] = nv
		}
	case []interface{}:
		for idx, val := range child {
			FlattenSingleValue(fmt.Sprintf("%v.%v", key, idx), m, val)
		}
	default:
		m[key] = child
	}
}

// GetStringFromJson extracts a string value from JSON data based on the provided keys.
// It uses the supplied workBuf to avoid unnecessary allocations and resizing.
// If the value is not a string or if an error occurs, it returns an appropriate error.
func GetStringFromJson(data []byte, workBuf []byte, keys ...string) ([]byte, error) {
	// Estimate the size needed for workBuf before copying the value
	estimatedSize := 0
	for _, key := range keys {
		if len(key) > estimatedSize {
			estimatedSize = len(key)
		}
	}

	// Resize workBuf to fit the estimated size
	workBuf = ResizeSlice(workBuf, estimatedSize)

	// Extract the value directly into workBuf
	var err error
	var value []byte
	var dataType jsonparser.ValueType

	value, dataType, _, err = jsonparser.Get(data, keys...)
	if err != nil {
		return nil, err
	}

	if dataType != jsonparser.String {
		if dataType == jsonparser.Null {
			return nil, fmt.Errorf("key %s has a null value", strings.Join(keys, ", "))
		}
		return nil, fmt.Errorf("expected string value for key(s) %s but got %s", strings.Join(keys, ", "), dataType)
	}

	// Resize workBuf again to fit the actual length of the extracted value
	workBuf = ResizeSlice(workBuf, len(value))

	// Copy the extracted value directly into workBuf
	copy(workBuf, value)

	return workBuf, nil
}
