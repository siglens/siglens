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

// ExtractJsonValueBuffered extracts the value of the given keys from the JSON data and returns it in a buffer.
func ExtractJsonValueBuffered(data []byte, workBuf []byte, keys ...string) ([]byte, jsonparser.ValueType, error) {
	// Extract the value using the given keys
	value, valueType, _, err := jsonparser.Get(data, keys...)
	if err != nil {
		return nil, jsonparser.NotExist, fmt.Errorf("key path %v not found: %v", keys, err)
	}

	// Extend the existing workBuf if necessary
	requiredLen := len(value)
	if requiredLen > cap(workBuf) {

		newBuf := make([]byte, len(workBuf), requiredLen)
		copy(newBuf, workBuf)
		workBuf = newBuf
	}

	workBuf = append(workBuf[:0], value...)

	return workBuf, valueType, nil
}
