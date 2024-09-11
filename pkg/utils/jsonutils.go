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
	"bytes"
	"encoding/json"
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

func ExtractJsonValueBuffered(data []byte, workBuf []byte, keys ...string) ([]byte, jsonparser.ValueType, error) {
	var err error
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	var current interface{} = nil
	for _, key := range keys {
		if err = decoder.Decode(&current); err != nil {
			return nil, jsonparser.NotExist, fmt.Errorf("error decoding JSON: %v", err)
		}
		switch obj := current.(type) {
		case map[string]interface{}:
			if value, found := obj[key]; found {
				current = value
			} else {
				return nil, jsonparser.NotExist, fmt.Errorf("key %s not found", key)
			}
		case []interface{}:
			return nil, jsonparser.NotExist, fmt.Errorf("unexpected array while traversing key %s", key)
		default:
			return nil, jsonparser.NotExist, fmt.Errorf("expected object for key %s but found %T", key, obj)
		}
	}
	valueBytes, err := json.Marshal(current)
	if err != nil {
		return nil, jsonparser.NotExist, fmt.Errorf("error converting value to bytes: %v", err)
	}
	workBuf = ResizeSlice(workBuf, len(valueBytes))
	copy(workBuf, valueBytes)
	var valueType jsonparser.ValueType
	switch current.(type) {
	case string:
		valueType = jsonparser.String
	case json.Number:
		valueType = jsonparser.Number
	case bool:
		valueType = jsonparser.Boolean
	case nil:
		valueType = jsonparser.Null
	default:
		valueType = jsonparser.Object
	}
	return workBuf, valueType, nil
}
func GetStringFromJson(data []byte, workBuf []byte, keys ...string) ([]byte, error) {
	workBuf, dataType, err := ExtractJsonValueBuffered(data, workBuf, keys...)
	if err != nil {
		return nil, err
	}
	if dataType != jsonparser.String {
		if dataType == jsonparser.Null {
			return nil, fmt.Errorf("key %s has a null value", strings.Join(keys, ", "))
		}
		return nil, fmt.Errorf("expected string value for key(s) %s but got %s", strings.Join(keys, ", "), dataType)
	}
	return workBuf, nil
}
