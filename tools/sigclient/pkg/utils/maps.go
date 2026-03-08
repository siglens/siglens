// Copyright (c) 2026 The SigScalr Authors.
// SPDX-License-Identifier: Apache-2.0
package utils

func EqualMaps[K comparable, V comparable](map1 map[K]V, map2 map[K]V) bool {
	if len(map1) != len(map2) {
		return false
	}

	for key, v1 := range map1 {
		if v2, ok := map2[key]; !ok || v1 != v2 {
			return false
		}
	}

	return true
}
