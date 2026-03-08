// Copyright (c) 2026 The SigScalr Authors.
// SPDX-License-Identifier: Apache-2.0

package utils

import "math/rand"

func RandomBuffer(size int, seed int) []byte {
	source := rand.NewSource(int64(seed))
	rand := rand.New(source)
	buf := make([]byte, size)

	for i := 0; i < size; i++ {
		buf[i] = byte(rand.Intn(256))
	}

	return buf
}
