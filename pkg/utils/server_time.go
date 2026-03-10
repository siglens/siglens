// Copyright (c) 2026 The SigScalr Authors.
// SPDX-License-Identifier: Apache-2.0

package utils

import (
	"time"
)

var serverStartTime time.Time

func SetServerStartTime(startTime time.Time) {
	serverStartTime = startTime
}
func GetServerStartTime() time.Time {
	return serverStartTime
}
