// Copyright (c) 2026 The SigScalr Authors.
// SPDX-License-Identifier: Apache-2.0

package loki

type LokiLogStream struct {
	Stream map[string]string `json:"stream"`
	Values [][]interface{}   `json:"values"`
}

type LokiLogData struct {
	Streams []LokiLogStream `json:"streams"`
}
