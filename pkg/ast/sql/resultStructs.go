// Copyright (c) 2026 The SigScalr Authors.
// SPDX-License-Identifier: Apache-2.0

package sql

type JDBCElasticSQLSearchResponse struct {
	Schema   []JdbcField     `json:"schema"`
	DataRows [][]interface{} `json:"rows"`
	Size     int             `json:"size"`
	Status   int             `json:"status"`
	Columns  []JdbcField     `json:"columns"`
	Errors   []string        `json:"errors,omitempty"`
}

type JdbcField struct {
	Name string `json:"name"`
	Type string `json:"type"`
}
