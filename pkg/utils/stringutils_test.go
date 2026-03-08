// Copyright (c) 2026 The SigScalr Authors.
// SPDX-License-Identifier: Apache-2.0

package utils

import "testing"

func Test_MightBeFloat(t *testing.T) {
	tests := []struct {
		input    string
		expected bool
	}{
		{"1.0", true},
		{"1", true},
		{"1:10:50", false},
		{"v1.0", false},
		{"-1E-10", true},
		{"+42", true},
	}
	for _, test := range tests {
		if got := MightBeFloat(test.input); got != test.expected {
			t.Errorf("MightBeFloat(%q) = %v, want %v", test.input, got, test.expected)
		}
	}
}
