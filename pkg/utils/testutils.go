// Copyright (c) 2026 The SigScalr Authors.
// SPDX-License-Identifier: Apache-2.0

package utils

import (
	"math/rand"
	"time"
)

type StringType uint8

const (
	Alpha StringType = iota
	Numeric
	AlphaNumeric
)

const alphabets = "abcdefghijklmnopqrstuvwxyz" + "ABCDEFGHIJKLMNOPQRSTUVWXYZ"

const digits = "0123456789"

var seededRand *rand.Rand = rand.New(rand.NewSource(time.Now().UnixNano()))

func GetRandomString(length int, stringType StringType) string {
	var charset string
	switch stringType {
	case Alpha:
		charset = alphabets
	case Numeric:
		charset = digits
	case AlphaNumeric:
		charset = alphabets + digits
	default:
		charset = alphabets + digits
	}

	b := make([]byte, length)
	for i := range b {
		b[i] = charset[seededRand.Intn(len(charset))]
	}
	return string(b)
}
