// Copyright (c) 2026 The SigScalr Authors.
// SPDX-License-Identifier: Apache-2.0

package utils

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"gopkg.in/yaml.v3"
)

func Test_WithDefault_Equals(t *testing.T) {
	wd1 := DefaultValue(42)
	wd2 := DefaultValue(42)
	assert.True(t, wd1.Equals(wd2))

	wd2.value = 43
	wd2.isSet = true
	assert.False(t, wd1.Equals(wd2))

	wd1.value = 43
	wd1.isSet = true
	assert.True(t, wd1.Equals(wd2))

	wd3 := DefaultValue(43)
	assert.True(t, wd1.Equals(wd3))
}

func Test_WithDefault_Unmarshal(t *testing.T) {
	yamlData := `
myBool1: false
myInt1: 100
myInt2: not an int
myString1: "hello"
myString2: ""
`

	type Config struct {
		MyBool1   WithDefault[bool]   `yaml:"myBool1"`
		MyBool2   WithDefault[bool]   `yaml:"myBool2"`
		MyInt1    WithDefault[int]    `yaml:"myInt1"`
		MyInt2    WithDefault[int]    `yaml:"myInt2"`
		MyInt3    WithDefault[int]    `yaml:"myInt3"`
		MyString1 WithDefault[string] `yaml:"myString1"`
		MyString2 WithDefault[string] `yaml:"myString2"`
		MyString3 WithDefault[string] `yaml:"myString3"`
	}
	config := Config{
		MyBool1:   DefaultValue(true),
		MyBool2:   DefaultValue(true),
		MyInt1:    DefaultValue(42),
		MyInt2:    DefaultValue(42),
		MyInt3:    DefaultValue(42),
		MyString1: DefaultValue("fallback"),
		MyString2: DefaultValue("fallback"),
		MyString3: DefaultValue("fallback"),
	}

	err := yaml.Unmarshal([]byte(yamlData), &config)
	assert.NoError(t, err)

	assert.Equal(t, false, config.MyBool1.Value())
	assert.Equal(t, true, config.MyBool2.Value())
	assert.Equal(t, 100, config.MyInt1.Value())
	assert.Equal(t, 42, config.MyInt2.Value())
	assert.Equal(t, "hello", config.MyString1.Value())
	assert.Equal(t, "fallback", config.MyString2.Value())
	assert.Equal(t, "fallback", config.MyString3.Value())
}
