/*
Copyright 2023.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package utils

import (
	"encoding/base64"
	"fmt"
	"testing"

	"github.com/cespare/xxhash"
	"github.com/stretchr/testify/assert"
	"github.com/valyala/fasthttp"
)

func Test_VerifyBasicAuth(t *testing.T) {
	const password = "hello"
	const username = "world"
	passwordHash := xxhash.Sum64String(password)
	usernameHash := xxhash.Sum64String(username)
	ctx := &fasthttp.RequestCtx{}

	// Test invalid case where no authorization is provided.
	assert.False(t, VerifyBasicAuth(ctx, usernameHash, passwordHash))

	// Test invalid case where the "Basic" prefix is missing.
	encoded := base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("%v:%v", username, password)))
	ctx.Request.Header.Set("Authorization", encoded)
	assert.False(t, VerifyBasicAuth(ctx, usernameHash, passwordHash))

	// Test invalid case where the username is wrong.
	encoded = base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("badUsername:%v", password)))
	ctx.Request.Header.Set("Authorization", "Basic "+encoded)
	assert.False(t, VerifyBasicAuth(ctx, usernameHash, passwordHash))

	// Test invalid case where the password is wrong.
	encoded = base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("%v:badPassword", username)))
	ctx.Request.Header.Set("Authorization", "Basic "+encoded)
	assert.False(t, VerifyBasicAuth(ctx, usernameHash, passwordHash))

	// Test invalid case where username and password are both wrong.
	encoded = base64.StdEncoding.EncodeToString([]byte("badUsername:badPassword"))
	ctx.Request.Header.Set("Authorization", "Basic "+encoded)
	assert.False(t, VerifyBasicAuth(ctx, usernameHash, passwordHash))

	// Test invalid case where the colon is missing.
	encoded = base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("%v%v", username, password)))
	ctx.Request.Header.Set("Authorization", "Basic "+encoded)
	assert.False(t, VerifyBasicAuth(ctx, usernameHash, passwordHash))

	// Test a valid case.
	encoded = base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("%v:%v", username, password)))
	ctx.Request.Header.Set("Authorization", "Basic "+encoded)
	assert.True(t, VerifyBasicAuth(ctx, usernameHash, passwordHash))
}
