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
	"compress/gzip"
	"encoding/base64"
	"fmt"
	"testing"

	"github.com/cespare/xxhash"
	log "github.com/sirupsen/logrus"
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

func Test_GetDecodedBody(t *testing.T) {
	const body = "hello world"
	ctx := &fasthttp.RequestCtx{}

	// Test when the body is not encoded.
	ctx.Request.SetBodyString(body)

	decodedBody, err := GetDecodedBody(ctx)
	assert.Nil(t, err)
	assert.Equal(t, body, string(decodedBody))

	// Test when the body is gzipped.
	buf := bytes.Buffer{}
	writer := gzip.NewWriter(&buf)
	_, err = writer.Write([]byte(body))
	assert.Nil(t, err)
	err = writer.Close()
	assert.Nil(t, err)
	gzippedBody := buf.Bytes()
	ctx.Request.Header.Set("Content-Encoding", "gzip")
	ctx.Request.SetBody(gzippedBody)

	decodedBody, err = GetDecodedBody(ctx)
	assert.Nil(t, err)
	assert.Equal(t, body, string(decodedBody))

	// Test an invalid encoding.
	ctx.Request.Header.Set("Content-Encoding", "invalid")
	decodedBody, err = GetDecodedBody(ctx)
	assert.NotNil(t, err)
	assert.Nil(t, decodedBody)
}

func Test_ExtractSeriesOfJsonObjects(t *testing.T) {
	const body = `{"a": 1}{
        "b": 2,
        "c": "crabs"}
        {"d": 3}`

	jsonObjects, err := ExtractSeriesOfJsonObjects([]byte(body))
	assert.Nil(t, err)
	assert.Equal(t, 3, len(jsonObjects))
	assert.Equal(t, map[string]interface{}{"a": float64(1)}, jsonObjects[0])
	assert.Equal(t, map[string]interface{}{"b": float64(2), "c": "crabs"}, jsonObjects[1])
	assert.Equal(t, map[string]interface{}{"d": float64(3)}, jsonObjects[2])

	// Test invalid JSON.
	const invalidBody = `{"a": 1}{`
	jsonObjects, err = ExtractSeriesOfJsonObjects([]byte(invalidBody))
	assert.NotNil(t, err)
	assert.Nil(t, jsonObjects)
}

// Hook to capture log entries
type LoggerHook struct {
	Entries []*log.Entry
}

func (hook *LoggerHook) Levels() []log.Level {
	return log.AllLevels
}

func (hook *LoggerHook) Fire(entry *log.Entry) error {
	hook.Entries = append(hook.Entries, entry)
	return nil
}

func Test_sendErrorWithStatus(t *testing.T) {
	logger := log.New()
	loggerHook := &LoggerHook{}
	logger.Hooks.Add(loggerHook)
	ctx := &fasthttp.RequestCtx{}

	// sendErrorWithStatus logs the function two levels up, so wrap it in a closure.
	func() {
		sendErrorWithStatus(logger, ctx, "user message", "extra log message", fmt.Errorf("some error"), fasthttp.StatusBadRequest)
	}()

	assert.Len(t, loggerHook.Entries, 1)
	assert.Contains(t, loggerHook.Entries[0].Message, "user message")
	assert.Contains(t, loggerHook.Entries[0].Message, "extra log message")
	assert.Contains(t, loggerHook.Entries[0].Message, "some error")
	assert.Contains(t, loggerHook.Entries[0].Message, "httpserverutils_test.go")
	assert.Contains(t, loggerHook.Entries[0].Message, "Test_sendErrorWithStatus")

	assert.Equal(t, fasthttp.StatusBadRequest, ctx.Response.StatusCode())
	assert.Equal(t, "{\"error\":\"user message\"}", string(ctx.Response.Body()))

}
