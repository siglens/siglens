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

package ingestserver

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/valyala/fasthttp"
	"github.com/valyala/fasthttp/fasthttputil"

	log "github.com/sirupsen/logrus"

	"github.com/siglens/siglens/pkg/config"
	vtable "github.com/siglens/siglens/pkg/virtualtable"
)

func cleanupOutDir() {
	os.RemoveAll("data/")
	os.RemoveAll("ingestnodes/")
}

func TestPartial_esBulkPostHandler(t *testing.T) {

	config.InitializeDefaultConfig()
	_ = vtable.InitVTable()
	// init a webServer to use the post handler method
	// setup listener , it's fasthttp in memory listener for TESTING only
	ln := fasthttputil.NewInmemoryListener()
	// now running fasthttp server in a goroutine

	handler := esPostBulkHandler()
	go func() {
		err := fasthttp.Serve(ln, handler)
		if err != nil {
			log.Panicf("failed to serve: %v", err)
		}
	}()

	defer func() {
		_ = ln.Close()
	}()

	//Client
	client := &fasthttp.Client{
		Dial: func(addr string) (net.Conn, error) {
			return ln.Dial()
		},
		DisableHeaderNamesNormalizing: true,
	}

	req := fasthttp.AcquireRequest()
	resp := fasthttp.AcquireResponse()
	defer func() {
		fasthttp.ReleaseResponse(resp)
		fasthttp.ReleaseRequest(req)
	}()

	postPayloadByte := []byte(`{"index" : { "_index" : "test", "_id" : "1" } }
	{ "field1" : "value1" }
	{ "delete" : { "_index" : "test", "_id" : "2" } }
	{ "create" : { "_index" : "test", "_id" : "3" } }
	{ "field1" : "value3" }
	{ "update" : {"_id" : "1", "_index" : "test"} }
	{ "doc" : {"field2" : "value2"} }`)

	req.SetHost("localhost:8080")
	req.Header.Add("Accept", "application/text")
	req.Header.SetMethod("POST")

	req.SetBody(postPayloadByte)

	err := client.Do(req, resp)
	assert.NoError(t, err)

	payload := []byte(`{"error":true,"items":[{"index":{"status":201}},{"index":{"error":{"reason":"indexing request failed","type":"mapper_parse_exception"}},"status":400},{"index":{"status":201}},{"index":{"error":{"reason":"indexing request failed","type":"mapper_parse_exception"}},"status":400}],"took":33068}`)
	expected := make(map[string]interface{})
	_ = json.Unmarshal(payload, &expected)
	actual := make(map[string]interface{})
	_ = json.Unmarshal(resp.Body(), &actual)

	//check if both response are true
	var flag bool

	if _, ok := actual["items"]; ok {
		//we found items key
		//check if status are same
		actual_response := actual["items"].([]interface{})
		expected_response := expected["items"].([]interface{})
		if reflect.DeepEqual(actual_response, expected_response) == true {
			flag = true
		}
	}

	assert.Equal(t, 200, resp.StatusCode())
	//TODO Fix test since we are not returning any loglines after parsing/validation
	//change test to check for response code instead of response body
	assert.Equal(t, true, flag)
	cleanupOutDir()
}

func TestOk_esBulkPostHandler(t *testing.T) {
	_ = vtable.InitVTable()
	config.InitializeDefaultConfig()
	// init a webServer to use the post handler method
	// setup listener , it's fasthttp in memory listener for TESTING only
	ln := fasthttputil.NewInmemoryListener()
	// now running fasthttp server in a goroutine
	handler := esPostBulkHandler()
	go func() {
		err := fasthttp.Serve(ln, handler)
		if err != nil {
			log.Panicf("failed to serve: %v", err)
		}
	}()

	defer func() {
		_ = ln.Close()
	}()

	//Client
	client := &fasthttp.Client{
		Dial: func(addr string) (net.Conn, error) {
			return ln.Dial()
		},
		DisableHeaderNamesNormalizing: true,
	}

	req := fasthttp.AcquireRequest()
	resp := fasthttp.AcquireResponse()
	defer func() {
		fasthttp.ReleaseResponse(resp)
		fasthttp.ReleaseRequest(req)
	}()

	postPayloadByte := []byte(`{"index" : { "_index" : "test", "_id" : "1" } }
	{ "field1" : "value1" }
	{ "create" : { "_index" : "test", "_id" : "3" } }
	{ "field1" : "value3" }`)

	req.SetHost("localhost:8080")
	req.Header.Add("Accept", "application/text")
	req.Header.SetMethod("POST")

	req.SetBody(postPayloadByte)
	err := client.Do(req, resp)
	assert.NoError(t, err)

	payload := []byte(`{"error":false,"items":[{"index":{"status":201}},{"index":{"status":201}}],"took":223}`)
	expected := make(map[string]interface{})
	_ = json.Unmarshal(payload, &expected)
	actual := make(map[string]interface{})
	_ = json.Unmarshal(resp.Body(), &actual)
	//check if both response are true
	var flag bool

	if _, ok := actual["items"]; ok {
		//we found items key
		//check if status are same
		actual_response := actual["items"].([]interface{})
		expected_response := expected["items"].([]interface{})
		if reflect.DeepEqual(actual_response, expected_response) == true {
			flag = true
		}
	}

	assert.Equal(t, 200, resp.StatusCode())
	//TODO Fix test since we are not returning any loglines after parsing/validation
	//change test to check for response code instead of response body
	assert.Equal(t, true, flag)
	cleanupOutDir()
}

func TestDelete_esBulkPostHandler(t *testing.T) {
	_ = vtable.InitVTable()
	config.InitializeDefaultConfig()
	// init a webServer to use the post handler method
	// setup listener , it's fasthttp in memory listener for TESTING only
	ln := fasthttputil.NewInmemoryListener()
	// now running fasthttp server in a goroutine
	handler := esPostBulkHandler()
	go func() {
		err := fasthttp.Serve(ln, handler)
		if err != nil {
			log.Panicf("failed to serve: %v", err)
		}
	}()

	defer func() {
		_ = ln.Close()
	}()

	//Client
	client := &fasthttp.Client{
		Dial: func(addr string) (net.Conn, error) {
			return ln.Dial()
		},
		DisableHeaderNamesNormalizing: true,
	}

	req := fasthttp.AcquireRequest()
	resp := fasthttp.AcquireResponse()
	defer func() {
		fasthttp.ReleaseResponse(resp)
		fasthttp.ReleaseRequest(req)
	}()

	postPayloadByte := []byte(`{ "delete" : { "_index" : "test", "_id" : "2" } }`)

	req.SetHost("localhost:8080")
	req.Header.Add("Accept", "application/text")
	req.Header.SetMethod("POST")

	req.SetBody(postPayloadByte)

	err := client.Do(req, resp)
	assert.NoError(t, err)

	payload := []byte(`{"index":{"error":{"reason":"request body is required","type":"parse_exception"}},"status":400}`)
	expected := make(map[string]interface{})
	_ = json.Unmarshal(payload, &expected)
	actual := make(map[string]interface{})
	_ = json.Unmarshal(resp.Body(), &actual)

	//check if both response are true
	var flag bool
	if reflect.DeepEqual(actual, expected) == true {
		flag = true
	}
	assert.Equal(t, 400, resp.StatusCode())
	//TODO Fix test since we are not returning any loglines after parsing/validation
	//change test to check for response code instead of response body
	assert.Equal(t, true, flag)
	cleanupOutDir()
}

func TestUpdate_esBulkPostHandler(t *testing.T) {
	_ = vtable.InitVTable()

	// init a webServer to use the post handler method
	// setup listener , it's fasthttp in memory listener for TESTING only
	ln := fasthttputil.NewInmemoryListener()
	// now running fasthttp server in a goroutine
	handler := esPostBulkHandler()
	go func() {
		err := fasthttp.Serve(ln, handler)
		if err != nil {
			log.Panicf("failed to serve: %v", err)
		}
	}()

	defer func() {
		_ = ln.Close()
	}()

	//Client
	client := &fasthttp.Client{
		Dial: func(addr string) (net.Conn, error) {
			return ln.Dial()
		},
		DisableHeaderNamesNormalizing: true,
	}

	req := fasthttp.AcquireRequest()
	resp := fasthttp.AcquireResponse()
	defer func() {
		fasthttp.ReleaseResponse(resp)
		fasthttp.ReleaseRequest(req)
	}()

	postPayloadByte := []byte(`{ "update" : {"_id" : "1", "_index" : "test"} }
	{ "doc" : {"field2" : "value2"} }`)

	req.SetHost("localhost:8080")
	req.Header.Add("Accept", "application/text")
	req.Header.SetMethod("POST")

	req.SetBody(postPayloadByte)

	err := client.Do(req, resp)
	assert.NoError(t, err)

	payload := []byte(`{"index":{"error":{"reason":"request body is required","type":"parse_exception"}},"status":400}`)
	expected := make(map[string]interface{})
	_ = json.Unmarshal(payload, &expected)
	actual := make(map[string]interface{})
	_ = json.Unmarshal(resp.Body(), &actual)

	//check if both response are true
	var flag bool
	if reflect.DeepEqual(actual, expected) == true {
		flag = true
	}

	assert.Equal(t, 400, resp.StatusCode())
	//TODO Fix test since we are not returning any loglines after parsing/validation
	//change test to check for response code instead of response body
	assert.Equal(t, true, flag)
	cleanupOutDir()
}

func Test_HealthHandler(t *testing.T) {
	_ = vtable.InitVTable()

	// init a webServer to use the post handler method
	// setup listener , it's fasthttp in memory listener for TESTING only
	ln := fasthttputil.NewInmemoryListener()
	// now running fasthttp server in a goroutine
	handler := getHealthHandler()
	go func() {
		err := fasthttp.Serve(ln, handler)
		if err != nil {
			log.Panicf("failed to serve: %v", err)
		}
	}()

	defer func() {
		_ = ln.Close()
	}()

	//Client
	client := &fasthttp.Client{
		Dial: func(addr string) (net.Conn, error) {
			return ln.Dial()
		},
		DisableHeaderNamesNormalizing: true,
	}

	req := fasthttp.AcquireRequest()
	resp := fasthttp.AcquireResponse()
	defer func() {
		fasthttp.ReleaseResponse(resp)
		fasthttp.ReleaseRequest(req)
	}()

	req.SetHost("localhost:8080")
	req.Header.SetMethod("GET")

	err := client.Do(req, resp)
	assert.NoError(t, err)

	payload := []byte(`{"message":"","status":200}`)

	assert.Equal(t, 200, resp.StatusCode())

	assert.Equal(t, payload, resp.Body())
	cleanupOutDir()
}

func Test_updateConfigParams(t *testing.T) {
	_ = vtable.InitVTable()
	// setup listener , it's fasthttp in memory listener for TESTING only
	ln := fasthttputil.NewInmemoryListener()
	// now running fasthttp server in a goroutine
	handler := postSetconfigHandler(false)
	go func() {
		err := fasthttp.Serve(ln, handler)
		if err != nil {
			log.Panicf("failed to serve: %v", err)
		}
	}()

	defer func() {
		_ = ln.Close()
	}()

	//Client
	client := &fasthttp.Client{
		Dial: func(addr string) (net.Conn, error) {
			return ln.Dial()
		},
		DisableHeaderNamesNormalizing: true,
	}

	req := fasthttp.AcquireRequest()
	resp := fasthttp.AcquireResponse()
	defer func() {
		fasthttp.ReleaseResponse(resp)
		fasthttp.ReleaseRequest(req)
	}()

	cases := []struct {
		input      string
		output     string
		statusCode int
	}{
		{ // case#1 Happy case
			`{
			   "eventTypeKeywords": ["key2","key1"]
			 }`,
			`{"message":"All OK","status":200}`,
			fasthttp.StatusOK,
		},
		{ // case#4 When bad json is provided
			`{
			   "eventTypeKeywords": ["key2","key1"],"excludeBloomItems": ["bloom3","bloom4"], "streamIdFieldNames":
			 }`,
			`{"message":"Bad request","status":400}`,
			fasthttp.StatusBadRequest,
		},
		{ // case#5 When a key that is not allowed to be updated is provided
			`{
			   "port":"9090", "eventTypeKeywords": ["key2","key1"]
			 }`,
			`{"message":"key = port not allowed to update","status":403}`,
			fasthttp.StatusForbidden,
		},
	}

	for i, test := range cases {

		postPayloadByte := []byte(test.input)

		req.SetHost("localhost:8080")
		req.Header.Add("Accept", "application/json")
		req.Header.SetMethod("POST")

		req.SetBody(postPayloadByte)

		err := client.Do(req, resp)
		assert.NoError(t, err)

		payload := []byte(test.output)
		assert.Equal(t, test.statusCode, resp.StatusCode(), fmt.Sprintf("Status code comparison failed, test=%v", i+1))
		assert.Equal(t, payload, resp.Body(), fmt.Sprintf("Body compare failed, test=%v", i+1))
	}
	cleanupOutDir()
}
