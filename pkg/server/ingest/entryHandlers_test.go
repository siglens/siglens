// Copyright (c) 2026 The SigScalr Authors.
// SPDX-License-Identifier: Apache-2.0

package ingestserver

import (
	"encoding/json"
	"net"
	"os"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/valyala/fasthttp"
	"github.com/valyala/fasthttp/fasthttputil"

	log "github.com/sirupsen/logrus"

	"github.com/siglens/siglens/pkg/config"
	server_utils "github.com/siglens/siglens/pkg/server/utils"
	vtable "github.com/siglens/siglens/pkg/virtualtable"
)

func cleanupOutDir() {
	os.RemoveAll("data/")
	os.RemoveAll("ingestnodes/")
}

func TestPartial_esBulkPostHandler(t *testing.T) {

	config.InitializeDefaultConfig(t.TempDir())
	_ = vtable.InitVTable(server_utils.GetMyIds)
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
	_ = vtable.InitVTable(server_utils.GetMyIds)
	config.InitializeDefaultConfig(t.TempDir())
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
	_ = vtable.InitVTable(server_utils.GetMyIds)
	config.InitializeDefaultConfig(t.TempDir())
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
	_ = vtable.InitVTable(server_utils.GetMyIds)

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
	_ = vtable.InitVTable(server_utils.GetMyIds)

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
