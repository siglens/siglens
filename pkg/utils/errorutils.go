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
	"fmt"
	"sync"
	"sync/atomic"

	log "github.com/sirupsen/logrus"
)

const MAX_SIMILAR_ERRORS_TO_LOG = 5 // Maximum number of similar errors to store

var qidToBatchErrorMap map[uint64]*BatchError
var qidToBatchErrorMapLock *sync.RWMutex

const NIL_VALUE_ERR = "NIL_VALUE_ERR"
const CONVERSION_ERR = "CONVERSION_ERR"

// Error shown when no dependency graphs are available for a particular time frame
const ErrNoDependencyGraphs = "no dependencies graphs have been generated"

// BatchErrorData holds error information for a specific error key
type BatchErrorData struct {
	errors  []error
	counter uint32
}

// BatchError manages batched errors with thread safety
type BatchError struct {
	beMap *sync.Map
	qid   int64
}

// ErrorWithCode combines an error with a standardized error code
type ErrorWithCode struct {
	code string
	err  error
}

func init() {
	qidToBatchErrorMap = make(map[uint64]*BatchError)
	qidToBatchErrorMapLock = &sync.RWMutex{}
}

func NewBatchError() *BatchError {
	return &BatchError{
		beMap: &sync.Map{},
		qid:   -1,
	}
}

// AddError adds an error to the batch with thread safety using sync.Map
func (be *BatchError) AddError(errKey string, err error) {
	if err == nil {
		return
	}

	if errorWithCode, ok := err.(*ErrorWithCode); ok {
		errKey = fmt.Sprintf("%s:%s", errKey, errorWithCode.code)
	}

	// Get or create BatchErrorData atomically
	value, _ := be.beMap.LoadOrStore(errKey, &BatchErrorData{
		errors: make([]error, 0, MAX_SIMILAR_ERRORS_TO_LOG),
	})
	data := value.(*BatchErrorData)

	atomic.AddUint32(&data.counter, 1)

	// Only store error if we haven't reached the limit
	if len(data.errors) < MAX_SIMILAR_ERRORS_TO_LOG {
		data.errors = append(data.errors, err)
	}
}

// LogAllErrors logs all accumulated errors
// Send qid as -1 if qid is not available
func (be *BatchError) LogAllErrors() {
	qidMsg := ""
	if be.qid >= 0 {
		qidMsg = fmt.Sprintf("qid=%v, ", be.qid)
	}

	be.beMap.Range(func(key, value interface{}) bool {
		data := value.(*BatchErrorData)
		log.WithFields(log.Fields{
			"count":   data.counter,
			"samples": data.errors,
		}).Errorf("%s ErrorKey=%v", qidMsg, key)
		return true
	})
}

// Reset clears all errors and qid
func (be *BatchError) Reset() {
	be.beMap = &sync.Map{}
	be.qid = -1
}

func (be *BatchError) HasErrors() bool {
	hasErrors := false
	be.beMap.Range(func(_, _ interface{}) bool {
		hasErrors = true
		return false // Stop iteration once we find any entry
	})
	return hasErrors
}

// NewErrorWithCode creates a new error with a code
func NewErrorWithCode(code string, err error) *ErrorWithCode {
	return &ErrorWithCode{
		code: code,
		err:  err,
	}
}

// Error implements the error interface
func (ewc *ErrorWithCode) Error() string {
	return fmt.Sprintf("ErrorCode=%s; err=%v", ewc.code, ewc.err)
}

// Unwrap returns the underlying error
func (ewc *ErrorWithCode) Unwrap() error {
	return ewc.err
}

// String implements the stringer interface
func (ewc *ErrorWithCode) String() string {
	return ewc.Error()
}

func NewBatchErrorWithQid(qid uint64) *BatchError {
	qidToBatchErrorMapLock.Lock()
	defer qidToBatchErrorMapLock.Unlock()
	be := &BatchError{
		beMap: &sync.Map{},
		qid:   int64(qid),
	}
	qidToBatchErrorMap[qid] = be
	return be
}

func GetBatchErrorWithQid(qid uint64) (*BatchError, bool) {
	qidToBatchErrorMapLock.RLock()
	defer qidToBatchErrorMapLock.RUnlock()
	be, exists := qidToBatchErrorMap[qid]
	return be, exists
}

func DeleteBatchErrorWithQid(qid uint64) {
	qidToBatchErrorMapLock.Lock()
	defer qidToBatchErrorMapLock.Unlock()
	delete(qidToBatchErrorMap, qid)
}

func GetOrCreateBatchErrorWithQid(qid uint64) *BatchError {
	if be, exists := GetBatchErrorWithQid(qid); exists {
		return be
	}
	return NewBatchErrorWithQid(qid)
}

func LogAllErrorsWithQidAndDelete(qid uint64) {
	if be, exists := GetBatchErrorWithQid(qid); exists {
		be.LogAllErrors()
		DeleteBatchErrorWithQid(qid)
	}
}

// WrapErrorf wraps the message with the error
// if err is of type ErrorWithCode, the code is preserved
func WrapErrorf(err error, message string, options ...any) error {
	if err == nil {
		return nil
	}

	if ewc, ok := err.(*ErrorWithCode); ok {
		return NewErrorWithCode(ewc.code, fmt.Errorf(message, options...))
	}

	return fmt.Errorf(message, options...)
}

func IsNilValueError(err error) bool {
	if ewc, ok := err.(*ErrorWithCode); ok {
		return ewc.code == NIL_VALUE_ERR
	}

	return false
}

func IsConversionError(err error) bool {
	if ewc, ok := err.(*ErrorWithCode); ok {
		return ewc.code == CONVERSION_ERR
	}

	return false
}

func IsNonNilValueError(err error) bool {
	return err != nil && !IsNilValueError(err)
}

func IsRPCUnavailableError(err error) bool {
	if ewc, ok := err.(*ErrorWithCode); ok {
		return ewc.code == "RPC__Unavailable"
	}

	return false
}
