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
	"encoding/gob"
	"fmt"
	"path/filepath"
	"runtime"
	"strings"

	log "github.com/sirupsen/logrus"
)

// Use this at the start of a function like this:
// SigDebugEnter(fmt.Sprintf("parameter foo=%v", foo))
// OR
// SigDebugEnter("")
func SigDebugEnter(extraMessage string) {
	// Get information about the function we're entering.
	programCounter, file, line, ok := runtime.Caller(1)
	if !ok {
		log.Warnf("SigDebugEnter: Unable to get caller information")
		return
	}
	funcName := extractFuncName(runtime.FuncForPC(programCounter).Name())
	fileName := filepath.Base(file)

	// Get information about the caller.
	var message string
	callerProgramCounter, callerFile, callerLine, callerOk := runtime.Caller(2)
	if callerOk {
		callerFuncName := extractFuncName(runtime.FuncForPC(callerProgramCounter).Name())
		callerFileName := filepath.Base(callerFile)
		message = fmt.Sprintf("Entering %s (%s:%d) from %s at %s:%d",
			funcName, fileName, line, callerFuncName, callerFileName, callerLine)
	} else {
		message = fmt.Sprintf("Entering %s (%s:%d) (cannot determine caller)", funcName, fileName, line)
	}

	if extraMessage != "" {
		message += "; " + extraMessage
	}

	log.Infof(message)
}

// Use this at the start of a function like this:
// defer SigDebugExit(func() string { return fmt.Sprintf("final foo=%v", foo) })
// OR
// defer SigDebugExit(nil)
//
// Note: this uses a function to compute the extra message so that when it's
// used with defer, the values captured are the values when the defer is
// executed, not when the defer is declared.
func SigDebugExit(computeExtraMessage func() string) {
	programCounter, file, line, ok := runtime.Caller(1)
	if !ok {
		log.Warnf("SigDebugExit: Unable to get caller information")
		return
	}
	funcName := extractFuncName(runtime.FuncForPC(programCounter).Name())
	fileName := filepath.Base(file)

	message := fmt.Sprintf("Exiting %s at %s:%d", funcName, fileName, line)

	if computeExtraMessage != nil {
		message += "; " + computeExtraMessage()
	}

	log.Infof(message)
}

// Takes a full function name like:
// github.com/siglens/siglens/pkg/segment/aggregations.PostQueryBucketCleaning
// and returns just the function name: PostQueryBucketCleaning
func extractFuncName(funcName string) string {
	funcNameSplit := strings.Split(funcName, ".")
	return funcNameSplit[len(funcNameSplit)-1]
}

// Useful for estimating runtime size of structs.
func GetGobSize(v interface{}) int {
	var buffer bytes.Buffer
	encoder := gob.NewEncoder(&buffer)

	err := encoder.Encode(v)
	if err != nil {
		return 0
	}

	return buffer.Len()
}

func GetNCallers(n int) []string {
	var callers []string
	for i := 0; i < n; i++ {
		programCounter, file, line, ok := runtime.Caller(i + 1) // Skip this function.
		if !ok {
			break
		}
		funcName := extractFuncName(runtime.FuncForPC(programCounter).Name())
		fileName := filepath.Base(file)
		callers = append(callers, fmt.Sprintf("%s(%s:%d)", funcName, fileName, line))
	}

	return callers
}
