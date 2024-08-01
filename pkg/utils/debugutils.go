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
	"path/filepath"
	"runtime"
	"strings"

	log "github.com/sirupsen/logrus"
)

// Use this at the start of a function like this:
// TraceEnter(fmt.Sprintf("parameter foo=%v", foo))
// OR
// TraceEnter("")
func TraceEnter(extraMessage string) {
	// Get information about the function we're entering.
	pc, file, line, ok := runtime.Caller(1)
	if !ok {
		log.Warnf("TraceEnter: Unable to get caller information")
		return
	}
	funcName := extractFuncName(runtime.FuncForPC(pc).Name())
	fileName := filepath.Base(file)

	// Get information about the caller.
	var message string
	callerPc, callerFile, callerLine, callerOk := runtime.Caller(2)
	if callerOk {
		callerFuncName := extractFuncName(runtime.FuncForPC(callerPc).Name())
		callerFileName := filepath.Base(callerFile)
		message = fmt.Sprintf("Entering %s at %s:%d from %s at %s:%d",
			funcName, fileName, line, callerFuncName, callerFileName, callerLine)
	} else {
		message = fmt.Sprintf("Entering %s at %s:%d (cannot determine caller)", funcName, fileName, line)
	}

	if extraMessage != "" {
		message += "; " + extraMessage
	}

	log.Infof(message)
}

// Use this at the start of a function like this:
// defer TraceExit(func() string { return fmt.Sprintf("final foo=%v", foo) })
// OR
// defer TraceExit(nil)
func TraceExit(computeExtraMessage func() string) {
	pc, file, line, ok := runtime.Caller(1)
	if !ok {
		log.Warnf("TraceExit: Unable to get caller information")
		return
	}
	funcName := extractFuncName(runtime.FuncForPC(pc).Name())
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
