// Copyright (c) 2026 The SigScalr Authors.
// SPDX-License-Identifier: Apache-2.0

package utils

import (
	"fmt"

	log "github.com/sirupsen/logrus"
)

func TeeErrorf(format string, args ...interface{}) error {
	err := fmt.Errorf(format, args...)
	log.Error(err.Error())

	return err
}

func LogUsingLevel(level log.Level, format string, args ...interface{}) {
	switch level {
	case log.TraceLevel:
		log.Tracef(format, args...)
	case log.DebugLevel:
		log.Debugf(format, args...)
	case log.InfoLevel:
		log.Infof(format, args...)
	case log.WarnLevel:
		log.Warnf(format, args...)
	case log.ErrorLevel:
		log.Errorf(format, args...)
	case log.FatalLevel:
		log.Fatalf(format, args...)
	case log.PanicLevel:
		log.Panicf(format, args...)
	default:
		log.Infof(format, args...)
	}
}
