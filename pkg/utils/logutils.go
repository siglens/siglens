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
