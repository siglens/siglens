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
	"regexp"

	log "github.com/sirupsen/logrus"
)

type SerializableRegex struct {
	rawRegex      string
	compiledRegex *regexp.Regexp
}

func (self *SerializableRegex) GetCompiledRegex() *regexp.Regexp {
	return self.compiledRegex
}

func (self *SerializableRegex) SetRegex(raw string) error {
	compiled, err := regexp.Compile(raw)
	if err != nil {
		log.Errorf("SerizalizableRegex: failed to compile regex \"%v\", err=%v", raw, err)
		return err
	}

	self.rawRegex = raw
	self.compiledRegex = compiled

	return nil
}
