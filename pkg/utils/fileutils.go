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
	"os"

	log "github.com/sirupsen/logrus"
)

type WriteMode int

const (
	Append WriteMode = iota
	Truncate
)

func AtomicWriteFile(fileName string, data []byte, mode WriteMode) error {
	tempFileName := fileName + ".tmp"
	fd, err := os.Create(tempFileName)
	if err != nil {
		log.Errorf("AtomicWriteFile: Cannot create temp file %v, err=%v", tempFileName, err)
		return err
	}
	defer fd.Close()

	// Write to the temp file.
	switch mode {
	case Append:
		existingData, err := os.ReadFile(fileName)
		if err != nil && !os.IsNotExist(err) {
			log.Errorf("AtomicWriteFile: Cannot read file %v, err=%v", fileName, err)
			return err
		}

		_, err = fd.Write(existingData)
		if err != nil {
			log.Errorf("AtomicWriteFile: Cannot write to temp file %v, err=%v", tempFileName, err)
			return err
		}

		_, err = fd.Write(data)
		if err != nil {
			log.Errorf("AtomicWriteFile: Cannot write to temp file %v, err=%v", tempFileName, err)
			return err
		}
	case Truncate:
		_, err = fd.Write(data)
		if err != nil {
			log.Errorf("AtomicWriteFile: Cannot write to temp file %v, err=%v", tempFileName, err)
			return err
		}
	default:
		err = fmt.Errorf("Invalid write mode %v", mode)
		log.Errorf("AtomicWriteFile: %v", err)
		return err
	}

	// Rename the temp file to the original file.
	err = os.Rename(tempFileName, fileName)
	if err != nil {
		log.Errorf("AtomicWriteFile: Cannot rename temp file %v to %v, err=%v", tempFileName, fileName, err)
		return err
	}

	return nil
}
