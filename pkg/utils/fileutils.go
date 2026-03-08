// Copyright (c) 2026 The SigScalr Authors.
// SPDX-License-Identifier: Apache-2.0

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

func GetFileSize(path string) (int64, error) {
	fileInfo, err := os.Stat(path)
	if err != nil {
		return 0, fmt.Errorf("GetFileSize: failed to get file info for %s: %w", path, err)
	}
	return fileInfo.Size(), nil
}
