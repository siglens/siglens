// Copyright (c) 2026 The SigScalr Authors.
// SPDX-License-Identifier: Apache-2.0

package suffix

import (
	"encoding/json"
	"fmt"
	"os"
	"path"
	"sync"

	"github.com/siglens/siglens/pkg/config"
	"github.com/siglens/siglens/pkg/hooks"
	log "github.com/sirupsen/logrus"
)

type entry struct {
	NextSuffix uint64 `json:"suffix"`
}

var suffixWriteLock sync.Mutex

func getSuffix(fileName string) (*entry, error) {
	jsonBytes, err := os.ReadFile(fileName)
	if os.IsNotExist(err) {
		return &entry{NextSuffix: 0}, nil
	}
	if err != nil {
		log.Errorf("getSuffix: Cannot read file %v, err=%v", fileName, err)
		return nil, err
	}

	// Handle an empty file.
	if len(jsonBytes) == 0 {
		log.Warnf("getSuffix: File %v is empty", fileName)
		return &entry{NextSuffix: 0}, nil
	}

	var entry entry
	err = json.Unmarshal(jsonBytes, &entry)
	if err != nil {
		log.Errorf("getSuffix: Cannot unmarshal json=%s from file=%v; err=%v", jsonBytes, fileName, err)
		return nil, err
	}

	return &entry, nil
}

func writeSuffix(fileName string, entry *entry) error {
	jsonBytes, err := json.Marshal(entry)
	if err != nil {
		log.Errorf("writeSuffix: Cannot marshal entry=%v to json; err=%v", entry, err)
		return err
	}

	suffixWriteLock.Lock()
	defer suffixWriteLock.Unlock()

	tempFileName := fileName + ".tmp"
	err = os.WriteFile(tempFileName, jsonBytes, 0644)
	if err != nil {
		log.Errorf("writeSuffix: Cannot write json=%s to file=%v; err=%v", jsonBytes, tempFileName, err)
		return err
	}

	err = os.Rename(tempFileName, fileName)
	if err != nil {
		log.Errorf("writeSuffix: Cannot rename file=%v to file=%v; err=%v", tempFileName, fileName, err)
		return err
	}

	return nil
}

func getAndIncrementSuffixFromFile(fileName string, getSegKey func(suffix uint64) string) (uint64, error) {
	dir := path.Dir(fileName)
	err := os.MkdirAll(dir, 0755)
	if err != nil {
		log.Errorf("getAndIncrementSuffixFromFile: Cannot create directory %v; err=%v", dir, err)
		return 0, err
	}

	entry, err := getSuffix(fileName)
	if err != nil {
		log.Errorf("getAndIncrementSuffixFromFile: Cannot get suffix from file %v; err=%v", fileName, err)
		return 0, err
	}

	if hook := hooks.GlobalHooks.GetNextSuffixHook; hook != nil {
		if getSegKey == nil {
			return 0, fmt.Errorf("getAndIncrementSuffixFromFile: getSegKey is nil")
		}

		suffix, err := hook(entry.NextSuffix, getSegKey)
		if err != nil {
			log.Errorf("getAndIncrementSuffixFromFile: Cannot get suffix from hook; err=%v", err)
			return 0, err
		}

		entry.NextSuffix = suffix
	}

	resultSuffix := entry.NextSuffix

	entry.NextSuffix++
	err = writeSuffix(fileName, entry)
	if err != nil {
		log.Errorf("getAndIncrementSuffixFromFile: Cannot write suffix to file %v; err=%v", fileName, err)
		return 0, err
	}

	return resultSuffix, nil
}

/*
Get the next suffix for the given streamid and table combination

Internally, creates & reads the suffix file persist suffixes
*/
func GetNextSuffix(streamid, table string) (uint64, error) {
	fileName := config.GetSuffixFile(table, streamid)
	getSegKey := func(suffix uint64) string {
		return config.GetSegKey(streamid, table, suffix)
	}

	nextSuffix, err := getAndIncrementSuffixFromFile(fileName, getSegKey)
	if err != nil {
		log.Errorf("GetSuffix: Error generating suffix for streamid=%v, table=%v. Err: %v", streamid, table, err)
		return 0, err
	}

	return nextSuffix, nil
}
