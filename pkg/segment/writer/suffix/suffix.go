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

package suffix

import (
	"encoding/json"
	"os"
	"path"
	"strings"

	"github.com/siglens/siglens/pkg/config"
	log "github.com/sirupsen/logrus"
)

type entry struct {
	NextSuffix uint64 `json:"suffix"`
}

func getSuffix(fileName string) (*entry, error) {
	jsonBytes, err := os.ReadFile(fileName)
	if os.IsNotExist(err) {
		return &entry{NextSuffix: 0}, nil
	}
	if err != nil {
		log.Errorf("getSuffix: Cannot read file %v, err=%v", fileName, err)
		return nil, err
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

	err = os.WriteFile(fileName, jsonBytes, 0644)
	if err != nil {
		log.Errorf("writeSuffix: Cannot write json=%s to file=%v; err=%v", jsonBytes, fileName, err)
		return err
	}

	return nil
}

func getAndIncrementSuffixFromFile(fileName string) (uint64, error) {
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

	nextSuffix := entry.NextSuffix
	entry.NextSuffix++
	err = writeSuffix(fileName, entry)
	if err != nil {
		log.Errorf("getAndIncrementSuffixFromFile: Cannot write suffix to file %v; err=%v", fileName, err)
		return 0, err
	}

	return nextSuffix, nil
}

/*
Get the next suffix for the given streamid and table combination

Internally, creates & reads the suffix file persist suffixes
*/
func GetNextSuffix(streamid, table string) (uint64, error) {
	fileName := getSuffixFile(table, streamid)
	nextSuffix, err := getAndIncrementSuffixFromFile(fileName)
	if err != nil {
		log.Errorf("GetSuffix: Error generating suffix for streamid=%v, table=%v. Err: %v", streamid, table, err)
		return 0, err
	}

	return nextSuffix, nil
}

func getSuffixFile(virtualTable string, streamId string) string {
	var sb strings.Builder
	sb.WriteString(config.GetDataPath())
	sb.WriteString(config.GetHostID())
	sb.WriteString("/suffix/")
	sb.WriteString(virtualTable)
	sb.WriteString("/")
	sb.WriteString(streamId)
	sb.WriteString(".suffix")
	return sb.String()
}
