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
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"strings"

	"github.com/siglens/siglens/pkg/config"
	log "github.com/sirupsen/logrus"
)

type SuffixEntry struct {
	Suffix uint64 `json:"suffix"`
}

func getSuffixFromFile(fileName string) (uint64, error) {
	f, err := os.OpenFile(fileName, os.O_RDWR, 0764)
	if os.IsNotExist(err) {
		err := os.MkdirAll(path.Dir(fileName), 0764)
		if err != nil {
			log.Errorf("getSuffixFromFile: directory %v does not exist and cannot be created, err=%v", path.Dir(fileName), err)
			return 0, err
		}
		f, err := os.Create(fileName)
		if err != nil {
			log.Errorf("getSuffixFromFile: Cannot create file %v, err=%v", fileName, err)
			return 0, err
		}
		defer f.Close()

		initialSuffix := &SuffixEntry{Suffix: 1}
		raw, err := json.Marshal(initialSuffix)
		if err != nil {
			log.Errorf("getSuffixFromFile: Cannot marshal initial suffix %v to json, err=%v", initialSuffix, err)
			return 0, err
		}
		_, err = f.Write(raw)
		if err != nil {
			log.Errorf("getSuffixFromFile: Cannot write initial suffix %v to file %v, err=%v", initialSuffix, fileName, err)
			return 0, err
		}
		_, err = f.WriteString("\n")
		if err != nil {
			log.Errorf("getSuffixFromFile: Cannot write newline to file %v (just created), err=%v", fileName, err)
			return 0, err
		}

		err = f.Sync()
		if err != nil {
			log.Errorf("getSuffixFromFile: Cannot sync file %v, err=%v", fileName, err)
			return 0, err
		}
		return 0, nil
	}
	if err != nil {
		log.Errorf("getSuffixFromFile: Cannot open file %v, err=%v", fileName, err)
		return 0, err
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	scanner.Scan()
	rawbytes := scanner.Bytes()
	if len(rawbytes) == 0 {
		log.Errorf("getSuffixFromFile: Empty suffix file %v", fileName)
		return 0, fmt.Errorf("empty suffix file %v", fileName)
	}
	var suffxE SuffixEntry
	err = json.Unmarshal(rawbytes, &suffxE)
	if err != nil {
		log.Errorf("getSuffixFromFile: Cannot unmarshal file=%v with data=%v from json, err=%v", fileName, string(rawbytes), err)
		return 0, err
	}
	retVal := suffxE.Suffix
	suffxE.Suffix++
	raw, err := json.Marshal(suffxE)
	if err != nil {
		log.Errorf("getSuffixFromFile: Cannot marshal suffix %v to json. file=%v, err=%v", suffxE, fileName, err)
		return 0, err
	}
	err = f.Truncate(0)
	if err != nil {
		log.Errorf("getSuffixFromFile: Cannot truncate file %v, err=%v", fileName, err)
		return 0, err
	}
	_, err = f.Seek(0, 0)
	if err != nil {
		log.Errorf("getSuffixFromFile: Cannot seek to start of file %v, err=%v", fileName, err)
		return 0, err
	}
	_, err = f.Write(raw)
	if err != nil {
		log.Errorf("getSuffixFromFile: Cannot write data %v to file %v, err=%v", string(raw), fileName, err)
		return 0, err
	}
	_, err = f.WriteString("\n")
	if err != nil {
		log.Errorf("getSuffixFromFile: Cannot write newline to file %v (already existed), err=%v", fileName, err)
		return 0, err
	}

	return retVal, nil
}

/*
Get the next suffix for the given streamid and table combination

Internally, creates & reads the suffix file persist suffixes
*/
func GetSuffix(streamid, table string) (uint64, error) {
	fileName := getSuffixFile(table, streamid)
	nextSuffix, err := getSuffixFromFile(fileName)
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
