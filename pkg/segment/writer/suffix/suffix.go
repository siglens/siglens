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
			return 0, err
		}
		f, err := os.Create(fileName)
		if err != nil {
			return 0, err
		}
		defer f.Close()
		if err != nil {
			return 0, err
		}

		initialSuffix := &SuffixEntry{Suffix: 1}
		raw, err := json.Marshal(initialSuffix)
		if err != nil {
			return 0, err
		}
		_, err = f.Write(raw)
		if err != nil {
			return 0, err
		}
		_, err = f.WriteString("\n")
		if err != nil {
			return 0, err
		}

		err = f.Sync()
		if err != nil {
			return 0, err
		}
		return 0, err
	}
	scanner := bufio.NewScanner(f)
	scanner.Scan()
	rawbytes := scanner.Bytes()
	var suffxE SuffixEntry
	err = json.Unmarshal(rawbytes, &suffxE)
	if err != nil {
		log.Errorf("GetSuffix: Cannot unmarshal data = %v, err= %v", string(rawbytes), err)
		return 0, err
	}
	retVal := suffxE.Suffix
	suffxE.Suffix++
	raw, err := json.Marshal(suffxE)
	if err != nil {
		return 0, err
	}
	err = f.Truncate(0)
	if err != nil {
		return 0, err
	}
	_, err = f.Seek(0, 0)
	if err != nil {
		return 0, err
	}
	_, err = f.Write(raw)
	if err != nil {
		return 0, err
	}
	_, err = f.WriteString("\n")
	if err != nil {
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
		log.Errorf("GetSuffix: Error generating suffix for streamid %v. Err: %v", streamid, err)
	}
	return nextSuffix, err
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
