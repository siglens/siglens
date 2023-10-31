/*
Copyright 2023.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

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
