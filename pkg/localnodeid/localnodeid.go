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

package localnodeid

import (
	"os"

	"github.com/lithammer/shortuuid/v4"
	"github.com/siglens/siglens/pkg/config"
	log "github.com/sirupsen/logrus"
)

const DEFAULT_ALLOWED_VOLUME_GB = 100

var runningNodeId string
var initIDFile bool // if true, this data dir has been initialized for the first time

func getNodeIdFile() string {
	return config.GetDataPath() + "common/id.info"
}

func getCommonDir() string {
	return config.GetDataPath() + "common/"
}

func initNewNodeID(fName string) {
	nodeUUID := shortuuid.New()
	err := os.MkdirAll(getCommonDir(), 0755)
	if err != nil {
		log.Errorf("Failed to create common directory: %v", err)
		runningNodeId = nodeUUID
		return
	}

	fd, err := os.OpenFile(fName, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0755)
	if err != nil {
		log.Errorf("Failed to open node id file %s: %+v", fName, err)
		runningNodeId = nodeUUID
		return
	}
	defer fd.Close()
	_, err = fd.WriteString(nodeUUID)
	if err != nil {
		log.Errorf("Failed to write to node id file %s: %+v", fName, err)
		runningNodeId = nodeUUID
		return
	}
	_ = fd.Sync()
	_ = fd.Chmod(0444)
	runningNodeId = nodeUUID
	initIDFile = true
}

func fetchNodeIdentifier() {
	// check if a node identifier file already exists, else init a uuid and flush it to disk
	fName := getNodeIdFile()
	if _, err := os.Stat(fName); err != nil {
		initNewNodeID(fName)
		return
	}

	readID, err := os.ReadFile(fName)
	if err != nil {
		initNewNodeID(fName)
		return
	}
	runningNodeId = string(readID)
	if len(runningNodeId) == 0 {
		initNewNodeID(fName)
	}
}

// returns true if this proc starts a new id file. returns false if an id file already exists
func IsInitServer() bool {
	return initIDFile
}

func GetRunningNodeID() string {
	if runningNodeId == "" {
		fetchNodeIdentifier()
	}
	return runningNodeId
}
