// Copyright (c) 2026 The SigScalr Authors.
// SPDX-License-Identifier: Apache-2.0

package localnodeid

import (
	"os"

	"github.com/lithammer/shortuuid/v4"
	"github.com/siglens/siglens/pkg/config"
	log "github.com/sirupsen/logrus"
)

const DEFAULT_ALLOWED_VOLUME_GB = 100

var runningNodeId string
var initIDFile bool // if true, this data dir has been initalized for the first time

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
		log.Errorf("initNewNodeID: Failed to create common directory=%v, Error=%v", getCommonDir(), err)
		runningNodeId = nodeUUID
		return
	}

	fd, err := os.OpenFile(fName, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0755)
	if err != nil {
		log.Errorf("initNewNodeID: Failed to open node id file %s: Error=%+v", fName, err)
		runningNodeId = nodeUUID
		return
	}
	defer fd.Close()
	_, err = fd.WriteString(nodeUUID)
	if err != nil {
		log.Errorf("initNewNodeID: Failed to write to node id file %s: Error=%+v", fName, err)
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
