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

package config

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"

	log "github.com/sirupsen/logrus"
)

var hostName string
var hostID string
var hostIP string
var smrBaseDir string

func GetBaseUploadDir() string {
	var sb strings.Builder
	sb.WriteString(GetRunningConfig().DataPath + "uploadjobs/")
	return sb.String()
}

func GetUploadFilename(baseDir string, streamid string) string {
	var sb strings.Builder

	err := os.MkdirAll(baseDir, 0764)
	if err != nil {
		log.Errorf("getUploadFilename: mkdir failed basedir=%v, err=%v", baseDir, err)
		return ""
	}
	sb.WriteString(baseDir)
	sb.WriteString(streamid + ".json")
	return sb.String()
}

func GetBaseGlobalSSRDir() string {
	var sb strings.Builder
	sb.WriteString(GetRunningConfig().DataPath + "common/")
	sb.WriteString("ssr/")
	return sb.String()
}

func GetSSRFilename(baseDir string) string {
	var sb strings.Builder

	err := os.MkdirAll(baseDir, 0764)
	if err != nil {
		log.Errorf("getSSRFilename: mkdir failed basedir=%v, err=%v", baseDir, err)
		return ""
	}
	sb.WriteString(baseDir)
	sb.WriteString("ssr.json")
	return sb.String()
}

func InitDerivedConfig(hostID string) error {
	if hostID == "" {
		return fmt.Errorf("InitDerivedConfig: hostID is empty")
	}
	if iName := GetSSInstanceName(); iName == "" {
		hostName, _ = os.Hostname()
	} else {
		hostName = iName
	}
	hostIP = getLocalIP()
	setNodeIdentifier(hostID)

	var sb strings.Builder
	sb.WriteString(GetDataPath() + "ingestnodes/")
	sb.WriteString(GetHostID() + "/")
	smrBaseDir = sb.String()

	err := os.MkdirAll(smrBaseDir, 0764)
	if err != nil {
		log.Errorf("InitDerivedConfig: mkdir failed basedir=%v, err=%v", smrBaseDir, err)
		return err
	}

	value := os.Getenv("PORT")
	if value != "" {
		queryPort, err := strconv.ParseUint(value, 10, 64)
		if err != nil {
			log.Errorf("InitDerivedConfig: failed to parse PORT=%v, err=%v", value, err)
		} else {
			SetQueryPort(queryPort)
		}
	}
	return nil
}

// Returns a string of the form <hostname>/<id>
// The id is unqiue across server restarts. The id changes only after the data directory is deleted
func GetHostID() string {
	return hostID
}

// returns hostname:ingestport
// TODO: caller assumes this is an ingest node
func GetHostIP() string {
	return hostName + ":" + fmt.Sprintf("%d", GetIngestPort())
}

// returns <<public ip>>:<grpcport> of the host
func GetPublicGRPCName() string {
	return hostIP + ":" + fmt.Sprintf("%d", GetRunningConfig().GRPCPort)
}

// this function returns the local ip address of the host
func getLocalIP() string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		log.Errorf("GetLocalIP: failed to get local ip: err=%v", err)
		return ""
	}
	var ip string
	for _, addr := range addrs {
		if ipnet, ok := addr.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				ip = ipnet.IP.String()
				break
			}
		}
	}
	return ip
}

func GetKeyFromSegfilename(s string) string {
	n := strings.LastIndexByte(s, '.')
	if n == -1 {
		return s
	}
	return s[:n]
}

func GetSmrBaseDir() string {
	return smrBaseDir
}

func SetSmrBaseDirForTestOnly(indir string) {
	smrBaseDir = indir
}

func setNodeIdentifier(nodeIdentifier string) {
	hostID = fmt.Sprintf("%s.%s", hostName, nodeIdentifier)
}

// Returns the hostname with no unique identifier
// This should be used if server restarts make no difference
func GetHostname() string {
	return hostName
}

// returns <<data path>>/ingestnodes/
func GetIngestNodeBaseDir() string {
	var sb strings.Builder
	sb.WriteString(GetDataPath() + "ingestnodes/")
	return sb.String()
}

// returns <<data path>>/ingestnodes/<hostid>/
func GetCurrentNodeIngestDir() string {
	var sb strings.Builder
	sb.WriteString(GetIngestNodeBaseDir())
	sb.WriteString(GetHostID())
	sb.WriteString("/")
	return sb.String()
}

func GetCurrentNodeQueryDir() string {
	var sb strings.Builder
	sb.WriteString(GetDataPath())
	sb.WriteString("querynodes/")
	sb.WriteString(GetHostID())
	sb.WriteString("/")
	return sb.String()
}
