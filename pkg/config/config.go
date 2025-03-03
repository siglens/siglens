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

package config

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/pbnjay/memory"
	"github.com/siglens/siglens/pkg/config/common"
	"github.com/siglens/siglens/pkg/hooks"
	segutils "github.com/siglens/siglens/pkg/segment/utils"
	"github.com/siglens/siglens/pkg/utils"
	log "github.com/sirupsen/logrus"
	"github.com/valyala/fasthttp"
	"gopkg.in/yaml.v3"
)

type ValuesRangeConfig struct {
	Min     int
	Max     int
	Default int
}

const MINUTES_REREAD_CONFIG = 2
const RunModFilePath = "data/common/runmod.cfg"

const SIZE_8GB_IN_MB = uint64(8192)

// How memory is split for rotated info. These should sum to 100.
const DEFAULT_ROTATED_CMI_MEM_PERCENT = 48
const DEFAULT_METADATA_MEM_PERCENT = 20
const DEFAULT_SEG_SEARCH_MEM_PERCENT = 30 // minimum percent allocated for segsearch
const DEFAULT_METRICS_MEM_PERCENT = 2
const DEFAULT_BYTES_PER_QUERY = 200 * 1024 * 1024 // 200MB

const DEFAULT_MAX_ALLOWED_COLUMNS = 20_000 // Max concurrent unrotated columns across all indexes

var configFileLastModified uint64

var runningConfig common.Configuration
var configFilePath string

var parallelism int64

const cgroupV1MaxMemoryPath = "/sys/fs/cgroup/memory/memory.limit_in_bytes"
const cgroupV2MaxMemoryPath = "/sys/fs/cgroup/memory.max"

const cgroupV1UsageMemoryPath = "/sys/fs/cgroup/memory/memory.usage_in_bytes"
const cgroupV2UsageMemoryPath = "/sys/fs/cgroup/memory.current"

type memoryValueType uint8

const (
	memoryValueMax memoryValueType = iota + 1
	memoryValueUsage
)

type cgroupVersion uint8

const (
	noCgroupVersion cgroupVersion = iota
	cgroupVersion1
	cgroupVersion2
)

var cgroupVersionDetected cgroupVersion

var memoryFilePaths struct {
	MaxPaths   []string
	UsagePaths []string
}

var idleWipFlushRange = ValuesRangeConfig{Min: 5, Max: 60, Default: 5}
var maxWaitWipFlushRange = ValuesRangeConfig{Min: 5, Max: 60, Default: 30}

var tracingEnabled bool // flag to enable/disable tracing; Set to true if TracingConfig.Endpoint != ""

const (
	MIN_QUERY_TIMEOUT_SECONDS = 60   // 1 minute
	MAX_QUERY_TIMEOUT_SECONDS = 1800 // 30 minutes
	DEFAULT_TIMEOUT_SECONDS   = 300  // 5 minutes
)

const DEFAULT_DISK_THRESHOLD_PERCENT uint64 = 95

func init() {
	parallelism = int64(runtime.GOMAXPROCS(0))
	if parallelism <= 1 {
		parallelism = 2
	}

	cgroupVersionDetected = detectCgroupVersion()
	initMemoryPaths()
}

func detectCgroupVersion() cgroupVersion {
	// Detect non-Linux OS
	if runtime.GOOS != "linux" {
		log.Infof("Non-Linux OS detected. Assuming no cgroup support.")
		return noCgroupVersion
	}

	data, err := os.ReadFile("/proc/mounts")
	if err != nil {
		log.Warnf("detectCgroupVersion: Error reading /proc/mounts: %v", err)
	} else {
		if strings.Contains(string(data), " cgroup2 ") {
			log.Infof("detectCgroupVersion: Detected cgroup v2")
			return cgroupVersion2
		}

		if strings.Contains(string(data), " cgroup ") {
			log.Infof("detectCgroupVersion: Detected cgroup v1 through /proc/mounts")
			return cgroupVersion1
		}
	}

	data, err = os.ReadFile("/proc/self/cgroup")
	if err == nil && strings.Contains(string(data), ":memory:") {
		log.Infof("detectCgroupVersion: Detected cgroup v1 through /proc/self/cgroup")
		return cgroupVersion1
	}

	log.Warn("detectCgroupVersion: Unable to detect cgroup version")
	return noCgroupVersion
}

// Detect whether the system is using cgroup v2
func isCgroupVersion2() bool {
	return cgroupVersionDetected == cgroupVersion2
}

func initMemoryPaths() {
	switch cgroupVersionDetected {
	case noCgroupVersion:
		return
	case cgroupVersion2:
		memoryFilePaths.MaxPaths = []string{cgroupV2MaxMemoryPath}
		memoryFilePaths.UsagePaths = []string{cgroupV2UsageMemoryPath}
	case cgroupVersion1:
		memoryFilePaths.MaxPaths = []string{cgroupV1MaxMemoryPath}
		memoryFilePaths.UsagePaths = []string{cgroupV1UsageMemoryPath}
	}

	if isRunningInKubernetes() {
		// Add Kubernetes-specific paths
		memoryFilePaths.MaxPaths = append(memoryFilePaths.MaxPaths,
			"/sys/fs/cgroup/kubepods/memory.max",                 // Kubernetes cgroup v2
			"/sys/fs/cgroup/kubepods.slice/memory.max",           // Kubernetes cgroup v2 with systemd
			"/sys/fs/cgroup/kubepods/burstable/memory.max",       // Kubernetes burstable v2
			"/sys/fs/cgroup/kubepods/burstable.slice/memory.max", // Kubernetes burstable v2 with systemd
		)

		memoryFilePaths.UsagePaths = append(memoryFilePaths.UsagePaths,
			"/sys/fs/cgroup/kubepods/memory.current",                 // Kubernetes cgroup v2
			"/sys/fs/cgroup/kubepods.slice/memory.current",           // Kubernetes cgroup v2 with systemd
			"/sys/fs/cgroup/kubepods/burstable/memory.current",       // Kubernetes burstable v2
			"/sys/fs/cgroup/kubepods/burstable.slice/memory.current", // Kubernetes burstable v2 with systemd
		)
	}
}

func isRunningInKubernetes() bool {
	// Check Kubernetes environment variables
	if os.Getenv("KUBERNETES_SERVICE_HOST") != "" && os.Getenv("KUBERNETES_SERVICE_PORT") != "" {
		return true
	}

	// Check ServiceAccount token file
	if _, err := os.Stat("/var/run/secrets/kubernetes.io/serviceaccount/token"); err == nil {
		return true
	}

	return false
}

// Detect dynamic cgroup path (Kubernetes/Docker support)
func detectCgroupPath(basePath string) string {
	isCgroupV2 := isCgroupVersion2()
	data, err := os.ReadFile("/proc/self/cgroup")
	if err != nil {
		return basePath // Fallback to static path
	}

	// Example content of /proc/self/cgroup:
	// 11:memory:/kubepods.slice/kubepods-burstable.slice/pod1234
	// 10:cpuset:/kubepods.slice/kubepods-burstable.slice/pod1234
	// ...

	// Split the data into lines
	lines := strings.Split(string(data), "\n")
	for _, line := range lines {
		// <hierarchy-ID>:<subsystem>:<cgroup-path>
		parts := strings.Split(line, ":")
		if len(parts) < 3 {
			continue
		}

		// Example:
		// parts[0] = "11"       // hierarchy ID
		// parts[1] = "memory"   // subsystem
		// parts[2] = "/kubepods.slice/kubepods-burstable.slice/pod1234" // cgroup path

		// Check if it's a memory cgroup or a unified hierarchy (cgroup v2)
		if isCgroupV2 || strings.Contains(parts[1], "memory") {
			cgroupPath := strings.TrimSpace(parts[2])
			if cgroupPath == "/" { // Root path
				return basePath
			}

			// Build dynamic path by replacing the base directory
			newPath := fmt.Sprintf("/sys/fs/cgroup%s/%s", cgroupPath, path.Base(basePath))
			if _, err := os.Stat(newPath); err == nil { // Check if path exists
				return newPath
			}

			// Check one level deeper (common in Kubernetes)
			nestedPath := fmt.Sprintf("/sys/fs/cgroup%s/%s", cgroupPath, path.Base(path.Dir(basePath)))
			if _, err := os.Stat(nestedPath); err == nil {
				return nestedPath
			}
		}
	}

	return basePath
}

func getContainerMemory(memoryValueType memoryValueType) (uint64, error) {
	if cgroupVersionDetected == noCgroupVersion {
		return 0, fmt.Errorf("getContainerMemory: Cgroup version not detected")
	}

	var memFilePaths []string

	switch memoryValueType {
	case memoryValueMax:
		memFilePaths = memoryFilePaths.MaxPaths
	case memoryValueUsage:
		memFilePaths = memoryFilePaths.UsagePaths
	default:
		return 0, fmt.Errorf("getContainerMemory: Invalid memoryValueType: %v", memoryValueType)
	}

	resolvedPath := detectCgroupPath(memFilePaths[0])
	if resolvedPath != memFilePaths[0] {
		log.Infof("getContainerMemory: Detected dynamic cgroup path: %v", resolvedPath)
		memFilePaths = append([]string{resolvedPath}, memFilePaths...)
	}

	var memory uint64

	for _, path := range memFilePaths {
		data, err := os.ReadFile(path)
		if err != nil {
			if !os.IsNotExist(err) {
				return 0, err
			}
			continue
		}

		memory, err = strconv.ParseUint(strings.TrimSpace(string(data)), 10, 64)
		if err != nil {
			return 0, fmt.Errorf("getContainerMemory: Error while converting memory limit: %v to uint64", string(data))
		}

		log.Infof("getContainerMemory: Fetching memory from cgroup file: %v", path)

		break
	}

	if memory == 0 {
		return 0, fmt.Errorf("getContainerMemory: Memory limit not found")
	}

	return memory, nil
}

func GetTotalMemoryAvailableToUse() uint64 {
	var gogc uint64
	v := os.Getenv("GOGC")
	if v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			log.Errorf("GetTotalMemoryAvailableToUse: Error while converting gogc: %v to int", v)
			n = 100
		}
		gogc = uint64(n)
	} else {
		gogc = 100
	}

	totalMemoryOnHost := getMaxMemoryAllowedToUseInBytesFromConfig()
	if totalMemoryOnHost == 0 {
		totalMemoryOnHost = GetMemoryMax()
	} else {
		log.Infof("GetTotalMemoryAvailableToUse: Using the total memory value set in the config. Memory: %v MB", segutils.ConvertUintBytesToMB(totalMemoryOnHost))
	}

	configuredMemory := totalMemoryOnHost * runningConfig.MemoryConfig.MaxUsagePercent / 100
	allowedMemory := configuredMemory / (1 + gogc/100)
	log.Infof("GetTotalMemoryAvailableToUse: GOGC: %+v, MemThresholdPerc: %v, HostRAM: %+v MB, RamAllowedToUse: %v MB", gogc,
		runningConfig.MemoryConfig.MaxUsagePercent,
		segutils.ConvertUintBytesToMB(totalMemoryOnHost),
		segutils.ConvertUintBytesToMB(allowedMemory))
	return allowedMemory
}

func GetMemoryMax() uint64 {
	var memoryMax uint64

	// try to get the memory from the cgroup
	memoryMax, err := getContainerMemory(memoryValueMax)
	if err != nil {
		log.Warnf("GetMemoryMax: Error while getting memory from cgroup: %v", err)
		// if we can't get the memory from the cgroup, get it from the OS
		memoryMax = memory.TotalMemory()
		log.Infof("GetMemoryMax: Memory from the Host in MB: %v", segutils.ConvertUintBytesToMB(memoryMax))
	} else {
		log.Infof("GetMemoryMax: Memory from cgroup in MB: %v", segutils.ConvertUintBytesToMB(memoryMax))
	}

	return memoryMax
}

func GetContainerMemoryUsage() (uint64, error) {
	var memoryInUse uint64

	// try to get the memory from the cgroup
	memoryInUse, err := getContainerMemory(memoryValueUsage)
	if err != nil {
		log.Debugf("GetContainerMemoryUsage: Error while getting memory from cgroup: %v", err)
		return 0, err
	}

	return memoryInUse, nil
}

func GetMemoryConfig() common.MemoryConfig {
	return runningConfig.MemoryConfig
}

func getMaxMemoryAllowedToUseInBytesFromConfig() uint64 {
	return runningConfig.MemoryConfig.MaxMemoryAllowedToUseInBytes
}

func GetMaxAllowedColumns() uint64 {
	return runningConfig.MaxAllowedColumns
}

/*
Returns GOMAXPROCS
*/
func GetParallelism() int64 {
	return parallelism
}

func GetDataDiskThresholdPercent() uint64 {
	return runningConfig.DataDiskThresholdPercent
}

func GetRunningConfig() *common.Configuration {
	return &runningConfig
}

func GetSSInstanceName() string {
	return runningConfig.SSInstanceName
}

func GetRetentionHours() int {
	return runningConfig.RetentionHours
}

func IsS3Enabled() bool {
	return runningConfig.S3.Enabled
}

func SetS3Enabled(flag bool) {
	runningConfig.S3.Enabled = flag
}

func GetS3BucketName() string {
	return runningConfig.S3.BucketName
}

func SetS3BucketName(bname string) {
	runningConfig.S3.BucketName = bname
}

func GetS3Region() string {
	return runningConfig.S3.RegionName
}

func SetS3Region(region string) {
	runningConfig.S3.RegionName = region
}

func GetS3BucketPrefix() string {
	return runningConfig.S3.BucketPrefix
}

func GetMaxSegFileSize() uint64 {
	return runningConfig.MaxSegFileSize
}

func GetESVersion() *string {
	return &runningConfig.ESVersion
}

// returns the configured ingest listen IP Addr
// if the node is not an ingest node, this will not be set
func GetIngestListenIP() string {
	return runningConfig.IngestListenIP
}

// returns the configured query listen IP Addr
// if the node is not a query node, this will not be set
func GetQueryListenIP() string {
	return runningConfig.QueryListenIP
}

// returns the configured ingest port
// if the node is not an ingest node, this will not be set
func GetIngestPort() uint64 {
	return runningConfig.IngestPort
}

// returns the configured query port
// if the node is not a query node, this will not be set
func GetQueryPort() uint64 {
	return runningConfig.QueryPort
}

func GetDataPath() string {
	return runningConfig.DataPath
}

func GetLookupPath() string {
	return runningConfig.DataPath + "lookups/"
}

// returns if tls is enabled
func IsTlsEnabled() bool {
	return runningConfig.TLS.Enabled
}

func IsMtlsEnabled() bool {
	return runningConfig.TLS.MtlsEnabled.Value()
}

func GetMtlsClientCaPath() string {
	return runningConfig.TLS.ClientCaPath
}

// returns the configured certificate path
func GetTLSCertificatePath() string {
	return runningConfig.TLS.CertificatePath
}

// returns the configured private key path
func GetTLSPrivateKeyPath() string {
	return runningConfig.TLS.PrivateKeyPath
}

func ShouldCompressStaticFiles() bool {
	return runningConfig.CompressStaticConverted
}

// used by
func GetQueryHostname() string {
	return runningConfig.QueryHostname
}

func SetTracingEnabled(flag bool) {
	tracingEnabled = flag
}

func IsTracingEnabled() bool {
	return tracingEnabled
}

func GetTracingServiceName() string {
	return runningConfig.Tracing.ServiceName
}

func GetTracingEndpoint() string {
	return runningConfig.Tracing.Endpoint
}

func GetTraceSamplingPercentage() float64 {
	return runningConfig.Tracing.SamplingPercentage
}

// returns SmtpHost, SmtpPort, SenderEmail and GmailAppPassword
func GetEmailConfig() (string, int, string, string) {
	return runningConfig.EmailConfig.SmtpHost, runningConfig.EmailConfig.SmtpPort, runningConfig.EmailConfig.SenderEmail, runningConfig.EmailConfig.GmailAppPassword
}

func SetEmailConfig(smtpHost string, smtpPort int, senderEmail string, gmailAppPassword string) {
	runningConfig.EmailConfig.SmtpHost = smtpHost
	runningConfig.EmailConfig.SmtpPort = smtpPort
	runningConfig.EmailConfig.SenderEmail = senderEmail
	runningConfig.EmailConfig.GmailAppPassword = gmailAppPassword
}

func GetUIDomain() string {
	hostname := GetQueryHostname()
	if hostname == "" {
		return "localhost"
	} else {
		host, _, err := net.SplitHostPort(hostname)
		if err != nil {
			log.Errorf("GetUIDomain: Failed to parse QueryHostname: %v, err: %v", hostname, err)
			return hostname
		}
		return host
	}
}

func GetSiglensDBConfig() (string, string, uint64, string, string, string) {
	return runningConfig.DatabaseConfig.Provider, runningConfig.DatabaseConfig.Host, runningConfig.DatabaseConfig.Port, runningConfig.DatabaseConfig.User, runningConfig.DatabaseConfig.Password, runningConfig.DatabaseConfig.Dbname
}

func GetLogPrefix() string {
	return runningConfig.Log.LogPrefix
}

func IsDebugMode() bool {
	return runningConfig.Debug
}

func IsPProfEnabled() bool {
	return runningConfig.PProfEnabledConverted
}

func IsPQSEnabled() bool {
	return runningConfig.PQSEnabledConverted
}

func IsAggregationsEnabled() bool {
	return runningConfig.AgileAggsEnabledConverted
}

func IsDualCaseCheckEnabled() bool {
	return runningConfig.DualCaseCheckConverted
}

func SetAggregationsFlag(enabled bool) {
	runningConfig.AgileAggsEnabledConverted = enabled
	runningConfig.AgileAggsEnabled = strconv.FormatBool(enabled)
}

func IsAnalyticsEnabled() bool {
	return runningConfig.AnalyticsEnabledConverted
}

func IsSafeMode() bool {
	return runningConfig.SafeServerStart
}

func GetRunningConfigAsJsonStr() (string, error) {
	buffer := new(bytes.Buffer)
	encoder := json.NewEncoder(buffer)
	encoder.SetIndent("", "")
	err := encoder.Encode(&runningConfig)
	return buffer.String(), err
}

func GetIdleWipFlushIntervalSecs() int {
	return runningConfig.IdleWipFlushIntervalSecs
}

func GetMaxWaitWipFlushIntervalSecs() int {
	return runningConfig.MaxWaitWipFlushIntervalSecs
}

func GetTimeStampKey() string {
	return runningConfig.TimeStampKey
}

func GetS3IngestQueueName() string {
	return runningConfig.S3IngestQueueName
}

func GetS3IngestQueueRegion() string {
	return runningConfig.S3IngestQueueRegion
}
func GetS3IngestBufferSize() uint64 {
	return runningConfig.S3IngestBufferSize
}
func GetMaxParallelS3IngestBuffers() uint64 {
	return runningConfig.MaxParallelS3IngestBuffers
}

func IsNewQueryPipelineEnabled() bool {
	// TODO: when we fully switch to the new pipeline, we can delete this function.
	return runningConfig.UseNewPipelineConverted
}

func SetNewQueryPipelineEnabled(enabled bool) {
	// TODO: when we fully switch to the new pipeline, we can delete this function.
	runningConfig.UseNewPipelineConverted = enabled
}

func IsLowMemoryModeEnabled() bool {
	return runningConfig.MemoryConfig.LowMemoryMode.Value()
}

// returns a map of s3 config
func GetS3ConfigMap() map[string]interface{} {
	data, err := json.Marshal(runningConfig.S3)
	if err != nil {
		return map[string]interface{}{}
	}

	var newMap map[string]interface{}
	err = json.Unmarshal(data, &newMap)
	if err != nil {
		return map[string]interface{}{}
	}
	return newMap
}

func getServerYamlConfig() common.RunModConfig {
	return common.RunModConfig{
		QueryTimeoutSecs: runningConfig.QueryTimeoutSecs,
		PQSEnabled:       runningConfig.PQSEnabledConverted,
	}
}

func IsIngestNode() bool {
	retVal, err := strconv.ParseBool(runningConfig.IngestNode)
	if err != nil {
		log.Errorf("IsIngestNode: Error parsing ingest node: [%v], Err: [%+v]. Defaulting to true", runningConfig.IngestNode, err)
		return true
	}
	return retVal
}

func IsQueryNode() bool {
	retVal, err := strconv.ParseBool(runningConfig.QueryNode)
	if err != nil {
		log.Errorf("IsIngestNode: Error parsing query node: [%v], Err: [%+v]. Defaulting to true", runningConfig.QueryNode, err)
		return true
	}
	return retVal
}

func SetIdleWipFlushIntervalSecs(val int) {
	if val < idleWipFlushRange.Min {
		log.Errorf("SetIdleWipFlushIntervalSecs: IdleWipFlushIntervalSecs should not be less than %vs", idleWipFlushRange.Min)
		log.Infof("SetIdleWipFlushIntervalSecs: Setting IdleWipFlushIntervalSecs to the min allowed: %vs", idleWipFlushRange.Min)
		val = idleWipFlushRange.Min
	}
	if val > idleWipFlushRange.Max {
		log.Warnf("SetIdleWipFlushIntervalSecs: IdleWipFlushIntervalSecs cannot be more than %vs. Defaulting to max allowed: %vs", idleWipFlushRange.Max, idleWipFlushRange.Max)
		val = idleWipFlushRange.Max
	}
	runningConfig.IdleWipFlushIntervalSecs = val
}

func SetRetention(val int) {
	runningConfig.RetentionHours = val
}

func SetTimeStampKey(val string) {
	runningConfig.TimeStampKey = val
}

func SetMaxSegFileSize(size uint64) {
	runningConfig.MaxSegFileSize = size
}

func SetRunningConfig(dir string) {
	runningConfig.DataPath = dir
}

func SetESVersion(val string) {
	runningConfig.ESVersion = val
}

func SetSSInstanceName(val string) {
	runningConfig.SSInstanceName = val
}

func SetDebugMode(log bool) {
	runningConfig.Debug = log
}

func SetDataPath(path string) {
	runningConfig.DataPath = path
}

func SetDataDiskThresholdPercent(percent uint64) {
	runningConfig.DataDiskThresholdPercent = percent
}

func SetMaxParallelS3IngestBuffers(maxBuf uint64) {
	runningConfig.MaxParallelS3IngestBuffers = maxBuf
}
func SetPQSEnabled(enabled bool) {
	runningConfig.PQSEnabledConverted = enabled
	runningConfig.PQSEnabled = strconv.FormatBool(enabled)
}

func SetQueryPort(value uint64) {
	runningConfig.QueryPort = value
}

func GetQueryTimeoutSecs() int {
	timeout := runningConfig.QueryTimeoutSecs
	if timeout <= 0 {
		log.Warnf("GetQueryTimeoutSecs: Invalid timeout %d, using default %d", timeout, DEFAULT_TIMEOUT_SECONDS)
		return DEFAULT_TIMEOUT_SECONDS
	}
	return timeout
}

func GetDefaultRunModConfig() common.RunModConfig {
	return common.RunModConfig{
		PQSEnabled:       true,
		QueryTimeoutSecs: DEFAULT_TIMEOUT_SECONDS,
	}
}

func SetQueryTimeoutSecs(timeout int) {
	runningConfig.QueryTimeoutSecs = timeout
}

func ValidateDeployment() (common.DeploymentType, error) {
	if IsQueryNode() && IsIngestNode() {
		if runningConfig.S3.Enabled {
			return common.SingleNodeS3, nil
		}
		return common.SingleNode, nil
	}
	return 0, fmt.Errorf("ValidateDeployment: single node deployment must have both query and ingest in the same node")
}

func WriteToYamlConfig() {
	setValues, err := yaml.Marshal(&runningConfig)
	if err != nil {
		log.Errorf("WriteToYamlConfig: error converting to yaml: %v", err)
	}
	err = os.WriteFile(configFilePath, setValues, 0644)
	if err != nil {
		log.Errorf("WriteToYamlConfig: error writing to yaml configFilePath: %v, err: %v", configFilePath, err)
	}
}

// InitConfigurationData is in charge to init the various Configuration data.
// It runs only once to instantiate Configuration options.
// If an error is encountered, the configuration was unable to be read, so siglens should properly exit to avoid startup with wrong configurations
func InitConfigurationData() error {
	log.Trace("Initdatastructure.ConfigurationData | START")
	configFilePath = ExtractCmdLineInput() // Function for validate command line INPUT
	log.Trace("Initdatastructure.ConfigurationData | STOP")
	config, err := ReadConfigFile(configFilePath)
	if err != nil {
		return err
	}
	runningConfig = config
	var readConfig common.RunModConfig
	readConfig, err = ReadRunModConfig(RunModFilePath)
	if err != nil && !os.IsNotExist(err) {
		log.Errorf("InitConfigurationData: Failed to read runmod config err: %v, config: %+v", err, readConfig)
	}
	fileInfo, err := os.Stat(configFilePath)
	if err != nil {
		log.Errorf("InitConfigurationData: Cannot stat config file while re-reading, configFilePath: %v, err: %v", configFilePath, err)
		return err
	}
	configFileLastModified = uint64(fileInfo.ModTime().UTC().Unix())
	go runRefreshConfigLoop()
	return nil
}

/*
Use only for testing purpose, DO NOT use externally
*/
func InitializeDefaultConfig(dataPath string) {
	if !strings.HasSuffix(dataPath, "/") {
		dataPath += "/"
	}

	runningConfig = GetTestConfig(dataPath)
	_ = InitDerivedConfig("test-uuid") // This is only used for testing
}

// To do - Currently we are assigning default value two times.. in InitializeDefaultConfig() for testing and
// ExtractConfigData(). Do this in one time.
func GetTestConfig(dataPath string) common.Configuration {
	// *************************************
	// THIS IS ONLY USED in TESTS, MAKE SURE:
	// 1. set the defaults ExtractConfigData
	// 2. set the defaults in server.yaml
	// 3. And Here.
	// ************************************

	testConfig := common.Configuration{
		IngestListenIP:              "[::]",
		QueryListenIP:               "[::]",
		IngestPort:                  8081,
		QueryPort:                   5122,
		IngestUrl:                   "",
		QueryNode:                   "true",
		IngestNode:                  "true",
		IdleWipFlushIntervalSecs:    5,
		MaxWaitWipFlushIntervalSecs: 30,
		DataPath:                    dataPath,
		S3:                          common.S3Config{Enabled: false, BucketName: "", BucketPrefix: "", RegionName: ""},
		RetentionHours:              24 * 90,
		TimeStampKey:                "timestamp",
		MaxSegFileSize:              4_294_967_296,
		LicenseKeyPath:              "./",
		ESVersion:                   "",
		Debug:                       false,
		PProfEnabled:                "true",
		PProfEnabledConverted:       true,
		DataDiskThresholdPercent:    85,
		S3IngestQueueName:           "",
		S3IngestQueueRegion:         "",
		S3IngestBufferSize:          1000,
		MaxParallelS3IngestBuffers:  10,
		SSInstanceName:              "",
		PQSEnabled:                  "false",
		PQSEnabledConverted:         false,
		SafeServerStart:             false,
		AnalyticsEnabled:            "false",
		AnalyticsEnabledConverted:   false,
		AgileAggsEnabled:            "true",
		AgileAggsEnabledConverted:   true,
		DualCaseCheck:               "false",
		DualCaseCheckConverted:      false,
		QueryHostname:               "",
		Log:                         common.LogConfig{LogPrefix: "", LogFileRotationSizeMB: 100, CompressLogFile: false},
		TLS:                         common.TLSConfig{Enabled: false, CertificatePath: "", PrivateKeyPath: ""},
		CompressStatic:              "false",
		CompressStaticConverted:     false,
		Tracing:                     common.TracingConfig{ServiceName: "", Endpoint: "", SamplingPercentage: 1},
		DatabaseConfig:              common.DatabaseConfig{Enabled: true, Provider: "sqlite"},
		EmailConfig:                 common.EmailConfig{SmtpHost: "smtp.gmail.com", SmtpPort: 587, SenderEmail: "doe1024john@gmail.com", GmailAppPassword: " "},
		MemoryConfig: common.MemoryConfig{
			MaxUsagePercent: 80,
			SearchPercent:   DEFAULT_SEG_SEARCH_MEM_PERCENT,
			CMIPercent:      DEFAULT_ROTATED_CMI_MEM_PERCENT,
			MetadataPercent: DEFAULT_METADATA_MEM_PERCENT,
			MetricsPercent:  DEFAULT_METRICS_MEM_PERCENT,
			BytesPerQuery:   DEFAULT_BYTES_PER_QUERY,
		},
		MaxAllowedColumns: DEFAULT_MAX_ALLOWED_COLUMNS,
	}

	return testConfig
}

func InitializeTestingConfig(dataPath string) {
	InitializeDefaultConfig(dataPath)
	SetDebugMode(false)
}

func ReadRunModConfig(fileName string) (common.RunModConfig, error) {
	_, err := os.Stat(fileName)
	if os.IsNotExist(err) {
		log.Infof("ReadRunModConfig: Config file '%s' does not exist. Awaiting user action to create it.", fileName)
		return common.RunModConfig{}, err
	} else if err != nil {
		log.Errorf("ReadRunModConfig: Error accessing config file: '%s', err: %v", fileName, err)
		return common.RunModConfig{}, err
	}

	jsonData, err := os.ReadFile(fileName)
	if err != nil {
		log.Errorf("ReadRunModConfig: Cannot read input file: %v, err: %v", fileName, err)
	}
	return ExtractReadRunModConfig(jsonData)
}

func ExtractReadRunModConfig(jsonData []byte) (common.RunModConfig, error) {
	var runModConfig common.RunModConfig
	// If runmod.cfg is empty, use server.yaml values
	if len(strings.TrimSpace(string(jsonData))) == 0 {
		log.Infof("ExtractReadRunModConfig: Empty or no runmod config, using server.yaml values")
		return getServerYamlConfig(), nil
	}
	err := json.Unmarshal(jsonData, &runModConfig)
	if err != nil {
		log.Errorf("ExtractReadRunModConfig: Failed to parse runmod.cfg data: %v, err: %v", string(jsonData), err)
		return runModConfig, err
	}

	validateAndApplyConfig(&runModConfig)
	return runModConfig, nil
}

func validateAndApplyConfig(config *common.RunModConfig) {
	reqData := make(map[string]interface{})
	jsonData, _ := json.Marshal(config)
	_ = json.Unmarshal(jsonData, &reqData)
	// Check if fields exist in runmod json
	if _, exists := reqData["queryTimeoutSecs"]; !exists {
		if runningConfig.QueryTimeoutSecs > DEFAULT_TIMEOUT_SECONDS {
			config.QueryTimeoutSecs = runningConfig.QueryTimeoutSecs
		} else {
			config.QueryTimeoutSecs = DEFAULT_TIMEOUT_SECONDS
		}
	}

	if _, exists := reqData["pqsEnabled"]; !exists {
		config.PQSEnabled = runningConfig.PQSEnabledConverted
	}

	SetPQSEnabled(config.PQSEnabled)
	SetQueryTimeoutSecs(config.QueryTimeoutSecs)
}

func ReadConfigFile(fileName string) (common.Configuration, error) {
	yamlData, err := os.ReadFile(fileName)
	if err != nil {
		log.Errorf("ReadConfigFile: Cannot read input file: %v, err: %v", fileName, err)
	}

	if hook := hooks.GlobalHooks.ExtractConfigHook; hook != nil {
		return hook(yamlData)
	} else {
		return ExtractConfigData(yamlData)
	}
}

func ExtractConfigData(yamlData []byte) (common.Configuration, error) {
	config := common.Configuration{
		MemoryConfig: common.MemoryConfig{
			LowMemoryMode: utils.DefaultValue(false),
		},
		TLS: common.TLSConfig{
			MtlsEnabled: utils.DefaultValue(false),
		},
	}
	err := yaml.Unmarshal(yamlData, &config)
	if err != nil {
		log.Errorf("ExtractConfigData: Error parsing yaml data: %v, err: %v", string(yamlData), err)
		return config, err
	}

	if len(config.IngestListenIP) <= 0 {
		config.IngestListenIP = "[::]"
	}
	if len(config.QueryListenIP) <= 0 {
		config.QueryListenIP = "[::]"
	}

	if config.IngestPort <= 0 {
		config.IngestPort = 8081
	}

	if config.QueryPort <= 0 {
		config.QueryPort = 5122
	}

	if config.IdleWipFlushIntervalSecs <= 0 {
		config.IdleWipFlushIntervalSecs = idleWipFlushRange.Default
	}
	if config.IdleWipFlushIntervalSecs < idleWipFlushRange.Min {
		log.Warnf("ExtractConfigData: IdleWipFlushIntervalSecs should not be less than %v seconds. Defaulting to min allowed: %v seconds", idleWipFlushRange.Min, idleWipFlushRange.Min)
		config.IdleWipFlushIntervalSecs = idleWipFlushRange.Min
	}
	if config.IdleWipFlushIntervalSecs > idleWipFlushRange.Max {
		log.Warnf("ExtractConfigData: IdleWipFlushIntervalSecs cannot be more than %v seconds. Defaulting to max allowed: %v seconds", idleWipFlushRange.Max, idleWipFlushRange.Max)
		config.IdleWipFlushIntervalSecs = idleWipFlushRange.Max
	}
	if config.MaxWaitWipFlushIntervalSecs <= 0 {
		config.MaxWaitWipFlushIntervalSecs = maxWaitWipFlushRange.Default
	}
	if config.MaxWaitWipFlushIntervalSecs < maxWaitWipFlushRange.Min {
		log.Warnf("ExtractConfigData: MaxWaitWipFlushIntervalSecs should not be less than %v seconds. Defaulting to min allowed: %v seconds", maxWaitWipFlushRange.Min, maxWaitWipFlushRange.Min)
		config.MaxWaitWipFlushIntervalSecs = maxWaitWipFlushRange.Min
	}
	if config.MaxWaitWipFlushIntervalSecs > maxWaitWipFlushRange.Max {
		log.Warnf("ExtractConfigData: MaxWaitWipFlushIntervalSecs cannot be more than %v seconds. Defaulting to max allowed: %v seconds", maxWaitWipFlushRange.Max, maxWaitWipFlushRange.Max)
		config.MaxWaitWipFlushIntervalSecs = maxWaitWipFlushRange.Max
	}
	if config.IdleWipFlushIntervalSecs > config.MaxWaitWipFlushIntervalSecs {
		log.Warnf("ExtractConfigData: IdleWipFlushIntervalSecs cannot be more than MaxWaitWipFlushIntervalSecs. Setting IdleWipFlushIntervalSecs to MaxWaitWipFlushIntervalSecs")
		config.IdleWipFlushIntervalSecs = config.MaxWaitWipFlushIntervalSecs
	}
	if len(config.Log.LogPrefix) <= 0 {
		config.Log.LogPrefix = ""
	}

	if len(config.QueryNode) <= 0 {
		config.QueryNode = "true"
	}

	if len(config.IngestNode) <= 0 {
		config.IngestNode = "true"
	}

	if len(config.PProfEnabled) <= 0 {
		config.PProfEnabled = "true"
	}
	pprofEnabled, err := strconv.ParseBool(config.PProfEnabled)
	if err != nil {
		log.Errorf("ExtractConfigData: failed to parse pprof enabled flag. Defaulting to true. Error: %v", err)
		pprofEnabled = true
		config.PProfEnabled = "true"
	}
	config.PProfEnabledConverted = pprofEnabled

	if len(config.PQSEnabled) <= 0 {
		config.PQSEnabled = "true"
	}
	pqsEnabled, err := strconv.ParseBool(config.PQSEnabled)
	if err != nil {
		log.Errorf("ExtractConfigData: failed to parse PQS enabled flag. Defaulting to false. Error: %v", err)
		pqsEnabled = true
		config.PQSEnabled = "true"
	}
	config.PQSEnabledConverted = pqsEnabled

	if len(config.AnalyticsEnabled) <= 0 {
		config.AnalyticsEnabled = "true"
	}
	analyticsEnabled, err := strconv.ParseBool(config.AnalyticsEnabled)
	if err != nil {
		log.Errorf("ExtractConfigData: failed to parse analytics enabled flag. Defaulting to true. Error: %v", err)
		analyticsEnabled = true
		config.AnalyticsEnabled = "true"
	}
	config.AnalyticsEnabledConverted = analyticsEnabled

	if len(config.AgileAggsEnabled) <= 0 {
		config.AgileAggsEnabled = "true"
	}
	AgileAggsEnabled, err := strconv.ParseBool(config.AgileAggsEnabled)
	if err != nil {
		log.Errorf("ExtractConfigData: failed to parse AgileAggs enabled flag. Defaulting to true. Error: %v", err)
		AgileAggsEnabled = true
		config.AgileAggsEnabled = "true"
	}
	config.AgileAggsEnabledConverted = AgileAggsEnabled

	if len(config.DualCaseCheck) <= 0 {
		config.DualCaseCheck = "true"
	}
	dualCaseCheck, err := strconv.ParseBool(config.DualCaseCheck)
	if err != nil {
		log.Errorf("ExtractConfigData: failed to parse DualCaseCheck flag. Defaulting to true. Error: %v", err)
		dualCaseCheck = true
		config.DualCaseCheck = "true"
	}
	config.DualCaseCheckConverted = dualCaseCheck

	if len(config.UseNewQueryPipeline) <= 0 {
		config.UseNewQueryPipeline = "true"
	}
	useNewPipeline, err := strconv.ParseBool(config.UseNewQueryPipeline)
	if err != nil {
		log.Errorf("ExtractConfigData: failed to parse UseNewQueryPipeline flag. Defaulting to true. Error: %v", err)
		useNewPipeline = true
		config.UseNewQueryPipeline = "true"
	}
	config.UseNewPipelineConverted = useNewPipeline

	if len(config.DataPath) <= 0 {
		config.DataPath = "data/"
	}

	if len(config.IngestUrl) <= 0 {
		config.IngestUrl = "http://localhost:" + strconv.FormatUint(config.IngestPort, 10)
	}

	if !config.S3.Enabled {
		config.S3.Enabled = false
	}

	if len(config.S3.BucketName) <= 0 {
		config.S3.BucketName = ""
	}
	if len(config.S3.RegionName) <= 0 {
		config.S3.RegionName = ""
	}
	if len(config.S3.BucketPrefix) <= 0 {
		config.S3.BucketPrefix = ""
	} else {
		if config.S3.BucketPrefix[len(config.S3.BucketPrefix)-1:] != "/" {
			config.S3.BucketPrefix = config.S3.BucketPrefix + "/"
		}

	}

	if config.RetentionHours == 0 || config.RetentionHours > 15*24 {
		log.Infof("ExtractConfigData: Setting to 360hrs (15 days) of retention as default...")
		config.RetentionHours = 15 * 24
	}
	if len(config.TimeStampKey) <= 0 {
		config.TimeStampKey = "timestamp"
	}
	if len(config.LicenseKeyPath) <= 0 {
		config.LicenseKeyPath = "./"
	}
	if config.MaxSegFileSize <= 0 {
		config.MaxSegFileSize = 4_294_967_296
	}
	if len(config.ESVersion) <= 0 {
		config.ESVersion = "6.8.20"
	}
	if strings.HasPrefix(config.DataPath, "./") {
		config.DataPath = strings.Trim(config.DataPath, "./")
	}
	if config.DataPath[len(config.DataPath)-1] != '/' {
		config.DataPath += "data/"
	}
	if config.Log.LogFileRotationSizeMB == 0 {
		config.Log.LogFileRotationSizeMB = 100
	}

	if config.DataDiskThresholdPercent == 0 {
		config.DataDiskThresholdPercent = DEFAULT_DISK_THRESHOLD_PERCENT
	}

	memoryLimits := config.MemoryConfig
	totalMemory := memory.TotalMemory()

	if memoryLimits.MaxMemoryAllowedToUseInBytes > totalMemory {
		log.Warnf("ExtractConfigData: MaxMemoryAllowedToUseInBytes is set to %v, greater than host memory: %v. setting it to host memory",
			memoryLimits.MaxMemoryAllowedToUseInBytes, totalMemory)
		memoryLimits.MaxMemoryAllowedToUseInBytes = totalMemory
	}

	if segutils.ConvertUintBytesToMB(totalMemory) < SIZE_8GB_IN_MB {
		if memoryLimits.MaxUsagePercent > 50 {
			log.Infof("ExtractConfigData: MaxUsagePercent is set to %v%% but bringing it down to 50%%", memoryLimits.MaxUsagePercent)
			memoryLimits.MaxUsagePercent = 50
		} else if memoryLimits.MaxUsagePercent == 0 {
			memoryLimits.MaxUsagePercent = 50
		}
	}

	if memoryLimits.MaxUsagePercent == 0 {
		memoryLimits.MaxUsagePercent = 80
	}
	if memoryLimits.SearchPercent == 0 {
		memoryLimits.SearchPercent = DEFAULT_SEG_SEARCH_MEM_PERCENT
	}
	if memoryLimits.CMIPercent == 0 {
		memoryLimits.CMIPercent = DEFAULT_ROTATED_CMI_MEM_PERCENT
	}
	if memoryLimits.MetadataPercent == 0 {
		memoryLimits.MetadataPercent = DEFAULT_METADATA_MEM_PERCENT
	}
	if memoryLimits.BytesPerQuery == 0 {
		memoryLimits.BytesPerQuery = DEFAULT_BYTES_PER_QUERY
	}
	total := memoryLimits.SearchPercent + memoryLimits.CMIPercent + memoryLimits.MetadataPercent
	if memoryLimits.MetricsPercent == 0 && total < 100 {
		memoryLimits.MetricsPercent = DEFAULT_METRICS_MEM_PERCENT
	}
	total += memoryLimits.MetricsPercent

	if total != 100 {
		err := fmt.Errorf("ExtractConfigData: Memory splits sum to %v!=100%%. Search: %v, CMI: %v, Metadata: %v, Metrics: %v",
			total, memoryLimits.SearchPercent, memoryLimits.CMIPercent, memoryLimits.MetadataPercent,
			memoryLimits.MetricsPercent)

		log.Error(err)
		return config, err
	}

	config.MemoryConfig = memoryLimits

	if config.MaxAllowedColumns == 0 {
		config.MaxAllowedColumns = DEFAULT_MAX_ALLOWED_COLUMNS
	}

	if len(config.S3IngestQueueName) <= 0 {
		config.S3IngestQueueName = ""
	}
	if len(config.S3IngestQueueRegion) <= 0 {
		config.S3IngestQueueRegion = ""
	}

	if config.MaxParallelS3IngestBuffers == 0 {
		config.MaxParallelS3IngestBuffers = 10
	}

	if config.S3IngestBufferSize == 0 {
		config.S3IngestBufferSize = 1000
	}

	if config.QueryHostname == "" {
		config.QueryHostname = fmt.Sprintf("localhost:%v", config.QueryPort)
	}

	if len(config.TLS.CertificatePath) >= 0 && strings.HasPrefix(config.TLS.CertificatePath, "./") {
		config.TLS.CertificatePath = strings.Trim(config.TLS.CertificatePath, "./")
	}

	if len(config.TLS.PrivateKeyPath) >= 0 && strings.HasPrefix(config.TLS.PrivateKeyPath, "./") {
		config.TLS.PrivateKeyPath = strings.Trim(config.TLS.PrivateKeyPath, "./")
	}

	if len(config.CompressStatic) <= 0 {
		config.CompressStatic = "true"
	}
	compressStatic, err := strconv.ParseBool(config.CompressStatic)
	if err != nil {
		compressStatic = true
		config.CompressStatic = "true"
		log.Errorf("ExtractConfigData: failed to parse compress static flag. Defaulting to %v. Error: %v",
			compressStatic, err)
	}
	config.CompressStaticConverted = compressStatic

	// Check for Tracing Config through environment variables
	if os.Getenv("TRACESTORE_ENDPOINT") != "" {
		config.Tracing.Endpoint = os.Getenv("TRACESTORE_ENDPOINT")
	}

	if os.Getenv("SIGLENS_TRACING_SERVICE_NAME") != "" {
		config.Tracing.ServiceName = os.Getenv("SIGLENS_TRACING_SERVICE_NAME")
	}

	if os.Getenv("TRACE_SAMPLING_PERCENTAGE") != "" {
		samplingPercentage, err := strconv.ParseFloat(os.Getenv("TRACE_SAMPLING_PERCENTAGE"), 64)
		if err != nil {
			log.Errorf("ExtractConfigData: Error parsing TRACE_SAMPLING_PERCENTAGE err: %v", err)
			log.Info("ExtractConfigData: Setting Trace Sampling Percentage to 1")
			config.Tracing.SamplingPercentage = 1
		} else {
			config.Tracing.SamplingPercentage = samplingPercentage
		}
	}

	if len(config.Tracing.ServiceName) <= 0 {
		config.Tracing.ServiceName = "siglens"
	}

	if len(config.Tracing.Endpoint) <= 0 {
		log.Info("ExtractConfigData: Tracing is disabled. Please set the endpoint in the config file to enable Tracing.")
		SetTracingEnabled(false)
	} else {
		log.Info("ExtractConfigData: Tracing is enabled. Tracing Endpoint: ", config.Tracing.Endpoint)
		SetTracingEnabled(true)
	}

	if config.Tracing.SamplingPercentage < 0 {
		config.Tracing.SamplingPercentage = 0
	} else if config.Tracing.SamplingPercentage > 100 {
		config.Tracing.SamplingPercentage = 100
	}
	if config.QueryTimeoutSecs <= 0 {
		config.QueryTimeoutSecs = DEFAULT_TIMEOUT_SECONDS
	}
	if config.QueryTimeoutSecs < MIN_QUERY_TIMEOUT_SECONDS {
		log.Errorf("ExtractConfigData: Query timeout cannot be less than 1 minute.")
		log.Info("ExtractConfigData: Setting Query timeout to 1 minute")
		config.QueryTimeoutSecs = MIN_QUERY_TIMEOUT_SECONDS
	} else if config.QueryTimeoutSecs > MAX_QUERY_TIMEOUT_SECONDS {
		log.Errorf("ExtractConfigData: Query timeout cannot exceed 30 minutes.")
		log.Info("ExtractConfigData: Setting Query timeout to 30 minutes")
		config.QueryTimeoutSecs = MAX_QUERY_TIMEOUT_SECONDS
	}
	return config, nil
}

func SetConfig(config common.Configuration) {
	runningConfig = config
}

func ExtractCmdLineInput() string {
	log.Trace("VerifyCommandLineInput | START")
	configFile := flag.String("config", "server.yaml", "Path to config file")

	flag.Parse()
	log.Infof("ExtractCmdLineInput: Extracting config from configFile: %v", *configFile)
	log.Trace("VerifyCommandLineInput | STOP")
	return *configFile
}

// WebConfig configuration for fasthttp, copy from fasthttp
type WebConfig struct {
	// Server name for sending in response headers.
	//
	// Default server name is used if left blank.
	Name string

	// The maximum number of concurrent connections the server may serve.
	//
	// DefaultConcurrency is used if not set.
	Concurrency int

	// Whether to disable keep-alive connections.
	//
	// The server will close all the incoming connections after sending
	// the first response to client if this option is set to true.
	//
	// By default keep-alive connections are enabled.
	DisableKeepalive bool

	// Per-connection buffer size for requests' reading.
	// This also limits the maximum header size.
	//
	// Increase this buffer if your clients send multi-KB RequestURIs
	// and/or multi-KB headers (for example, BIG cookies).
	//
	// Default buffer size is used if not set.
	ReadBufferSize int

	// Per-connection buffer size for responses' writing.
	//
	// Default buffer size is used if not set.
	WriteBufferSize int

	// ReadTimeout is the amount of time allowed to read
	// the full request including body. The connection's read
	// deadline is reset when the connection opens, or for
	// keep-alive connections after the first byte has been read.
	//
	// By default request read timeout is unlimited.
	ReadTimeout time.Duration

	// WriteTimeout is the maximum duration before timing out
	// writes of the response. It is reset after the request handler
	// has returned.
	//
	// By default response write timeout is unlimited.
	WriteTimeout time.Duration

	// IdleTimeout is the maximum amount of time to wait for the
	// next request when keep-alive is enabled. If IdleTimeout
	// is zero, the value of ReadTimeout is used.
	IdleTimeout time.Duration

	// Maximum number of concurrent client connections allowed per IP.
	//
	// By default unlimited number of concurrent connections
	// may be established to the server from a single IP address.
	MaxConnsPerIP int

	// Maximum number of requests served per connection.
	//
	// The server closes connection after the last request.
	// 'Connection: close' header is added to the last response.
	//
	// By default unlimited number of requests may be served per connection.
	MaxRequestsPerConn int

	// MaxKeepaliveDuration is a no-op and only left here for backwards compatibility.
	// Deprecated: Use IdleTimeout instead.
	MaxKeepaliveDuration time.Duration

	// Whether to enable tcp keep-alive connections.
	//
	// Whether the operating system should send tcp keep-alive messages on the tcp connection.
	//
	// By default tcp keep-alive connections are disabled.
	TCPKeepalive bool

	// Period between tcp keep-alive messages.
	//
	// TCP keep-alive period is determined by operating system by default.
	TCPKeepalivePeriod time.Duration

	// Maximum request body size.
	//
	// The server rejects requests with bodies exceeding this limit.
	//
	// Request body size is limited by DefaultMaxRequestBodySize by default.
	MaxRequestBodySize int

	// Aggressively reduces memory usage at the cost of higher CPU usage
	// if set to true.
	//
	// Try enabling this option only if the server consumes too much memory
	// serving mostly idle keep-alive connections. This may reduce memory
	// usage by more than 50%.
	//
	// Aggressive memory usage reduction is disabled by default.
	ReduceMemoryUsage bool

	// Rejects all non-GET requests if set to true.
	//
	// This option is useful as anti-DoS protection for servers
	// accepting only GET requests. The request size is limited
	// by ReadBufferSize if GetOnly is set.
	//
	// Server accepts all the requests by default.
	GetOnly bool

	// Logs all errors, including the most frequent
	// 'connection reset by peer', 'broken pipe' and 'connection timeout'
	// errors. Such errors are common in production serving real-world
	// clients.
	//
	// By default the most frequent errors such as
	// 'connection reset by peer', 'broken pipe' and 'connection timeout'
	// are suppressed in order to limit output log traffic.
	LogAllErrors bool

	// Header names are passed as-is without normalization
	// if this option is set.
	//
	// Disabled header names' normalization may be useful only for proxying
	// incoming requests to other servers expecting case-sensitive
	// header names. See https://github.com/valyala/fasthttp/issues/57
	// for details.
	//
	// By default, request and response header names are normalized, i.e.
	// The first letter and the first letters following dashes
	// are uppercase, while all the other letters are lowercase.
	// Examples:
	//
	//     * HOST -> Host
	//     * content-type -> Content-Type
	//     * cONTENT-lenGTH -> Content-Length
	DisableHeaderNamesNormalizing bool

	// SleepWhenConcurrencyLimitsExceeded is a duration to be slept of if
	// the concurrency limit in exceeded (default [when is 0]: don't sleep
	// and accept new connections immediately).
	SleepWhenConcurrencyLimitsExceeded time.Duration

	// NoDefaultServerHeader, when set to true, causes the default Server header
	// to be excluded from the Response.
	//
	// The default Server header value is the value of the Name field or an
	// internal default value in its absence. With this option set to true,
	// the only time a Server header will be sent is if a non-zero length
	// value is explicitly provided during a request.
	NoDefaultServerHeader bool

	// NoDefaultContentType, when set to true, causes the default Content-Type
	// header to be excluded from the Response.
	//
	// The default Content-Type header value is the internal default value. When
	// set to true, the Content-Type will not be present.
	NoDefaultContentType bool

	// KeepHijackedConns is an opt-in disable of connection
	// close by fasthttp after connections' HijackHandler returns.
	// This allows to save goroutines, e.g. when fasthttp used to upgrade
	// http connections to WS and connection goes to another handler,
	// which will close it when needed.
	KeepHijackedConns bool
}

const (
	ServerName         = "SigLens"
	ReadBufferSize     = 4096
	MaxConnsPerIP      = 3000
	MaxRequestsPerConn = 1000
	MaxRequestBodySize = 512 * 1000 * 1000
	Concurrency        = 3000
)

// DefaultIngestServerHttpConfig   set fasthttp server default configuration
func DefaultIngestServerHttpConfig() WebConfig {
	return WebConfig{
		Name:               ServerName,
		ReadBufferSize:     ReadBufferSize,
		MaxConnsPerIP:      MaxConnsPerIP,
		MaxRequestsPerConn: MaxRequestsPerConn,
		MaxRequestBodySize: MaxRequestBodySize, //  100 << 20, // 100MB // 1000 * 4, // MaxRequestBodySize:
		Concurrency:        Concurrency,
	}
}

// DefaultUIServerHttpConfig  set fasthttp server default configuration
func DefaultUIServerHttpConfig() WebConfig {
	return WebConfig{
		Name:               fmt.Sprintf("%s-ws", ServerName),
		ReadBufferSize:     ReadBufferSize,
		MaxConnsPerIP:      MaxConnsPerIP,
		MaxRequestsPerConn: MaxRequestsPerConn,
		MaxRequestBodySize: MaxRequestBodySize, //  100 << 20, // 100MB // 1000 * 4, // MaxRequestBodySize:
		Concurrency:        Concurrency,
	}
}

func ProcessGetConfig(ctx *fasthttp.RequestCtx) {
	var httpResp utils.HttpServerResponse
	jsonStr, err := GetRunningConfigAsJsonStr()
	if err == nil {
		ctx.SetStatusCode(fasthttp.StatusOK)
		httpResp.Message = jsonStr
		httpResp.StatusCode = fasthttp.StatusOK
	} else {
		ctx.SetStatusCode(fasthttp.StatusServiceUnavailable)
		httpResp.Message = err.Error()
		httpResp.StatusCode = fasthttp.StatusServiceUnavailable
	}
	utils.WriteResponse(ctx, httpResp)
}

func ProcessGetConfigAsJson(ctx *fasthttp.RequestCtx) {
	ctx.SetStatusCode(fasthttp.StatusOK)
	utils.WriteJsonResponse(ctx, &runningConfig)
}

func ProcessForceReadConfig(ctx *fasthttp.RequestCtx) {
	newConfig, err := ReadConfigFile(configFilePath)
	if err != nil {
		log.Errorf("ProcessForceReadConfig: Error while reading config file, configFilepath: %v, err: %v", configFilePath, err)
		return
	}
	SetConfig(newConfig)
	ctx.SetStatusCode(fasthttp.StatusOK)
	utils.WriteJsonResponse(ctx, &runningConfig)
}

func refreshConfig() {
	fileInfo, err := os.Stat(configFilePath)
	if err != nil {
		log.Errorf("refreshConfig: Cannot stat config file while re-reading, configFilepath: %v, err: %v", configFilePath, err)
		return
	}
	modifiedTime := fileInfo.ModTime()
	modifiedTimeSec := uint64(modifiedTime.UTC().Unix())
	if modifiedTimeSec > configFileLastModified {
		newConfig, err := ReadConfigFile(configFilePath)
		if err != nil {
			log.Errorf("refreshConfig: Error while reading config file, configFilepath: %v, err: %v", configFilePath, err)
			return
		}
		SetConfig(newConfig)

		log.Infof("refreshConfig: cfg  updated, modifiedTimeSec: %v, lastModified: %v",
			modifiedTimeSec, configFileLastModified)
		configJSON, err := json.MarshalIndent(newConfig, "", "  ")
		if err != nil {
			log.Errorf("refreshConfig : Error marshalling config struct %v", err.Error())
		}
		log.Infof("refreshConfig: newConfig: %v", string(configJSON))
		configFileLastModified = modifiedTimeSec
	}
}

func runRefreshConfigLoop() {
	for {
		time.Sleep(MINUTES_REREAD_CONFIG * time.Minute)
		refreshConfig()
	}
}

func GetQueryServerBaseUrl() string {
	hostname := GetQueryHostname()
	if IsTlsEnabled() {
		hostname = "https://" + hostname
	} else {
		hostname = "http://" + hostname
	}
	return hostname
}

func GetSuffixFile(virtualTable string, streamId string) string {
	var sb strings.Builder
	sb.WriteString(GetDataPath())
	sb.WriteString(GetHostID())
	sb.WriteString("/suffix/")
	sb.WriteString(virtualTable)
	sb.WriteString("/")
	sb.WriteString(streamId)
	sb.WriteString(".suffix")
	return sb.String()
}

func GetBaseVTableDir(streamid string, virtualTableName string) string {
	return filepath.Join(GetDataPath(), GetHostID(), "final", virtualTableName, streamid)
}

func GetBaseSegDir(streamid string, virtualTableName string, suffix uint64) string {
	// Note: this is coupled to getSegBaseDirFromFilename. If the directory
	// structure changes, change getSegBaseDirFromFilename too.
	// TODO: use filepath.Join to avoid "/" issues
	var sb strings.Builder
	sb.WriteString(GetDataPath())
	sb.WriteString(GetHostID())
	sb.WriteString("/final/")
	sb.WriteString(virtualTableName + "/")
	sb.WriteString(streamid + "/")
	sb.WriteString(strconv.FormatUint(suffix, 10) + "/")
	basedir := sb.String()
	return basedir
}

func GetSegKey(streamid string, virtualTableName string, suffix uint64) string {
	return fmt.Sprintf("%s%d", GetBaseSegDir(streamid, virtualTableName, suffix), suffix)
}

func GetSegKeyFromVTableDir(virtualTableDir string, suffixStr string) string {
	return filepath.Join(virtualTableDir, suffixStr, suffixStr)
}

// DefaultUIServerHttpConfig  set fasthttp server default configuration
func DefaultQueryServerHttpConfig() WebConfig {
	return WebConfig{
		Name:               fmt.Sprintf("%s-query", ServerName),
		ReadBufferSize:     ReadBufferSize,
		MaxConnsPerIP:      MaxConnsPerIP,
		MaxRequestsPerConn: MaxRequestsPerConn,
		MaxRequestBodySize: MaxRequestBodySize, //  100 << 20, // 100MB // 1000 * 4, // MaxRequestBodySize:
		Concurrency:        Concurrency,
	}
}

func DefaultIngestionHttpConfig() WebConfig {
	return WebConfig{
		Name:               fmt.Sprintf("%s-ingest", ServerName),
		ReadBufferSize:     ReadBufferSize,
		MaxConnsPerIP:      MaxConnsPerIP,
		MaxRequestsPerConn: MaxRequestsPerConn,
		MaxRequestBodySize: MaxRequestBodySize, //  100 << 20, // 100MB // 1000 * 4, // MaxRequestBodySize:
		Concurrency:        Concurrency,
	}
}

func DefaultAddonsServerHttpConfig() WebConfig {
	return WebConfig{
		Name:               fmt.Sprintf("%s-addons", ServerName),
		ReadBufferSize:     ReadBufferSize,
		MaxConnsPerIP:      MaxConnsPerIP,
		MaxRequestsPerConn: MaxRequestsPerConn,
		MaxRequestBodySize: MaxRequestBodySize,
		Concurrency:        Concurrency,
	}
}
