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
	"errors"
	"flag"
	"fmt"
	"os"
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

const MINUTES_REREAD_CONFIG = 15
const RunModFilePath = "data/common/runmod.cfg"

var configFileLastModified uint64

var runningConfig common.Configuration
var configFilePath string

var parallelism int64

var tracingEnabled bool // flag to enable/disable tracing; Set to true if TracingConfig.Endpoint != ""

func init() {
	parallelism = int64(runtime.GOMAXPROCS(0))
	if parallelism <= 1 {
		parallelism = 2
	}
}

func GetTotalMemoryAvailable() uint64 {
	var gogc uint64
	v := os.Getenv("GOGC")
	if v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			log.Error("Error while converting gogc to int")
			n = 100
		}
		gogc = uint64(n)
	} else {
		gogc = 100
	}
	hostMemory := memory.TotalMemory() * runningConfig.MemoryThresholdPercent / 100
	allowedMemory := hostMemory / (1 + gogc/100)
	log.Infof("GOGC: %+v, MemThresholdPerc: %v, HostRAM: %+v MB, RamAllowedToUse: %v MB", gogc,
		runningConfig.MemoryThresholdPercent,
		segutils.ConvertUintBytesToMB(memory.TotalMemory()),
		segutils.ConvertUintBytesToMB(allowedMemory))
	return allowedMemory
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

func GetEventTypeKeywords() *[]string {
	return &runningConfig.EventTypeKeywords
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

func GetMaxSegFileSize() *uint64 {
	return &runningConfig.MaxSegFileSize
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

// returns if tls is enabled
func IsTlsEnabled() bool {
	return runningConfig.TLS.Enabled
}

// returns the configured certificate path
func GetTLSCertificatePath() string {
	return runningConfig.TLS.CertificatePath
}

// returns the configured private key path
func GetTLSPrivateKeyPath() string {
	return runningConfig.TLS.PrivateKeyPath
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
		return hostname
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

func IsPQSEnabled() bool {
	return runningConfig.PQSEnabledConverted
}

func IsAggregationsEnabled() bool {
	return runningConfig.AgileAggsEnabledConverted
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

func GetSegFlushIntervalSecs() int {
	if runningConfig.SegFlushIntervalSecs > 600 {
		log.Errorf("GetSegFlushIntervalSecs:SegFlushIntervalSecs cannot be more than 10 mins")
		runningConfig.SegFlushIntervalSecs = 600
	}
	return runningConfig.SegFlushIntervalSecs
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

func IsIngestNode() bool {
	retVal, err := strconv.ParseBool(runningConfig.IngestNode)
	if err != nil {
		log.Errorf("Error parsing ingest node: [%v] Err: [%+v]. Defaulting to true", runningConfig.IngestNode, err)
		return true
	}
	return retVal
}

func IsQueryNode() bool {
	retVal, err := strconv.ParseBool(runningConfig.QueryNode)
	if err != nil {
		log.Errorf("Error parsing query node: [%v] Err: [%+v]. Defaulting to true", runningConfig.QueryNode, err)
		return true
	}
	return retVal
}

func SetEventTypeKeywords(val []string) {
	runningConfig.EventTypeKeywords = val
}

func SetSegFlushIntervalSecs(val int) {
	if val < 1 {
		log.Errorf("SetSegFlushIntervalSecs : SegFlushIntervalSecs should not be less than 1s")
		log.Infof("SetSegFlushIntervalSecs : Setting SegFlushIntervalSecs to 1 by default")
		val = 1
	}
	runningConfig.SegFlushIntervalSecs = val
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

func ValidateDeployment() (common.DeploymentType, error) {
	if IsQueryNode() && IsIngestNode() {
		if runningConfig.S3.Enabled {
			return common.SingleNodeS3, nil
		}
		return common.SingleNode, nil
	}
	return 0, fmt.Errorf("single node deployment must have both query and ingest in the same node")
}

func WriteToYamlConfig() {
	setValues, err := yaml.Marshal(&runningConfig)
	if err != nil {
		log.Errorf("error converting to yaml: %v", err)
	}
	err = os.WriteFile(configFilePath, setValues, 0644)
	if err != nil {
		log.Errorf("error writing to yaml file: %v", err)
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
		log.Errorf("InitConfigurationData: Failed to read runmod config: %v, config: %+v", err, readConfig)
	}
	fileInfo, err := os.Stat(configFilePath)
	if err != nil {
		log.Errorf("refreshConfig: Cannot stat config file while re-reading, err= %v", err)
		return err
	}
	configFileLastModified = uint64(fileInfo.ModTime().UTC().Unix())
	go runRefreshConfigLoop()
	return nil
}

/*
Use only for testing purpose, DO NOT use externally
*/
func InitializeDefaultConfig() {
	runningConfig = GetTestConfig()
	_ = InitDerivedConfig("test-uuid") // This is only used for testing
}

// To do - Currently we are assigning default value two times.. in InitializeDefaultConfig() for testing and
// ExtractConfigData(). Do this in one time.
func GetTestConfig() common.Configuration {
	// *************************************
	// THIS IS ONLY USED in TESTS, MAKE SURE:
	// 1. set the defaults ExtractConfigData
	// 2. set the defaults in server.yaml
	// 3. And Here.
	// ************************************

	testConfig := common.Configuration{
		IngestListenIP:             "0.0.0.0",
		QueryListenIP:              "0.0.0.0",
		IngestPort:                 8081,
		QueryPort:                  5122,
		IngestUrl:                  "",
		EventTypeKeywords:          []string{"eventType"},
		QueryNode:                  "true",
		IngestNode:                 "true",
		SegFlushIntervalSecs:       5,
		DataPath:                   "data/",
		S3:                         common.S3Config{Enabled: false, BucketName: "", BucketPrefix: "", RegionName: ""},
		RetentionHours:             24 * 90,
		TimeStampKey:               "timestamp",
		MaxSegFileSize:             1_073_741_824,
		LicenseKeyPath:             "./",
		ESVersion:                  "",
		Debug:                      false,
		MemoryThresholdPercent:     80,
		DataDiskThresholdPercent:   85,
		S3IngestQueueName:          "",
		S3IngestQueueRegion:        "",
		S3IngestBufferSize:         1000,
		MaxParallelS3IngestBuffers: 10,
		SSInstanceName:             "",
		PQSEnabled:                 "false",
		PQSEnabledConverted:        false,
		SafeServerStart:            false,
		AnalyticsEnabled:           "false",
		AnalyticsEnabledConverted:  false,
		AgileAggsEnabled:           "true",
		AgileAggsEnabledConverted:  true,
		QueryHostname:              "",
		Log:                        common.LogConfig{LogPrefix: "", LogFileRotationSizeMB: 100, CompressLogFile: false},
		TLS:                        common.TLSConfig{Enabled: false, CertificatePath: "", PrivateKeyPath: ""},
		Tracing:                    common.TracingConfig{ServiceName: "", Endpoint: "", SamplingPercentage: 1},
		DatabaseConfig:             common.DatabaseConfig{Enabled: true, Provider: "sqlite"},
		EmailConfig:                common.EmailConfig{SmtpHost: "smtp.gmail.com", SmtpPort: 587, SenderEmail: "doe1024john@gmail.com", GmailAppPassword: " "},
	}

	return testConfig
}

func InitializeTestingConfig() {
	InitializeDefaultConfig()
	SetDebugMode(true)
	SetDataPath("data/")
}

func ReadRunModConfig(fileName string) (common.RunModConfig, error) {
	_, err := os.Stat(fileName)
	if os.IsNotExist(err) {
		log.Infof("ReadRunModConfig:Config file '%s' does not exist. Awaiting user action to create it.", fileName)
		return common.RunModConfig{}, err
	} else if err != nil {
		log.Errorf("ReadRunModConfig:Error accessing config file '%s': %v", fileName, err)
		return common.RunModConfig{}, err
	}

	jsonData, err := os.ReadFile(fileName)
	if err != nil {
		log.Errorf("ReadRunModConfig:Cannot read input fileName = %v, err=%v", fileName, err)
	}
	return ExtractReadRunModConfig(jsonData)
}

func ExtractReadRunModConfig(jsonData []byte) (common.RunModConfig, error) {
	var runModConfig common.RunModConfig
	err := json.Unmarshal(jsonData, &runModConfig)
	if err != nil {
		log.Errorf("ExtractReadRunModConfig:Failed to parse runmod.cfg: %v", err)
		return runModConfig, err
	}

	SetPQSEnabled(runModConfig.PQSEnabled)
	return runModConfig, nil
}

func ReadConfigFile(fileName string) (common.Configuration, error) {
	yamlData, err := os.ReadFile(fileName)
	if err != nil {
		log.Errorf("Cannot read input fileName = %v, err=%v", fileName, err)
	}

	if hook := hooks.GlobalHooks.ExtractConfigHook; hook != nil {
		return hook(yamlData)
	} else {
		return ExtractConfigData(yamlData)
	}
}

func ExtractConfigData(yamlData []byte) (common.Configuration, error) {
	var config common.Configuration
	err := yaml.Unmarshal(yamlData, &config)
	if err != nil {
		log.Errorf("Error parsing yaml err=%v", err)
		return config, err
	}

	if len(config.IngestListenIP) <= 0 {
		config.IngestListenIP = "0.0.0.0"
	}
	if len(config.QueryListenIP) <= 0 {
		config.QueryListenIP = "0.0.0.0"
	}

	if config.IngestPort <= 0 {
		config.IngestPort = 8081
	}

	if config.QueryPort <= 0 {
		config.QueryPort = 5122
	}

	if len(config.EventTypeKeywords) <= 0 {
		config.EventTypeKeywords = []string{"eventType"}
	}
	if config.SegFlushIntervalSecs <= 0 {
		config.SegFlushIntervalSecs = 5
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

	if len(config.PQSEnabled) <= 0 {
		config.PQSEnabled = "false"
	}
	pqsEnabled, err := strconv.ParseBool(config.PQSEnabled)
	if err != nil {
		log.Errorf("ExtractConfigData: failed to parse PQS enabled flag. Defaulting to false. Error: %v", err)
		pqsEnabled = false
		config.PQSEnabled = "false"
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

	if config.RetentionHours == 0 {
		log.Infof("Defaulting to 2160hrs (90 days) of retention...")
		config.RetentionHours = 90 * 24
	}
	if len(config.TimeStampKey) <= 0 {
		config.TimeStampKey = "timestamp"
	}
	if len(config.LicenseKeyPath) <= 0 {
		config.LicenseKeyPath = "./"
	}
	if config.MaxSegFileSize <= 0 {
		config.MaxSegFileSize = 1_073_741_824
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
		config.DataDiskThresholdPercent = 85
	}
	if config.MemoryThresholdPercent == 0 {
		config.MemoryThresholdPercent = 80
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

	if len(config.TLS.CertificatePath) >= 0 && strings.HasPrefix(config.TLS.CertificatePath, "./") {
		config.TLS.CertificatePath = strings.Trim(config.TLS.CertificatePath, "./")
	}

	if len(config.TLS.PrivateKeyPath) >= 0 && strings.HasPrefix(config.TLS.PrivateKeyPath, "./") {
		config.TLS.PrivateKeyPath = strings.Trim(config.TLS.PrivateKeyPath, "./")
	}

	// Check for Tracing Config through environment variables
	if os.Getenv("TRACESTORE_ENDPOINT") != "" {
		config.Tracing.Endpoint = os.Getenv("TRACESTORE_ENDPOINT")
	}

	if os.Getenv("SIGLENS_TRACING_SERVICE_NAME") != "" {
		config.Tracing.ServiceName = os.Getenv("SIGLENS_TRACING_SERVICE_NAME")
	}

	if os.Getenv("TRACE_SAMPLING_PRECENTAGE") != "" {
		samplingPercentage, err := strconv.ParseFloat(os.Getenv("TRACE_SAMPLING_PRECENTAGE"), 64)
		if err != nil {
			log.Errorf("Error parsing TRACE_SAMPLING_PRECENTAGE: %v", err)
			log.Info("Setting Trace Sampling Percentage to 1")
			config.Tracing.SamplingPercentage = 1
		} else {
			config.Tracing.SamplingPercentage = samplingPercentage
		}
	}

	if len(config.Tracing.ServiceName) <= 0 {
		config.Tracing.ServiceName = "siglens"
	}

	if len(config.Tracing.Endpoint) <= 0 {
		log.Info("Tracing is disabled. Please set the endpoint in the config file to enable Tracing.")
		SetTracingEnabled(false)
	} else {
		log.Info("Tracing is enabled. Tracing Endpoint: ", config.Tracing.Endpoint)
		SetTracingEnabled(true)
	}

	if config.Tracing.SamplingPercentage < 0 {
		config.Tracing.SamplingPercentage = 0
	} else if config.Tracing.SamplingPercentage > 100 {
		config.Tracing.SamplingPercentage = 100
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
	log.Info("Extracting config from configFile: ", *configFile)
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
		log.Errorf("refreshConfig: Cannot stat config file while re-reading, err= %v", err)
		return
	}
	SetConfig(newConfig)
	ctx.SetStatusCode(fasthttp.StatusOK)
	utils.WriteJsonResponse(ctx, &runningConfig)
}

func refreshConfig() {
	fileInfo, err := os.Stat(configFilePath)
	if err != nil {
		log.Errorf("refreshConfig: Cannot stat config file while re-reading, err= %v", err)
		return
	}
	modifiedTime := fileInfo.ModTime()
	modifiedTimeSec := uint64(modifiedTime.UTC().Unix())
	if modifiedTimeSec > configFileLastModified {
		newConfig, err := ReadConfigFile(configFilePath)
		if err != nil {
			log.Errorf("refreshConfig: Cannot stat config file while re-reading, err= %v", err)
			return
		}
		SetConfig(newConfig)
		configFileLastModified = modifiedTimeSec
	}
}

func runRefreshConfigLoop() {
	for {
		time.Sleep(MINUTES_REREAD_CONFIG * time.Minute)
		refreshConfig()
	}
}

func ProcessSetConfig(persistent bool, ctx *fasthttp.RequestCtx) {
	var httpResp utils.HttpServerResponse
	var reqBodyMap map[string]interface{}
	reqBodyStr := ctx.PostBody()
	err := json.Unmarshal([]byte(reqBodyStr), &reqBodyMap)
	if err != nil {
		log.Printf("Error = %v", err)
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		httpResp.Message = "Bad request"
		httpResp.StatusCode = fasthttp.StatusBadRequest
		utils.WriteResponse(ctx, httpResp)
		return
	}
	err = setConfigParams(reqBodyMap)
	if err == nil {
		ctx.SetStatusCode(fasthttp.StatusOK)
		httpResp.Message = "All OK"
		httpResp.StatusCode = fasthttp.StatusOK
		utils.WriteResponse(ctx, httpResp)
		if persistent {
			WriteToYamlConfig()
		}
	} else {
		ctx.SetStatusCode(fasthttp.StatusForbidden)
		httpResp.Message = err.Error()
		httpResp.StatusCode = fasthttp.StatusForbidden
		utils.WriteResponse(ctx, httpResp)
	}
}

func setConfigParams(reqBodyMap map[string]interface{}) error {
	for inputCfgParam := range reqBodyMap {
		if inputCfgParam == "eventTypeKeywords" {
			inputValueParam := reqBodyMap["eventTypeKeywords"]
			evArray, err := extractStrArray(inputValueParam)
			if err != nil {
				return err
			}
			SetEventTypeKeywords(evArray)
		} else {
			err := fmt.Errorf("key = %v not allowed to update", inputCfgParam)
			return err
		}
	}
	return nil
}

func extractStrArray(inputValueParam interface{}) ([]string, error) {
	switch inputValueParam.(type) {
	case []interface{}:
		break
	default:
		err := fmt.Errorf("inputValueParam type = %T not accepted", inputValueParam)
		return nil, err
	}
	evArray := []string{}
	for _, element := range inputValueParam.([]interface{}) {
		switch element := element.(type) {
		case string:
			str := element
			evArray = append(evArray, str)
		default:
			err := fmt.Errorf("element type = %T not accepted", element)
			return nil, err
		}
	}
	return evArray, nil
}

func getQueryServerPort() (uint64, error) {
	if runningConfig.QueryPort == 0 {
		return 0, errors.New("QueryServer Port config was not specified")
	}
	return runningConfig.QueryPort, nil
}

func GetQueryServerBaseUrl() string {
	hostname := GetQueryHostname()
	if hostname == "" {
		port, err := getQueryServerPort()
		if err != nil {
			return "http://localhost:5122"
		}
		return "http://localhost:" + fmt.Sprintf("%d", port)
	} else {
		if IsTlsEnabled() {
			hostname = "https://" + hostname
		} else {
			hostname = "http://" + hostname
		}
		return hostname
	}
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
