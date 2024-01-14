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
	segutils "github.com/siglens/siglens/pkg/segment/utils"
	"github.com/siglens/siglens/pkg/utils"
	log "github.com/sirupsen/logrus"
	"github.com/valyala/fasthttp"
	"gopkg.in/yaml.v3"
)

const MINUTES_REREAD_CONFIG = 15

var configFileLastModified uint64

type DeploymentType uint8

const (
	SingleNode = iota + 1
	SingleNodeS3
	DistributedS3
)

func (d DeploymentType) String() string {
	return [...]string{"INVALID", "SingleNode", "SingleNodeS3", "DistributedS3"}[d]
}

type S3Config struct {
	Enabled      bool   `yaml:"enabled"`
	BucketName   string `yaml:"bucketName"`
	BucketPrefix string `yaml:"bucketPrefix"`
	RegionName   string `yaml:"regionName"`
}

type EtcdConfig struct {
	Enabled  bool     `yaml:"enabled"`
	SeedUrls []string `yaml:"seedUrls"`
}

type EmailConfig struct {
	SmtpHost         string `yaml:"smtpHost"`
	SmtpPort         int    `yaml:"smtpPort"`
	SenderEmail      string `yaml:"senderEmail"`
	GmailAppPassword string `yaml:"gmailAppPassword"`
}

type LogConfig struct {
	LogPrefix             string `yaml:"logPrefix"`             // Prefix of log file. Can be a directory. if empty will log to stdout
	LogFileRotationSizeMB int    `yaml:"logFileRotationSizeMB"` //Max size of log file in megabytes
	CompressLogFile       bool   `yaml:"compressLogFile"`
}

type TLSConfig struct {
	Enabled    bool   `yaml:"enabled"`
	ACMEFolder string `yaml:"acmeFolder"` // folder to store acme certificates
}

type AlertConfig struct {
	Enabled  bool   `yaml:"enabled"`
	Provider string `yaml:"provider"`
	Host     string `yaml:"host"`
	Port     uint64 `yaml:"port"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
	Dbname   string `yaml:"dbname"`
}

type DatabaseConfig struct {
	Enabled  bool   `yaml:"enabled"`
	Provider string `yaml:"provider"`
	Host     string `yaml:"host"`
	Port     uint64 `yaml:"port"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
	Dbname   string `yaml:"dbname"`
}

/*  If you add a new config parameters to the Configuration struct below, make sure to add the default value
assignment in the following functions
1) ExtractConfigData function
2) InitializeDefaultConfig function */

// If you add a new config parameters to the Configuration struct below, make sure to add a descriptive info in server.yaml
type Configuration struct {
	IngestPort                 uint64   `yaml:"ingestPort"`           // Port for ingestion server
	QueryPort                  uint64   `yaml:"queryPort"`            // Port used for query server
	PsqlPort                   uint64   `yaml:"psqlPort"`             // Port used for sql server
	EventTypeKeywords          []string `yaml:"eventTypeKeywords"`    //Required event type keyword
	QueryNode                  string   `yaml:"queryNode"`            //Node to enable/disable all query endpoints
	IngestNode                 string   `yaml:"ingestNode"`           //Node to enable/disable all ingest endpoints
	SegFlushIntervalSecs       int      `yaml:"segFlushIntervalSecs"` // Time Interval after which to write to segfile
	DataPath                   string   `yaml:"dataPath"`
	RetentionHours             int      `yaml:"retentionHours"`
	TimeStampKey               string   `yaml:"timestampKey"`
	MaxSegFileSize             uint64   `yaml:"maxSegFileSize"` // segment file size (in bytes)
	LicenseKeyPath             string   `yaml:"licenseKeyPath"`
	ESVersion                  string   `yaml:"esVersion"`
	Debug                      bool     `yaml:"debug"`                  // debug logging
	MemoryThresholdPercent     uint64   `yaml:"memoryThresholdPercent"` // percent of all available free data allocated for loading micro indices in memory
	DataDiskThresholdPercent   uint64   `yaml:"dataDiskThresholdPercent"`
	GRPCPort                   uint64   `yaml:"grpcPort"` // Address to listen for GRPC connections
	S3IngestQueueName          string   `yaml:"s3IngestQueueName"`
	S3IngestQueueRegion        string   `yaml:"s3IngestQueueRegion"`
	S3IngestBufferSize         uint64   `yaml:"s3IngestBufferSize"`
	MaxParallelS3IngestBuffers uint64   `yaml:"maxParallelS3IngestBuffers"`
	SSInstanceName             string   `yaml:"ssInstanceName"`
	PQSEnabled                 string   `yaml:"pqsEnabled"` // is pqs enabled?
	pqsEnabledConverted        bool     // converted bool value of PQSEnabled yaml
	SafeServerStart            bool     `yaml:"safeMode"`         // if set to true, siglens will start a mock webserver with a custom health handler. Actual server will NOT be started
	AnalyticsEnabled           string   `yaml:"analyticsEnabled"` // is analytics enabled?
	analyticsEnabledConverted  bool
	AgileAggsEnabled           string `yaml:"agileAggsEnabled"` // should we read/write AgileAggsTrees?
	AgileAggsEnabledConverted  bool
	QueryHostname              string         `yaml:"queryHostname"` // hostname of the query server. i.e. if DNS is https://cloud.siglens.com, this should be cloud.siglens.com
	IngestUrl                  string         `yaml:"ingestUrl"`     // full address of the ingest server, including scheme and port, e.g. https://ingest.siglens.com:8080
	S3                         S3Config       `yaml:"s3"`            // s3 related config
	Etcd                       EtcdConfig     `yaml:"etcd"`          // Etcd related config
	Log                        LogConfig      `yaml:"log"`           // Log related config
	TLS                        TLSConfig      `yaml:"tls"`           // TLS related config
	EmailConfig                EmailConfig    `yaml:"emailConfig"`
	DatabaseConfig             DatabaseConfig `yaml:"minionSearch"`
}

var runningConfig Configuration
var configFilePath string

var parallelism int64

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

func GetRunningConfig() *Configuration {
	return &runningConfig
}

// Returns :Port
func GetGRPCPort() string {
	return ":" + strconv.FormatUint(runningConfig.GRPCPort, 10)
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

// returns the psql port
func GetPsqlPort() (uint64, error) {
	if runningConfig.PsqlPort != 0 {
		return runningConfig.PsqlPort, nil
	}
	return 0, errors.New("Psql port not defined in server.yaml")
}

func GetDataPath() string {
	return runningConfig.DataPath
}

// returns the configured acme path
func GetTLSACMEDir() string {
	return runningConfig.TLS.ACMEFolder
}

// returns if tls is enabled
func IsTlsEnabled() bool {
	return runningConfig.TLS.Enabled
}

// used by
func GetQueryHostname() string {
	return runningConfig.QueryHostname
}

func GetEtcdConfig() EtcdConfig {
	return runningConfig.Etcd
}

// returns SmtpHost, SmtpPort, SenderEmail and GmailAppPassword
func GetEmailConfig() (string, int, string, string) {
	return runningConfig.EmailConfig.SmtpHost, runningConfig.EmailConfig.SmtpPort, runningConfig.EmailConfig.SenderEmail, runningConfig.EmailConfig.GmailAppPassword
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
	return runningConfig.pqsEnabledConverted
}

func IsAggregationsEnabled() bool {
	return runningConfig.AgileAggsEnabledConverted
}

func SetAggregationsFlag(enabled bool) {
	runningConfig.AgileAggsEnabledConverted = enabled
	runningConfig.AgileAggsEnabled = strconv.FormatBool(enabled)
}

func IsAnalyticsEnabled() bool {
	return runningConfig.analyticsEnabledConverted
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

func SetCertsPath(path string) {
	runningConfig.TLS.ACMEFolder = path
}

func SetDataDiskThresholdPercent(percent uint64) {
	runningConfig.DataDiskThresholdPercent = percent
}

func SetMaxParallelS3IngestBuffers(maxBuf uint64) {
	runningConfig.MaxParallelS3IngestBuffers = maxBuf
}
func SetPQSEnabled(enabled bool) {
	runningConfig.pqsEnabledConverted = enabled
	runningConfig.PQSEnabled = strconv.FormatBool(enabled)
}

func SetQueryPort(value uint64) {
	runningConfig.QueryPort = value
}

func IsMultinodeEnabled() bool {
	return runningConfig.Etcd.Enabled
}

func ValidateDeployment() (DeploymentType, error) {

	if runningConfig.Etcd.Enabled {
		if runningConfig.S3.Enabled {
			return DistributedS3, nil
		}
		return 0, fmt.Errorf("etcd must be enabled with S3")
	}
	if IsQueryNode() && IsIngestNode() {
		if runningConfig.S3.Enabled {
			return SingleNodeS3, nil
		}
		return SingleNode, nil
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
	fileInfo, err := os.Stat(configFilePath)
	if err != nil {
		log.Errorf("refreshConfig: Cannot stat config file while re-reading, err= %v", err)
		return err
	}
	configFileLastModified = uint64(fileInfo.ModTime().UTC().Unix())
	go refreshConfig()
	return nil
}

/*
	Use only for testing purpose, DO NOT use externally

To do - Currently we are assigning default value two times.. in InitializeDefaultConfig() for testing and
ExtractConfigData(). Do this in one time.
*/
func InitializeDefaultConfig() {

	// *************************************
	// THIS IS ONLY USED in TESTS, MAKE SURE:
	// 1. set the defaults ExtractConfigData
	// 2. set the defaults in server.yaml
	// 3. And Here.
	// ************************************

	runningConfig = Configuration{
		IngestPort:                 8081,
		QueryPort:                  5122,
		IngestUrl:                  "",
		EventTypeKeywords:          []string{"eventType"},
		QueryNode:                  "true",
		IngestNode:                 "true",
		SegFlushIntervalSecs:       5,
		DataPath:                   "data/",
		S3:                         S3Config{false, "", "", ""},
		RetentionHours:             24 * 90,
		TimeStampKey:               "timestamp",
		MaxSegFileSize:             1_073_741_824,
		LicenseKeyPath:             "./",
		ESVersion:                  "",
		Debug:                      false,
		MemoryThresholdPercent:     80,
		DataDiskThresholdPercent:   85,
		GRPCPort:                   50051,
		S3IngestQueueName:          "",
		S3IngestQueueRegion:        "",
		S3IngestBufferSize:         1000,
		MaxParallelS3IngestBuffers: 10,
		SSInstanceName:             "",
		PQSEnabled:                 "true",
		pqsEnabledConverted:        false,
		SafeServerStart:            false,
		AnalyticsEnabled:           "false",
		analyticsEnabledConverted:  false,
		AgileAggsEnabled:           "true",
		AgileAggsEnabledConverted:  true,
		QueryHostname:              "",
		Log:                        LogConfig{"", 100, false},
		TLS:                        TLSConfig{false, "certs/"},
		DatabaseConfig:             DatabaseConfig{Enabled: true, Provider: "sqlite"},
	}
	_ = InitDerivedConfig("test-uuid") // This is only used for testing
	runningConfig.EmailConfig = EmailConfig{"smtp.gmail.com", 587, "doe1024john@gmail.com", " "}
}

func InitializeTestingConfig() {
	InitializeDefaultConfig()
	SetDebugMode(true)
	SetDataPath("data/")
}

func ReadConfigFile(fileName string) (Configuration, error) {
	yamlData, err := os.ReadFile(fileName)
	if err != nil {
		log.Errorf("Cannot read input fileName = %v, err=%v", fileName, err)
	}
	return ExtractConfigData(yamlData)
}

func ExtractConfigData(yamlData []byte) (Configuration, error) {
	var config Configuration
	err := yaml.Unmarshal(yamlData, &config)
	if err != nil {
		log.Errorf("Error parsing yaml err=%v", err)
		return config, err
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
		config.PQSEnabled = "true"
	}
	pqsEnabled, err := strconv.ParseBool(config.PQSEnabled)
	if err != nil {
		log.Errorf("ExtractConfigData: failed to parse PQS enabled flag. Defaulting to true. Error: %v", err)
		pqsEnabled = true
		config.PQSEnabled = "true"
	}
	config.pqsEnabledConverted = pqsEnabled

	if len(config.AnalyticsEnabled) <= 0 {
		config.AnalyticsEnabled = "true"
	}
	analyticsEnabled, err := strconv.ParseBool(config.AnalyticsEnabled)
	if err != nil {
		log.Errorf("ExtractConfigData: failed to parse analytics enabled flag. Defaulting to true. Error: %v", err)
		analyticsEnabled = true
		config.AnalyticsEnabled = "true"
	}
	config.analyticsEnabledConverted = analyticsEnabled

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

	if config.GRPCPort == 0 {
		config.GRPCPort = 50051
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
	if len(config.TLS.ACMEFolder) >= 0 && strings.HasPrefix(config.TLS.ACMEFolder, "./") {
		config.TLS.ACMEFolder = strings.Trim(config.TLS.ACMEFolder, "./")
	}

	return config, nil
}

func SetConfig(config Configuration) {
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
	for {
		time.Sleep(MINUTES_REREAD_CONFIG * time.Minute)
		fileInfo, err := os.Stat(configFilePath)
		if err != nil {
			log.Errorf("refreshConfig: Cannot stat config file while re-reading, err= %v", err)
			continue
		}
		modifiedTime := fileInfo.ModTime()
		modifiedTimeSec := uint64(modifiedTime.UTC().Unix())
		if modifiedTimeSec > configFileLastModified {
			newConfig, err := ReadConfigFile(configFilePath)
			if err != nil {
				log.Errorf("refreshConfig: Cannot stat config file while re-reading, err= %v", err)
				continue
			}
			SetConfig(newConfig)
			configFileLastModified = modifiedTimeSec
		}
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
