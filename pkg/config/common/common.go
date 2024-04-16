package common

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
	Enabled         bool   `yaml:"enabled"`         // enable/disable tls
	CertificatePath string `yaml:"certificatePath"` // path to certificate file
	PrivateKeyPath  string `yaml:"privateKeyPath"`  // path to private key file
}

type TracingConfig struct {
	ServiceName        string  `yaml:"serviceName"`        // service name for tracing
	Endpoint           string  `yaml:"endpoint"`           // endpoint URL for tracing
	SamplingPercentage float64 `yaml:"samplingPercentage"` // sampling percentage for tracing (0-100)
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
	IngestListenIP             string   `yaml:"ingestListenIP"`       // Listen IP used for ingestion server
	QueryListenIP              string   `yaml:"queryListenIP"`        // Listen IP used for query server
	IngestPort                 uint64   `yaml:"ingestPort"`           // Port for ingestion server
	QueryPort                  uint64   `yaml:"queryPort"`            // Port used for query server
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
	S3IngestQueueName          string   `yaml:"s3IngestQueueName"`
	S3IngestQueueRegion        string   `yaml:"s3IngestQueueRegion"`
	S3IngestBufferSize         uint64   `yaml:"s3IngestBufferSize"`
	MaxParallelS3IngestBuffers uint64   `yaml:"maxParallelS3IngestBuffers"`
	SSInstanceName             string   `yaml:"ssInstanceName"`
	PQSEnabled                 string   `yaml:"pqsEnabled"` // is pqs enabled?
	PQSEnabledConverted        bool     // converted bool value of PQSEnabled yaml
	SafeServerStart            bool     `yaml:"safeMode"`         // if set to true, siglens will start a mock webserver with a custom health handler. Actual server will NOT be started
	AnalyticsEnabled           string   `yaml:"analyticsEnabled"` // is analytics enabled?
	AnalyticsEnabledConverted  bool
	AgileAggsEnabled           string `yaml:"agileAggsEnabled"` // should we read/write AgileAggsTrees?
	AgileAggsEnabledConverted  bool
	QueryHostname              string         `yaml:"queryHostname"` // hostname of the query server. i.e. if DNS is https://cloud.siglens.com, this should be cloud.siglens.com
	IngestUrl                  string         `yaml:"ingestUrl"`     // full address of the ingest server, including scheme and port, e.g. https://ingest.siglens.com:8080
	S3                         S3Config       `yaml:"s3"`            // s3 related config
	Log                        LogConfig      `yaml:"log"`           // Log related config
	TLS                        TLSConfig      `yaml:"tls"`           // TLS related config
	Tracing                    TracingConfig  `yaml:"tracing"`       // Tracing related config
	EmailConfig                EmailConfig    `yaml:"emailConfig"`
	DatabaseConfig             DatabaseConfig `yaml:"minionSearch"`
}

type RunModConfig struct {
	PQSEnabled bool `json:"pqsEnabled"`
}
