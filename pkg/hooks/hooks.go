package hooks

import (
	htmltemplate "html/template"
	texttemplate "text/template"

	"github.com/fasthttp/router"
	commonconfig "github.com/siglens/siglens/pkg/config/common"
	"github.com/siglens/siglens/pkg/utils"
	"github.com/valyala/fasthttp"
)

type Hooks struct {
	StartupHook  func()
	HtmlSnippets HtmlSnippets
	JsSnippets   JsSnippets

	// Startup
	ServeStaticHook        func(router *router.Router, htmlTemplate *htmltemplate.Template)
	ParseTemplatesHook     func(htmlTemplate *htmltemplate.Template, textTemplate *texttemplate.Template)
	CheckLicenseHook       func()
	CheckOrgValidityHook   func()
	AfterConfigHook        func(baseLogDir string)
	ValidateDeploymentHook func() (commonconfig.DeploymentType, error)
	GetNodeIdHook          func() string
	ExtractConfigHook      func(yamlData []byte) (commonconfig.Configuration, error)
	LogConfigHook          func()
	StartSiglensExtrasHook func(nodeID string) error

	// Cluster health
	IngestStatsHandlerHook     func(ctx *fasthttp.RequestCtx, myid uint64)
	StatsHandlerHook           func(ctx *fasthttp.RequestCtx, myid uint64)
	SetExtraIngestionStatsHook func(map[string]interface{})
	MiddlewareExtractOrgIdHook func(ctx *fasthttp.RequestCtx) (uint64, error)
	AddMultinodeStatsHook      func(indexData map[string]utils.ResultPerIndex, orgId uint64,
		logsIncomingBytes *float64, logsOnDiskBytes *float64, logsEventCount *int64,
		metricsIncomingBytes *uint64, metricsOnDiskBytes *uint64, metricsDatapointsCount *uint64,
		queryCount *uint64, totalResponseTime *float64)

	// Retention
	ExtraRetentionCleanerHook     func() error
	InternalRetentionCleanerHook1 func() string
	InternalRetentionCleanerHook2 func(string, int)

	// Usage stats
	GetQueryCountHook                 func()
	WriteUsageStatsIfConditionHook    func() bool
	WriteUsageStatsElseExtraLogicHook func()
	ForceFlushIfConditionHook         func() bool

	// Blobstore
	InitBlobStoreExtrasHook             func() (bool, error)
	UploadSegmentFilesExtrasHook        func(allFiles []string) (bool, error)
	UploadIngestNodeExtrasHook          func() (bool, error)
	UploadQueryNodeExtrasHook           func() (bool, error)
	DeleteBlobExtrasHook                func(filepath string) (bool, error)
	DownloadAllIngestNodesDirExtrasHook func() (bool, error)
	DownloadAllQueryNodesDirExtrasHook  func() (bool, error)
	DownloadSegmentBlobExtrasHook       func(filename string) (bool, error)
	GetFileSizeExtrasHook               func(filename string) (bool, uint64)
	DoesMetaFileExistExtrasHook         func(filename string) (bool, bool, error)

	// Server helpers
	GetOrgIdHookQuery         func(ctx *fasthttp.RequestCtx) (uint64, error)
	GetOrgIdHook              func(ctx *fasthttp.RequestCtx) (uint64, error)
	ExtractKibanaRequestsHook func(kibanaIndices []string, qid uint64) map[string]interface{}

	// Ingest server
	IngestMiddlewareRecoveryHook func(ctx *fasthttp.RequestCtx) error
	KibanaIngestHandlerHook      func(ctx *fasthttp.RequestCtx)
	GetIdsConditionHook          func(ids []uint64) bool
	ExtraIngestEndpointsHook     func(router *router.Router, recovery func(next func(ctx *fasthttp.RequestCtx)) func(ctx *fasthttp.RequestCtx))

	// Query server
	QueryMiddlewareRecoveryHook func(ctx *fasthttp.RequestCtx) error
	ExtraQueryEndpointsHook     func(router *router.Router, recovery func(next func(ctx *fasthttp.RequestCtx)) func(ctx *fasthttp.RequestCtx)) error

	// Query summary
	ShouldAddDistributedInfoHook func() bool
}

type HtmlSnippets struct {
	RunCheck1 string
	RunCheck2 string
	RunCheck3 string
	Button1   string
	Popup1    string

	OrgSettingsOrgName         string
	OrgSettingsRetentionPeriod string
	OrgSettingsExtras          string

	Constants map[string]interface{}
}

type JsSnippets struct {
	ClusterStatsExtraFunctions  string
	ClusterStatsExtraSetup      string
	ClusterStatsSetUserRole     string
	ClusterStatsAdminView       string
	ClusterStatsAdminButton     string
	ClusterStatsCallDisplayRows string

	CommonExtraFunctions string
	Button1Function      string

	SettingsExtraOnReadySetup      string
	SettingsRetentionDataThenBlock string
	SettingsExtraFunctions         string

	TestDataSendData string

	OrgUpperNavTabs string
	OrgUpperNavUrls string
}

var GlobalHooks = Hooks{
	ParseTemplatesHook: func(htmlTemplate *htmltemplate.Template, textTemplate *texttemplate.Template) {
		*htmlTemplate = *htmltemplate.Must(htmlTemplate.ParseGlob("./static/*.html"))
		*textTemplate = *texttemplate.Must(textTemplate.ParseGlob("./static/js/*.js"))
	},
}
