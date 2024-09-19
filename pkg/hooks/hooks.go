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

package hooks

import (
	htmltemplate "html/template"
	texttemplate "text/template"

	"github.com/fasthttp/router"
	commonconfig "github.com/siglens/siglens/pkg/config/common"
	"github.com/siglens/siglens/pkg/grpc"
	segutils "github.com/siglens/siglens/pkg/segment/utils"
	"github.com/siglens/siglens/pkg/utils"
	"github.com/valyala/fasthttp"
)

type Hooks struct {
	StartupHook  func()
	HtmlSnippets HtmlSnippets
	JsSnippets   JsSnippets

	// Startup and shutdown
	ServeStaticHook           func(router *router.Router, htmlTemplate *htmltemplate.Template)
	ParseTemplatesHook        func(htmlTemplate *htmltemplate.Template, textTemplate *texttemplate.Template)
	CheckLicenseHook          func()
	CheckOrgValidityHook      func()
	AfterConfigHook           func(baseLogDir string)
	ValidateDeploymentHook    func() (commonconfig.DeploymentType, error)
	GetNodeIdHook             func() string
	ExtractConfigHook         func(yamlData []byte) (commonconfig.Configuration, error)
	LogConfigHook             func()
	StartSiglensExtrasHook    func(nodeID string) error
	ShutdownSiglensExtrasHook func()
	ShutdownSiglensPreHook    func()

	// Cluster health
	IngestStatsHandlerHook     func(ctx *fasthttp.RequestCtx, myid uint64)
	StatsHandlerHook           func(ctx *fasthttp.RequestCtx, myid uint64)
	SetExtraIngestionStatsHook func(map[string]interface{})
	MiddlewareExtractOrgIdHook func(ctx *fasthttp.RequestCtx) (uint64, error)
	// TODO: There are too many arguments here. Consider refactoring by creating a struct.
	AddMultinodeStatsHook func(indexData utils.AllIndexesStats, orgId uint64,
		logsIncomingBytes *float64, logsOnDiskBytes *float64, logsEventCount *int64,
		metricsIncomingBytes *uint64, metricsOnDiskBytes *uint64, metricsDatapointsCount *uint64,
		queryCount *uint64, totalResponseTimeSinceRestart *float64, totalResponseTimeSinceInstall *float64,
		totalQueryCountSinceInstall *uint64, totalColumnsSet map[string]struct{})

	AddMultinodeSystemInfoHook func(ctx *fasthttp.RequestCtx)
	// rStats is of type usageStats.ReadStats
	AddMultinodeIngestStatsHook func(rStats interface{}, pastXhours uint64, granularity uint8, orgId uint64)
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
	EsBulkIngestInternalHook     func(*fasthttp.RequestCtx, map[string]interface{}, string, bool, string, uint64, uint64) error
	GetIdsConditionHook          func() (bool, []uint64)
	ExtraIngestEndpointsHook     func(router *router.Router, recovery func(next func(ctx *fasthttp.RequestCtx)) func(ctx *fasthttp.RequestCtx))
	OverrideIngestRequestHook    func(ctx *fasthttp.RequestCtx, myid uint64, ingestFunc grpc.IngestFuncEnum, useIngestHook bool) bool

	// Query server
	QueryMiddlewareRecoveryHook func(ctx *fasthttp.RequestCtx) error
	ExtraQueryEndpointsHook     func(router *router.Router, recovery func(next func(ctx *fasthttp.RequestCtx)) func(ctx *fasthttp.RequestCtx)) error

	// Query summary
	ShouldAddDistributedInfoHook func() bool

	// Distributed query
	InitDistributedQueryServiceHook func(querySummary interface{}, allSegFileResults interface{}) interface{}
	FilterQsrsHook                  func(qsrs interface{}, isRotated bool) (interface{}, error)

	// Handling ingestion
	BeforeHandlingBulkRequest func(ctx *fasthttp.RequestCtx, myid uint64) (bool, uint64)
	AfterWritingToSegment     func(rid uint64, segstore interface{}, record []byte, ts uint64, signalType segutils.SIGNAL_TYPE) error
	AfterHandlingBulkRequest  func(ctx *fasthttp.RequestCtx, rid uint64) bool
	RotateSegment             func(segstore interface{}, streamId string, forceRotate bool) (bool, error)
	AddSegMeta                func(segmeta interface{}) (bool, error)
	AfterSegmentRotation      func(segmeta interface{}) error
}

type HtmlSnippets struct {
	RunCheck1 string
	RunCheck2 string
	RunCheck3 string
	Button1   string
	Popup1    string

	OrgSettingsOrgName         string
	OrgSettingsRetentionPeriod string
	OrgDeploymentType          string
	OrgSettingsExtras          string
	DistNodesExtras            string
	OrgSLOs                    string
	SLOCss                     string
	EnterpriseEnabled          bool
	Constants                  map[string]interface{}
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

	OrgAllSlos string
	PanelFlag  bool
}

var GlobalHooks = Hooks{
	ParseTemplatesHook: func(htmlTemplate *htmltemplate.Template, textTemplate *texttemplate.Template) {
		*htmlTemplate = *htmltemplate.Must(htmlTemplate.ParseGlob("./static/*.html"))
		*textTemplate = *texttemplate.Must(textTemplate.ParseGlob("./static/js/*.js"))
	},
}
