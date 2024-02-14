package hooks

import (
	htmltemplate "html/template"
	texttemplate "text/template"

	"github.com/fasthttp/router"
)

type Hooks struct {
	StartupHook  func()
	HtmlSnippets HtmlSnippets
	JsSnippets   JsSnippets

	ServeStaticHook    func(router *router.Router, htmlTemplate *htmltemplate.Template)
	ParseTemplatesHook func(htmlTemplate *htmltemplate.Template, textTemplate *texttemplate.Template)
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
	OrgSettingsExtraImports    string

	IndexExtraImports string
}

type JsSnippets struct {
	ClusterStatsExtraFunctions  string
	ClusterStatsExtraSetup      string
	ClusterStatsSetUserRole     string
	ClusterStatsAdminView       string
	ClusterStatsAdminButton     string
	ClusterStatsCallDisplayRows string

	CommonExtraFunctions string

	NavBarExtraFunctions string

	SettingsExtraOnReadySetup      string
	SettingsRetentionDataThenBlock string
	SettingsExtraFunctions         string

	TestDataSendData string
}

var GlobalHooks = Hooks{
	ParseTemplatesHook: func(htmlTemplate *htmltemplate.Template, textTemplate *texttemplate.Template) {
		*htmlTemplate = *htmltemplate.Must(htmlTemplate.ParseGlob("./static/*.html"))
		*textTemplate = *texttemplate.Must(textTemplate.ParseGlob("./static/js/*.js"))
	},
}
