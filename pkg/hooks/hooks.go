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

	ServeStaticHook    func(router *router.Router)
	ParseTemplatesHook func(htmlTemplate *htmltemplate.Template, textTemplate *texttemplate.Template)
}

type HtmlSnippets struct {
	RunSaasAuthCheck              string
	RunRbacAuthCheck              string
	RunOktaSignin                 string
	LogoutButton                  string
	DeleteIndex                   string
	OrgName                       string
	RetentionPeriod               string
	ExtraOrgSettings              string
	ImportExtraOrgSettingsScripts string
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
}

var GlobalHooks = Hooks{
	ServeStaticHook: func(router *router.Router) {
		router.ServeFiles("/{filepath:*}", "./static")
	},
	ParseTemplatesHook: func(htmlTemplate *htmltemplate.Template, textTemplate *texttemplate.Template) {
		htmlTemplate = htmltemplate.Must(htmlTemplate.ParseGlob("./static/*.html"))
		textTemplate = texttemplate.Must(textTemplate.ParseGlob("./static/js/*.js"))
	},

	HtmlSnippets: HtmlSnippets{
		OrgName:         `<td id="orgName">SigLens</td>`,
		RetentionPeriod: `<td id="retention-days-value">4 days</td>`,
	},
}
