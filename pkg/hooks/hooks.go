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
	RunSaasAuthCheck string
	RunRbacAuthCheck string
	RunOktaSignin    string
	LogoutButton     string
	DeleteIndex      string
}

type JsSnippets struct {
	// TODO: add string fields
}

var GlobalHooks = Hooks{
	ServeStaticHook: func(router *router.Router) {
		router.ServeFiles("/{filepath:*}", "./static")
	},
	ParseTemplatesHook: func(htmlTemplate *htmltemplate.Template, textTemplate *texttemplate.Template) {
		htmlTemplate = htmltemplate.Must(htmlTemplate.ParseGlob("./static/*.html"))
		textTemplate = texttemplate.Must(textTemplate.ParseGlob("./static/js/*.js"))
	},
}
