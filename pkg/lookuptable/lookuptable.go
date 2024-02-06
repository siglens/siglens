package lookuptable

import (
	htmltemplate "html/template"
	texttemplate "text/template"

	"github.com/fasthttp/router"
)

type LookupTable struct {
	StartupHook func()

	ServeHtmlHook func(router *router.Router)
	TemplateHook  func(htmlTemplate *htmltemplate.Template, textTemplate *texttemplate.Template)

	// HTML snippets
	HelloHtml           string
	LinkToOtherPageHtml string

	// JavaScript snippets
	HelloJs string
}

var GlobalLookupTable = LookupTable{
	ServeHtmlHook: func(router *router.Router) {
		router.ServeFiles("/{filepath:*}", "./static")
	},
	TemplateHook: func(htmlTemplate *htmltemplate.Template, textTemplate *texttemplate.Template) {
		htmlTemplate = htmltemplate.Must(htmlTemplate.ParseGlob("./static/*.html"))
		textTemplate = texttemplate.Must(textTemplate.ParseGlob("./static/js/*.js"))
	},
}
