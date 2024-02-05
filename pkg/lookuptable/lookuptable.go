package lookuptable

import (
	"html/template"

	"github.com/fasthttp/router"
)

type LookupTable struct {
	StartupHook func()

	ServeHtmlHook func(router *router.Router)
	TemplateHook  func(tpl *template.Template)

	// HTML templates
	HelloHtml string
}

var GlobalLookupTable = LookupTable{
	ServeHtmlHook: func(router *router.Router) {
		router.ServeFiles("/{filepath:*}", "./static")
	},
	TemplateHook: func(tpl *template.Template) {
		tpl = template.Must(tpl.ParseGlob("./static/*.html"))
	},
}
