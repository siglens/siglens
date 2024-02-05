package lookuptable

import (
	"html/template"

	"github.com/fasthttp/router"
)

type LookupTable struct {
	StartupHook func()

	ServeHtmlHook func(router *router.Router)
	TemplateHook  func() *template.Template
}

var GlobalLookupTable = LookupTable{
	ServeHtmlHook: func(router *router.Router) {
		router.ServeFiles("/{filepath:*}", "./static")
	},
	TemplateHook: func() *template.Template {
		return template.Must(template.ParseGlob("./static/*.html"))
	},
}
