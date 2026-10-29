package platform2

import (
	_ "embed"
	"partner-hub/web"
)

//go:embed index.html
var page []byte

func Page() []byte { return web.Render(page, "platform2") }
