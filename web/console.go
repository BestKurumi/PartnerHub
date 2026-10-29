// Package web embeds the shared, dependency-free console presentation assets.
package web

import (
	"bytes"
	"embed"
)

//go:embed console.css
var style []byte

//go:embed console.js
var script []byte

//go:embed *-extra.js
var pages embed.FS

// Render returns an independent page without external script or font requests.
func Render(template []byte, app string) []byte {
	out := bytes.Replace(template, []byte("/*CONSOLE_CSS*/"), style, 1)
	out = bytes.Replace(out, []byte("/*CONSOLE_JS*/"), script, 1)
	page, _ := pages.ReadFile(app + "-extra.js")
	return bytes.Replace(out, []byte("/*APP_JS*/"), page, 1)
}
