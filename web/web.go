// Package web embeds the dashboard into the binary, so deployment stays a single file.
package web

import _ "embed"

//go:embed index.html
var Index []byte

//go:embed login.html
var Login []byte
