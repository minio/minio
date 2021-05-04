package browser

import "embed"

//go:embed release/*
var fs embed.FS

// GetStaticAssets returns assets
func GetStaticAssets() embed.FS {
	return fs
}
