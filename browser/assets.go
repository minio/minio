package browser

import "embed"

//go:embed production/*
var fs embed.FS

// GetStaticAssets returns assets
func GetStaticAssets() embed.FS {
	return fs
}
