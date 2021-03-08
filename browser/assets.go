package browser

import "embed"

//go:embed production/*
var fs embed.FS

func GetStaticAssets() embed.FS {
  return fs
}
