// Package linenoise wraps the linenoise library (https://github.com/antirez/linenoise).
//
// The package is imported with "go." prefix
// 	import "github.com/GeertJohan/go.linenoise"
//
// Simple readline usage:
// 	linenoise.Line("prompt> ")
//
// Adding lines to history, you could simply do this for every line you read.
// 	linenoise.AddHistory("This will be displayed in prompt when arrow-up is pressed")
package linenoise
