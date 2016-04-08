package main

import "errors"

var errDiskPathFull = errors.New("disk path full")
var errVolumeExists = errors.New("volume already exists")
var errVolumeNotFound = errors.New("volume not found")
var errFileNotFound = errors.New("file not found")
