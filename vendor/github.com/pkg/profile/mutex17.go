// +build !go1.8

package profile

// mock mutex support for Go 1.7 and earlier.

func enableMutexProfile() {}

func disableMutexProfile() {}
