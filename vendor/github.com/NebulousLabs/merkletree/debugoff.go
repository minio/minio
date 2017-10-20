// +build !debug

package merkletree

const (
	// DEBUG indicates whether debugging is enabled. When debugging is enabled,
	// checks are performed on all stateful objects to make sure no supposedly
	// impossible conditions have occurred. The DEBUG flag is for developers.
	DEBUG = false
)
