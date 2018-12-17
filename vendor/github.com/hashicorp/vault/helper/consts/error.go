package consts

import "errors"

var (
	// ErrSealed is returned if an operation is performed on a sealed barrier.
	// No operation is expected to succeed before unsealing
	ErrSealed = errors.New("Vault is sealed")

	// ErrStandby is returned if an operation is performed on a standby Vault.
	// No operation is expected to succeed until active.
	ErrStandby = errors.New("Vault is in standby mode")

	// Used when .. is used in a path
	ErrPathContainsParentReferences = errors.New("path cannot contain parent references")
)
