// +build go1.8

package profile

import "runtime"

func enableMutexProfile() {
	runtime.SetMutexProfileFraction(1)
}

func disableMutexProfile() {
	runtime.SetMutexProfileFraction(0)
}
