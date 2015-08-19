/*
 * Minimalist Object Storage, (C) 2015 Minio, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses)/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Package probe implements a simple mechanism to trace and return errors in large programs.
package probe

// wrappedError implements a container for *probe.Error.
type wrappedError struct {
	err *Error
}

// WrapError function wraps a *probe.Error into a 'error' compatible duck type.
func WrapError(err *Error) error {
	return &wrappedError{err: err}
}

// UnwrapError tries to convert generic 'error' into typed *probe.Error and returns true, false otherwise.
func UnwrapError(err error) (*Error, bool) {
	switch e := err.(type) {
	case *wrappedError:
		return e.err, true
	default:
		return nil, false
	}
}

// Error interface method.
func (w *wrappedError) Error() string {
	return w.err.String()
}
