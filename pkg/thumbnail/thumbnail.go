/*
 * Minio Cloud Storage, (C) 2017 Minio, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Package thumbnail allows for easy and fast generation of image thumbnails
// across different platforms by selecting the fastest available generator.
package thumbnail

import (
	"errors"
	"io"
)

// Binary flags to avoid using a specific library.
const (
	AvoidConvert = 1 << iota
	AvoidRez
)

// ErrNoLibrary signifies no library was available to convert the image with.
// They may all be disabled or unreachable or were causing errors. This may
// be a problem with the image itself.
var ErrNoLibrary = errors.New("no library available")

// Options allows for options to be passed to GenerateThumbnail.
type Options struct {
	// The dimensions of the output thumbnail image, in format WIDTHxHEIGHT.
	Dimensions string

	// The format of the output image, i.e. jpg, png, gif.
	Format string

	// The collection of libraries to not test, i.e. AvoidConvert to avoid ImageMagick's `convert`.
	AvoidLibraries int
}

// GenerateThumbnail generates a thumbnail of an image. It takes in an image from `input`,
// an io.Reader, and returns the generated thumbnail through `output`, an io.Writer. It tries
// multiple types of thumbnail generating tools, fastest first.
func GenerateThumbnail(input io.Reader, output io.Writer, options Options) (err error) {
	// First, try to use `convert` to resize the image.
	if options.AvoidLibraries&AvoidConvert == 0 {
		err = resizeUsingConvert(input, output, options.Dimensions, options.Format)
		if err == nil {
			// Success, stop execution.
			return
		}
	}

	// Next try to use rez, a native Go library.
	if options.AvoidLibraries&AvoidRez == 0 {
		err = resizeUsingRez(input, output, options.Dimensions, options.Format)
		return err
	}

	return ErrNoLibrary
}
