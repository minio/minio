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

package thumbnail

import (
	"fmt"
	"image"
	"image/gif"
	"image/jpeg"
	"image/png"
	"io"

	"github.com/bamiaux/rez"
)

// Attempts to resize an image using the rez library.
func resizeUsingRez(inputReader io.Reader, outputWriter io.Writer, dimensions, format string) error {
	// Get images
	input, _, err := image.Decode(inputReader)
	if err != nil {
		return err
	}

	var width int
	var height int
	fmt.Sscanf(dimensions, "%dx%d", &width, &height)

	typedImage, ok := input.(*image.YCbCr)
	if !ok {
		return fmt.Errorf("input picture must be YCbCr")
	}

	var output image.Image
	output = image.NewYCbCr(image.Rect(0, 0, width, height), typedImage.SubsampleRatio)

	// Pump it into the library.
	err = rez.Convert(output, input, rez.NewBilinearFilter())
	if err != nil {
		return err
	}

	// Write the output image to the output file.
	if format == "jpeg" {
		err = jpeg.Encode(outputWriter, output, nil)
	} else if format == "png" {
		err = png.Encode(outputWriter, output)
	} else if format == "gif" {
		err = gif.Encode(outputWriter, output, nil)
	} else {
		err = fmt.Errorf("unsupported image format '%s' for rez (use jpeg, png, or gif)", format)
	}

	return err
}
