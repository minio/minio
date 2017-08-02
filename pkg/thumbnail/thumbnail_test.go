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
	"bytes"
	"image"
	_ "image/gif"
	_ "image/jpeg"
	"io/ioutil"
	"os"
	"testing"
)

func TestGenerateThumbnail(t *testing.T) {
	// Generate a thumbnail for testdata/car.png and check to make sure
	// it is both a JPEG and 128x78 (convert does it differently).
	img1, err := os.Open("testdata/car.png")
	if err != nil {
		t.Fatal(err)
	}
	defer img1.Close()

	options := Options{
		Dimensions:     "128x128",
		Format:         "jpeg",
		AvoidLibraries: AvoidRez,
	}

	buffer := new(bytes.Buffer)

	err = GenerateThumbnail(img1, buffer, options)
	if err != nil {
		t.Error(err)
	}

	img, format, err := image.Decode(buffer)
	if err != nil {
		t.Error(err)
	} else {
		if format != "jpeg" {
			t.Errorf("expected jpeg format, got %s", format)
		}

		// convert should give us a non-square thumbnail
		width := img.Bounds().Max.X
		height := img.Bounds().Max.Y
		if width != 128 || height != 78 {
			t.Errorf("expected 128x78 dimensions, got %dx%d", width, height)
		}
	}

	// Now do it for a jpg.
	img2, err := os.Open("testdata/beach.jpg")
	if err != nil {
		t.Fatal(err)
	}
	defer img2.Close()

	options = Options{
		Dimensions:     "64x64",
		Format:         "gif",
		AvoidLibraries: AvoidConvert,
	}

	buffer = new(bytes.Buffer)

	err = GenerateThumbnail(img2, buffer, options)
	if err != nil {
		t.Error(err)
	}

	img, format, err = image.Decode(buffer)
	if err != nil {
		t.Error(err)
	} else {
		if format != "gif" {
			t.Errorf("expected gif format, got %s", format)
		}

		width := img.Bounds().Max.X
		height := img.Bounds().Max.Y
		if width != 64 || height != 64 {
			t.Errorf("expected 64x64 dimensions, got %dx%d", width, height)
		}
	}

	// This should fail with no libraries.
	options = Options{
		Dimensions:     "1x1",
		Format:         "jpeg",
		AvoidLibraries: AvoidConvert | AvoidRez,
	}

	err = GenerateThumbnail(img2, ioutil.Discard, options)
	if err != ErrNoLibrary {
		t.Errorf("expected ErrNoLibrary, got %s", err.Error())
	}
}
