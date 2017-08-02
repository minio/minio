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
	"io/ioutil"
	"os"
	"testing"
)

func TestResizeUsingRez(t *testing.T) {
	// It should pass on errors from image.Decode.
	badImage := bytes.NewBufferString("\x00")

	err := resizeUsingRez(badImage, ioutil.Discard, "", "")
	if err != image.ErrFormat {
		t.Errorf("expected image.ErrFormat, got %v", err)
	}

	// It should error on a bad output format.
	img, err := os.Open("testdata/car.png")
	if err != nil {
		t.Fatal(err)
	}
	defer img.Close()

	err = resizeUsingRez(img, ioutil.Discard, "100x100", "badimageformat")
	if err == nil {
		t.Error("expected error, got nil")
	}
}
