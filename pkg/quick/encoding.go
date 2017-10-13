/*
 * Quick - Quick key value store for config files and persistent state files
 *
 * Quick (C) 2017 Minio, Inc.
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

package quick

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	yaml "gopkg.in/yaml.v2"
)

// ConfigEncoding is a generic interface which
// marshal/unmarshal configuration.
type ConfigEncoding interface {
	Unmarshal([]byte, interface{}) error
	Marshal(interface{}) ([]byte, error)
}

// YAML encoding implements ConfigEncoding
type yamlEncoding struct{}

func (y yamlEncoding) Unmarshal(b []byte, v interface{}) error {
	return yaml.Unmarshal(b, v)
}

func (y yamlEncoding) Marshal(v interface{}) ([]byte, error) {
	return yaml.Marshal(v)
}

// JSON encoding implements ConfigEncoding
type jsonEncoding struct{}

func (j jsonEncoding) Unmarshal(b []byte, v interface{}) error {
	err := json.Unmarshal(b, v)
	if err != nil {
		// Try to return a sophisticated json error message if possible
		switch jerr := err.(type) {
		case *json.SyntaxError:
			return fmt.Errorf("Unable to parse JSON schema due to a syntax error at '%s'",
				FormatJSONSyntaxError(bytes.NewReader(b), jerr.Offset))
		case *json.UnmarshalTypeError:
			return fmt.Errorf("Unable to parse JSON, type '%v' cannot be converted into the Go '%v' type",
				jerr.Value, jerr.Type)
		}
		return err
	}
	return nil
}

func (j jsonEncoding) Marshal(v interface{}) ([]byte, error) {
	return json.MarshalIndent(v, "", "\t")
}

// Convert a file extension to the appropriate struct capable
// to marshal/unmarshal data
func ext2EncFormat(fileExtension string) ConfigEncoding {
	// Lower the file extension
	ext := strings.ToLower(fileExtension)
	ext = strings.TrimPrefix(ext, ".")
	// Return the appropriate encoder/decoder according
	// to the extension
	switch ext {
	case "yml", "yaml":
		// YAML
		return yamlEncoding{}
	default:
		// JSON
		return jsonEncoding{}
	}
}

// toMarshaller returns the right marshal function according
// to the given file extension
func toMarshaller(ext string) func(interface{}) ([]byte, error) {
	return ext2EncFormat(ext).Marshal
}

// toUnmarshaller returns the right marshal function according
// to the given file extension
func toUnmarshaller(ext string) func([]byte, interface{}) error {
	return ext2EncFormat(ext).Unmarshal
}

// saveFileConfig marshals with the right encoding format
// according to the filename extension, if no extension is
// provided, json will be selected.
func saveFileConfig(filename string, v interface{}) error {
	// Fetch filename's extension
	ext := filepath.Ext(filename)
	// Marshal data
	dataBytes, err := toMarshaller(ext)(v)
	if err != nil {
		return err
	}
	if runtime.GOOS == "windows" {
		dataBytes = []byte(strings.Replace(string(dataBytes), "\n", "\r\n", -1))
	}
	// Save data.
	return writeFile(filename, dataBytes)

}

// loadFileConfig unmarshals the file's content with the right
// decoder format according to the filename extension. If no
// extension is provided, json will be selected by default.
func loadFileConfig(filename string, v interface{}) error {
	if _, err := os.Stat(filename); err != nil {
		return err
	}
	fileData, err := ioutil.ReadFile(filename)
	if err != nil {
		return err
	}
	if runtime.GOOS == "windows" {
		fileData = []byte(strings.Replace(string(fileData), "\r\n", "\n", -1))
	}
	// Unmarshal file's content
	return toUnmarshaller(filepath.Ext(filename))(fileData, v)
}
