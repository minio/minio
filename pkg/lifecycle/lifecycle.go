/*
 * Minio Cloud Storage, (C) 2019 Minio, Inc.
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

package lifecycle

import (
	"encoding/xml"
	"fmt"
	"io"
)

// LifeCycle - Configuration for bucket lifecycle.
type LifeCycle struct {
	XMLName xml.Name `xml:"LifecycleConfiguration"`
	Rules   []Rule   `xml:"Rule"`
}

// IsEmpty - returns whether policy is empty or not.
func (lifecycle LifeCycle) IsEmpty() bool {
	return len(lifecycle.Rules) == 0
}

// ParseConfig - parses data in given reader to LifeCycle.
func ParseConfig(reader io.Reader) (*LifeCycle, error) {
	var lifecycle LifeCycle
	if err := xml.NewDecoder(reader).Decode(&lifecycle); err != nil {
		return nil, err
	}

	if err := lifecycle.Validate(); err != nil {
		return nil, err
	}
	return &lifecycle, nil
}

func (lc LifeCycle) tooManyRules() error {
	if len(lc.Rules) > 1000 {
		return fmt.Errorf("Lifecycle configuration allows a maximum of 1000 rules")
	}
	return nil
}

func (lc LifeCycle) noRules() error {
	if len(lc.Rules) == 0 {
		return fmt.Errorf("Lifecycle configuration should have at least one rule")
	}
	return nil
}

// Validate - validates the lifecycle configuration
func (lc LifeCycle) Validate() error {
	if err := lc.tooManyRules(); err != nil {
		return err
	}
	if err := lc.noRules(); err != nil {
		return err
	}
	// for _, r := range lc.Rules {
	// 	if err := r.Validate(); err != nil {
	// 		return err
	// 	}
	// }
	return nil
}
