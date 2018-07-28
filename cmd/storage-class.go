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

package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

const (
	// metadata entry for storage class
	amzStorageClass = "x-amz-storage-class"
	// Canonical metadata entry for storage class
	amzStorageClassCanonical = "X-Amz-Storage-Class"
	// Reduced redundancy storage class
	reducedRedundancyStorageClass = "REDUCED_REDUNDANCY"
	// Standard storage class
	standardStorageClass = "STANDARD"
	// Reduced redundancy storage class environment variable
	reducedRedundancyStorageClassEnv = "MINIO_STORAGE_CLASS_RRS"
	// Standard storage class environment variable
	standardStorageClassEnv = "MINIO_STORAGE_CLASS_STANDARD"
	// Supported storage class scheme is EC
	supportedStorageClassScheme = "EC"
	// Minimum parity disks
	minimumParityDisks = 2
	defaultRRSParity   = 2
)

// Struct to hold storage class
type storageClass struct {
	Scheme string
	Parity int
}

type storageClassConfig struct {
	Standard storageClass `json:"standard"`
	RRS      storageClass `json:"rrs"`
}

// Validate SS and RRS parity when unmarshalling JSON.
func (sCfg *storageClassConfig) UnmarshalJSON(data []byte) error {
	type Alias storageClassConfig
	aux := &struct {
		*Alias
	}{
		Alias: (*Alias)(sCfg),
	}
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}
	return validateParity(aux.Standard.Parity, aux.RRS.Parity)
}

// Validate if storage class in metadata
// Only Standard and RRS Storage classes are supported
func isValidStorageClassMeta(sc string) bool {
	return sc == reducedRedundancyStorageClass || sc == standardStorageClass
}

func (sc *storageClass) UnmarshalText(b []byte) error {
	scStr := string(b)
	if scStr == "" {
		return nil
	}
	s, err := parseStorageClass(scStr)
	if err != nil {
		return err
	}
	sc.Parity = s.Parity
	sc.Scheme = s.Scheme
	return nil
}

func (sc *storageClass) MarshalText() ([]byte, error) {
	if sc.Scheme != "" && sc.Parity != 0 {
		return []byte(fmt.Sprintf("%s:%d", sc.Scheme, sc.Parity)), nil
	}
	return []byte(""), nil
}

// Parses given storageClassEnv and returns a storageClass structure.
// Supported Storage Class format is "Scheme:Number of parity disks".
// Currently only supported scheme is "EC".
func parseStorageClass(storageClassEnv string) (sc storageClass, err error) {
	s := strings.Split(storageClassEnv, ":")

	// only two elements allowed in the string - "scheme" and "number of parity disks"
	if len(s) > 2 {
		return storageClass{}, uiErrStorageClassValue(nil).Msg("Too many sections in " + storageClassEnv)
	} else if len(s) < 2 {
		return storageClass{}, uiErrStorageClassValue(nil).Msg("Too few sections in " + storageClassEnv)
	}

	// only allowed scheme is "EC"
	if s[0] != supportedStorageClassScheme {
		return storageClass{}, uiErrStorageClassValue(nil).Msg("Unsupported scheme " + s[0] + ". Supported scheme is EC")
	}

	// Number of parity disks should be integer
	parityDisks, err := strconv.Atoi(s[1])
	if err != nil {
		return storageClass{}, uiErrStorageClassValue(err)
	}

	sc = storageClass{
		Scheme: s[0],
		Parity: parityDisks,
	}

	return sc, nil
}

// Validates the parity disks.
func validateParity(ssParity, rrsParity int) (err error) {
	if ssParity == 0 && rrsParity == 0 {
		return nil
	}

	if !globalIsXL {
		return fmt.Errorf("Setting storage class only allowed for erasure coding mode")
	}

	// SS parity disks should be greater than or equal to minimumParityDisks. Parity below minimumParityDisks is not recommended.
	if ssParity > 0 && ssParity < minimumParityDisks {
		return fmt.Errorf("Standard storage class parity %d should be greater than or equal to %d", ssParity, minimumParityDisks)
	}

	// RRS parity disks should be greater than or equal to minimumParityDisks. Parity below minimumParityDisks is not recommended.
	if rrsParity > 0 && rrsParity < minimumParityDisks {
		return fmt.Errorf("Reduced redundancy storage class parity %d should be greater than or equal to %d", rrsParity, minimumParityDisks)
	}

	if ssParity > globalXLSetDriveCount/2 {
		return fmt.Errorf("Standard storage class parity %d should be less than or equal to %d", ssParity, globalXLSetDriveCount/2)
	}

	if rrsParity > globalXLSetDriveCount/2 {
		return fmt.Errorf("Reduced redundancy storage class parity %d should be less than  or equal to %d", rrsParity, globalXLSetDriveCount/2)
	}

	if ssParity > 0 && rrsParity > 0 {
		if ssParity < rrsParity {
			return fmt.Errorf("Standard storage class parity disks %d should be greater than or equal to Reduced redundancy storage class parity disks %d", ssParity, rrsParity)
		}
	}
	return nil
}

// Returns the data and parity drive count based on storage class
// If storage class is set using the env vars MINIO_STORAGE_CLASS_RRS and MINIO_STORAGE_CLASS_STANDARD
// or config.json fields
// -- corresponding values are returned
// If storage class is not set during startup, default values are returned
// -- Default for Reduced Redundancy Storage class is, parity = 2 and data = N-Parity
// -- Default for Standard Storage class is, parity = N/2, data = N/2
// If storage class is empty
// -- standard storage class is assumed and corresponding data and parity is returned
func getRedundancyCount(sc string, totalDisks int) (data, parity int) {
	parity = totalDisks / 2
	switch sc {
	case reducedRedundancyStorageClass:
		if globalRRStorageClass.Parity != 0 {
			// set the rrs parity if available
			parity = globalRRStorageClass.Parity
		} else {
			// else fall back to default value
			parity = defaultRRSParity
		}
	case standardStorageClass, "":
		if globalStandardStorageClass.Parity != 0 {
			// set the standard parity if available
			parity = globalStandardStorageClass.Parity
		}
	}
	// data is always totalDisks - parity
	return totalDisks - parity, parity
}

// Returns per object readQuorum and writeQuorum
// readQuorum is the minimum required disks to read data.
// writeQuorum is the minimum required disks to write data.
func objectQuorumFromMeta(ctx context.Context, xl xlObjects, partsMetaData []xlMetaV1, errs []error) (objectReadQuorum, objectWriteQuorum int, err error) {
	// get the latest updated Metadata and a count of all the latest updated xlMeta(s)
	latestXLMeta, err := getLatestXLMeta(ctx, partsMetaData, errs)

	if err != nil {
		return 0, 0, err
	}

	// Since all the valid erasure code meta updated at the same time are equivalent, pass dataBlocks
	// from latestXLMeta to get the quorum
	return latestXLMeta.Erasure.DataBlocks, latestXLMeta.Erasure.DataBlocks + 1, nil
}
