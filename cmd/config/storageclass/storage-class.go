/*
 * MinIO Cloud Storage, (C) 2019 MinIO, Inc.
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

package storageclass

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/minio/minio/cmd/config"
	"github.com/minio/minio/pkg/env"
)

// Standard constants for all storage class
const (
	// Reduced redundancy storage class
	RRS = "REDUCED_REDUNDANCY"
	// Standard storage class
	STANDARD = "STANDARD"
)

// Standard constats for config info storage class
const (
	ClassStandard = "standard"
	ClassRRS      = "rrs"

	// Reduced redundancy storage class environment variable
	RRSEnv = "MINIO_STORAGE_CLASS_RRS"
	// Standard storage class environment variable
	StandardEnv = "MINIO_STORAGE_CLASS_STANDARD"

	// Supported storage class scheme is EC
	schemePrefix = "EC"

	// Min parity disks
	minParityDisks = 2

	// Default RRS parity is always minimum parity.
	defaultRRSParity = minParityDisks
)

// DefaultKVS - default storage class config
var (
	DefaultKVS = config.KVS{
		config.KV{
			Key:   ClassStandard,
			Value: "",
		},
		config.KV{
			Key:   ClassRRS,
			Value: "EC:2",
		},
	}
)

// StorageClass - holds storage class information
type StorageClass struct {
	Parity int
}

// Config storage class configuration
type Config struct {
	Standard StorageClass `json:"standard"`
	RRS      StorageClass `json:"rrs"`
}

// UnmarshalJSON - Validate SS and RRS parity when unmarshalling JSON.
func (sCfg *Config) UnmarshalJSON(data []byte) error {
	type Alias Config
	aux := &struct {
		*Alias
	}{
		Alias: (*Alias)(sCfg),
	}
	return json.Unmarshal(data, &aux)
}

// IsValid - returns true if input string is a valid
// storage class kind supported.
func IsValid(sc string) bool {
	return sc == RRS || sc == STANDARD
}

// UnmarshalText unmarshals storage class from its textual form into
// storageClass structure.
func (sc *StorageClass) UnmarshalText(b []byte) error {
	scStr := string(b)
	if scStr == "" {
		return nil
	}
	s, err := parseStorageClass(scStr)
	if err != nil {
		return err
	}
	sc.Parity = s.Parity
	return nil
}

// MarshalText - marshals storage class string.
func (sc *StorageClass) MarshalText() ([]byte, error) {
	if sc.Parity != 0 {
		return []byte(fmt.Sprintf("%s:%d", schemePrefix, sc.Parity)), nil
	}
	return []byte(""), nil
}

func (sc *StorageClass) String() string {
	if sc.Parity != 0 {
		return fmt.Sprintf("%s:%d", schemePrefix, sc.Parity)
	}
	return ""
}

// Parses given storageClassEnv and returns a storageClass structure.
// Supported Storage Class format is "Scheme:Number of parity disks".
// Currently only supported scheme is "EC".
func parseStorageClass(storageClassEnv string) (sc StorageClass, err error) {
	s := strings.Split(storageClassEnv, ":")

	// only two elements allowed in the string - "scheme" and "number of parity disks"
	if len(s) > 2 {
		return StorageClass{}, config.ErrStorageClassValue(nil).Msg("Too many sections in " + storageClassEnv)
	} else if len(s) < 2 {
		return StorageClass{}, config.ErrStorageClassValue(nil).Msg("Too few sections in " + storageClassEnv)
	}

	// only allowed scheme is "EC"
	if s[0] != schemePrefix {
		return StorageClass{}, config.ErrStorageClassValue(nil).Msg("Unsupported scheme " + s[0] + ". Supported scheme is EC")
	}

	// Number of parity disks should be integer
	parityDisks, err := strconv.Atoi(s[1])
	if err != nil {
		return StorageClass{}, config.ErrStorageClassValue(err)
	}

	return StorageClass{
		Parity: parityDisks,
	}, nil
}

// Validates the parity disks.
func validateParity(ssParity, rrsParity, setDriveCount int) (err error) {
	if ssParity == 0 && rrsParity == 0 {
		return nil
	}

	// SS parity disks should be greater than or equal to minParityDisks.
	// Parity below minParityDisks is not supported.
	if ssParity < minParityDisks {
		return fmt.Errorf("Standard storage class parity %d should be greater than or equal to %d",
			ssParity, minParityDisks)
	}

	// RRS parity disks should be greater than or equal to minParityDisks.
	// Parity below minParityDisks is not supported.
	if rrsParity < minParityDisks {
		return fmt.Errorf("Reduced redundancy storage class parity %d should be greater than or equal to %d", rrsParity, minParityDisks)
	}

	if ssParity > setDriveCount/2 {
		return fmt.Errorf("Standard storage class parity %d should be less than or equal to %d", ssParity, setDriveCount/2)
	}

	if rrsParity > setDriveCount/2 {
		return fmt.Errorf("Reduced redundancy storage class parity %d should be less than  or equal to %d", rrsParity, setDriveCount/2)
	}

	if ssParity > 0 && rrsParity > 0 {
		if ssParity < rrsParity {
			return fmt.Errorf("Standard storage class parity disks %d should be greater than or equal to Reduced redundancy storage class parity disks %d", ssParity, rrsParity)
		}
	}
	return nil
}

// GetParityForSC - Returns the data and parity drive count based on storage class
// If storage class is set using the env vars MINIO_STORAGE_CLASS_RRS and MINIO_STORAGE_CLASS_STANDARD
// or config.json fields
// -- corresponding values are returned
// If storage class is not set during startup, default values are returned
// -- Default for Reduced Redundancy Storage class is, parity = 2 and data = N-Parity
// -- Default for Standard Storage class is, parity = N/2, data = N/2
// If storage class is empty
// -- standard storage class is assumed and corresponding data and parity is returned
func (sCfg Config) GetParityForSC(sc string) (parity int) {
	switch strings.TrimSpace(sc) {
	case RRS:
		// set the rrs parity if available
		if sCfg.RRS.Parity == 0 {
			return defaultRRSParity
		}
		return sCfg.RRS.Parity
	default:
		return sCfg.Standard.Parity
	}
}

// Enabled returns if etcd is enabled.
func Enabled(kvs config.KVS) bool {
	ssc := kvs.Get(ClassStandard)
	rrsc := kvs.Get(ClassRRS)
	return ssc != "" || rrsc != ""
}

// LookupConfig - lookup storage class config and override with valid environment settings if any.
func LookupConfig(kvs config.KVS, setDriveCount int) (cfg Config, err error) {
	cfg = Config{}
	cfg.Standard.Parity = setDriveCount / 2
	cfg.RRS.Parity = defaultRRSParity

	if err = config.CheckValidKeys(config.StorageClassSubSys, kvs, DefaultKVS); err != nil {
		return Config{}, err
	}

	ssc := env.Get(StandardEnv, kvs.Get(ClassStandard))
	rrsc := env.Get(RRSEnv, kvs.Get(ClassRRS))
	// Check for environment variables and parse into storageClass struct
	if ssc != "" {
		cfg.Standard, err = parseStorageClass(ssc)
		if err != nil {
			return Config{}, err
		}
	}
	if cfg.Standard.Parity == 0 {
		cfg.Standard.Parity = setDriveCount / 2
	}

	if rrsc != "" {
		cfg.RRS, err = parseStorageClass(rrsc)
		if err != nil {
			return Config{}, err
		}
	}
	if cfg.RRS.Parity == 0 {
		cfg.RRS.Parity = defaultRRSParity
	}

	// Validation is done after parsing both the storage classes. This is needed because we need one
	// storage class value to deduce the correct value of the other storage class.
	if err = validateParity(cfg.Standard.Parity, cfg.RRS.Parity, setDriveCount); err != nil {
		return Config{}, err
	}

	return cfg, nil
}
