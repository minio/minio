// Copyright (c) 2015-2021 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package storageclass

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/minio/minio/internal/config"
	"github.com/minio/pkg/env"
)

// Standard constants for all storage class
const (
	// Reduced redundancy storage class
	RRS = "REDUCED_REDUNDANCY"
	// Standard storage class
	STANDARD = "STANDARD"
	// DMA storage class
	DMA = "DMA"

	// Valid values are "write" and "read+write"
	DMAWrite     = "write"
	DMAReadWrite = "read+write"
)

// Standard constats for config info storage class
const (
	ClassStandard = "standard"
	ClassRRS      = "rrs"
	ClassDMA      = "dma"

	// Reduced redundancy storage class environment variable
	RRSEnv = "MINIO_STORAGE_CLASS_RRS"
	// Standard storage class environment variable
	StandardEnv = "MINIO_STORAGE_CLASS_STANDARD"
	// DMA storage class environment variable
	DMAEnv = "MINIO_STORAGE_CLASS_DMA"

	// Supported storage class scheme is EC
	schemePrefix = "EC"

	// Min parity disks
	minParityDisks = 2

	// Default RRS parity is always minimum parity.
	defaultRRSParity = minParityDisks

	// Default DMA value
	defaultDMA = DMAReadWrite
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
		config.KV{
			Key:   ClassDMA,
			Value: defaultDMA,
		},
	}
)

// StorageClass - holds storage class information
type StorageClass struct {
	Parity int
}

// ConfigLock is a global lock for storage-class config
var ConfigLock = sync.RWMutex{}

// Config storage class configuration
type Config struct {
	Standard StorageClass `json:"standard"`
	RRS      StorageClass `json:"rrs"`
	DMA      string       `json:"dma"`
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
	return []byte{}, nil
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

// ValidateParity validate standard storage class parity.
func ValidateParity(ssParity, setDriveCount int) error {
	// SS parity disks should be greater than or equal to minParityDisks.
	// Parity below minParityDisks is not supported.
	if ssParity > 0 && ssParity < minParityDisks {
		return fmt.Errorf("Standard storage class parity %d should be greater than or equal to %d",
			ssParity, minParityDisks)
	}

	if ssParity > setDriveCount/2 {
		return fmt.Errorf("Standard storage class parity %d should be less than or equal to %d", ssParity, setDriveCount/2)
	}

	return nil
}

// Validates the parity disks.
func validateParity(ssParity, rrsParity, setDriveCount int) (err error) {
	// SS parity disks should be greater than or equal to minParityDisks.
	// Parity below minParityDisks is not supported.
	if ssParity > 0 && ssParity < minParityDisks {
		return fmt.Errorf("Standard storage class parity %d should be greater than or equal to %d",
			ssParity, minParityDisks)
	}

	// RRS parity disks should be greater than or equal to minParityDisks.
	// Parity below minParityDisks is not supported.
	if rrsParity > 0 && rrsParity < minParityDisks {
		return fmt.Errorf("Reduced redundancy storage class parity %d should be greater than or equal to %d", rrsParity, minParityDisks)
	}

	if ssParity > setDriveCount/2 {
		return fmt.Errorf("Standard storage class parity %d should be less than or equal to %d", ssParity, setDriveCount/2)
	}

	if rrsParity > setDriveCount/2 {
		return fmt.Errorf("Reduced redundancy storage class parity %d should be less than  or equal to %d", rrsParity, setDriveCount/2)
	}

	if ssParity > 0 && rrsParity > 0 {
		if ssParity > 0 && ssParity < rrsParity {
			return fmt.Errorf("Standard storage class parity disks %d should be greater than or equal to Reduced redundancy storage class parity disks %d", ssParity, rrsParity)
		}
	}
	return nil
}

// GetParityForSC - Returns the data and parity drive count based on storage class
// If storage class is set using the env vars MINIO_STORAGE_CLASS_RRS and
// MINIO_STORAGE_CLASS_STANDARD or server config fields corresponding values are
// returned.
//
// -- if input storage class is empty then standard is assumed
// -- if input is RRS but RRS is not configured default '2' parity
//    for RRS is assumed
// -- if input is STANDARD but STANDARD is not configured '0' parity
//    is returned, the caller is expected to choose the right parity
//    at that point.
func (sCfg Config) GetParityForSC(sc string) (parity int) {
	ConfigLock.RLock()
	defer ConfigLock.RUnlock()
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

// Update update storage-class with new config
func (sCfg Config) Update(newCfg Config) {
	ConfigLock.Lock()
	defer ConfigLock.Unlock()
	sCfg.RRS = newCfg.RRS
	sCfg.DMA = newCfg.DMA
	sCfg.Standard = newCfg.Standard
}

// GetDMA - returns DMA configuration.
func (sCfg Config) GetDMA() string {
	ConfigLock.RLock()
	defer ConfigLock.RUnlock()
	return sCfg.DMA
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

	if err = config.CheckValidKeys(config.StorageClassSubSys, kvs, DefaultKVS); err != nil {
		return Config{}, err
	}

	ssc := env.Get(StandardEnv, kvs.Get(ClassStandard))
	rrsc := env.Get(RRSEnv, kvs.Get(ClassRRS))
	dma := env.Get(DMAEnv, kvs.Get(ClassDMA))
	// Check for environment variables and parse into storageClass struct
	if ssc != "" {
		cfg.Standard, err = parseStorageClass(ssc)
		if err != nil {
			return Config{}, err
		}
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

	if dma == "" {
		dma = defaultDMA
	}
	if dma != DMAReadWrite && dma != DMAWrite {
		return Config{}, errors.New(`valid dma values are "read-write" and "write"`)
	}
	cfg.DMA = dma

	// Validation is done after parsing both the storage classes. This is needed because we need one
	// storage class value to deduce the correct value of the other storage class.
	if err = validateParity(cfg.Standard.Parity, cfg.RRS.Parity, setDriveCount); err != nil {
		return Config{}, err
	}

	return cfg, nil
}
