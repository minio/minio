// Copyright (c) 2015-2024 MinIO, Inc.
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
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/dustin/go-humanize"
	"github.com/minio/minio/internal/config"
	"github.com/minio/minio/internal/logger"
	"github.com/minio/pkg/v3/env"
)

// Standard constants for all storage class
const (
	// Reduced redundancy storage class
	RRS = "REDUCED_REDUNDANCY"
	// Standard storage class
	STANDARD = "STANDARD"
)

// Standard constants for config info storage class
const (
	ClassStandard = "standard"
	ClassRRS      = "rrs"
	Optimize      = "optimize"
	InlineBlock   = "inline_block"

	// Reduced redundancy storage class environment variable
	RRSEnv = "MINIO_STORAGE_CLASS_RRS"
	// Standard storage class environment variable
	StandardEnv = "MINIO_STORAGE_CLASS_STANDARD"
	// Optimize storage class environment variable
	OptimizeEnv = "MINIO_STORAGE_CLASS_OPTIMIZE"
	// Inline block indicates the size of the shard
	// that is considered for inlining, remember this
	// shard value is the value per drive shard it
	// will vary based on the parity that is configured
	// for the STANDARD storage_class.
	// inlining means data and metadata are written
	// together in a single file i.e xl.meta
	InlineBlockEnv = "MINIO_STORAGE_CLASS_INLINE_BLOCK"

	// Supported storage class scheme is EC
	schemePrefix = "EC"

	// Min parity drives
	minParityDrives = 0

	// Default RRS parity is always minimum parity.
	defaultRRSParity = 1
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
			Value: "EC:1",
		},
		config.KV{
			Key:   Optimize,
			Value: "availability",
		},
		config.KV{
			Key:           InlineBlock,
			Value:         "",
			HiddenIfEmpty: true,
		},
	}
)

// StorageClass - holds storage class information
type StorageClass struct {
	Parity int
}

// ConfigLock is a global lock for storage-class config
var ConfigLock sync.RWMutex

// Config storage class configuration
type Config struct {
	Standard    StorageClass `json:"standard"`
	RRS         StorageClass `json:"rrs"`
	Optimize    string       `json:"optimize"`
	inlineBlock int64

	initialized bool
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
		return fmt.Appendf(nil, "%s:%d", schemePrefix, sc.Parity), nil
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
// Supported Storage Class format is "Scheme:Number of parity drives".
// Currently only supported scheme is "EC".
func parseStorageClass(storageClassEnv string) (sc StorageClass, err error) {
	s := strings.Split(storageClassEnv, ":")

	// only two elements allowed in the string - "scheme" and "number of parity drives"
	if len(s) > 2 {
		return StorageClass{}, config.ErrStorageClassValue(nil).Msg("Too many sections in " + storageClassEnv)
	} else if len(s) < 2 {
		return StorageClass{}, config.ErrStorageClassValue(nil).Msg("Too few sections in " + storageClassEnv)
	}

	// only allowed scheme is "EC"
	if s[0] != schemePrefix {
		return StorageClass{}, config.ErrStorageClassValue(nil).Msg("Unsupported scheme " + s[0] + ". Supported scheme is EC")
	}

	// Number of parity drives should be integer
	parityDrives, err := strconv.Atoi(s[1])
	if err != nil {
		return StorageClass{}, config.ErrStorageClassValue(err)
	}
	if parityDrives < 0 {
		return StorageClass{}, config.ErrStorageClassValue(nil).Msg("Unsupported parity value " + s[1] + " provided")
	}
	return StorageClass{
		Parity: parityDrives,
	}, nil
}

// ValidateParity validate standard storage class parity.
func ValidateParity(ssParity, setDriveCount int) error {
	// SS parity drives should be greater than or equal to minParityDrives.
	// Parity below minParityDrives is not supported.
	if ssParity > 0 && ssParity < minParityDrives {
		return fmt.Errorf("parity %d should be greater than or equal to %d",
			ssParity, minParityDrives)
	}

	if ssParity > setDriveCount/2 {
		return fmt.Errorf("parity %d should be less than or equal to %d", ssParity, setDriveCount/2)
	}

	return nil
}

// Validates the parity drives.
func validateParity(ssParity, rrsParity, setDriveCount int) (err error) {
	// SS parity drives should be greater than or equal to minParityDrives.
	// Parity below minParityDrives is not supported.
	if ssParity > 0 && ssParity < minParityDrives {
		return fmt.Errorf("Standard storage class parity %d should be greater than or equal to %d",
			ssParity, minParityDrives)
	}

	// RRS parity drives should be greater than or equal to minParityDrives.
	// Parity below minParityDrives is not supported.
	if rrsParity > 0 && rrsParity < minParityDrives {
		return fmt.Errorf("Reduced redundancy storage class parity %d should be greater than or equal to %d", rrsParity, minParityDrives)
	}

	if setDriveCount > 2 {
		if ssParity > setDriveCount/2 {
			return fmt.Errorf("Standard storage class parity %d should be less than or equal to %d", ssParity, setDriveCount/2)
		}

		if rrsParity > setDriveCount/2 {
			return fmt.Errorf("Reduced redundancy storage class parity %d should be less than or equal to %d", rrsParity, setDriveCount/2)
		}
	}

	if ssParity > 0 && rrsParity > 0 {
		if ssParity < rrsParity {
			return fmt.Errorf("Standard storage class parity drives %d should be greater than or equal to Reduced redundancy storage class parity drives %d", ssParity, rrsParity)
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
//
// -- if input is RRS but RRS is not configured/initialized '-1' parity
//
//	for RRS is assumed, the caller is expected to choose the right parity
//	at that point.
//
// -- if input is STANDARD but STANDARD is not configured/initialized '-1' parity
//
//	is returned, the caller is expected to choose the right parity
//	at that point.
func (sCfg *Config) GetParityForSC(sc string) (parity int) {
	ConfigLock.RLock()
	defer ConfigLock.RUnlock()
	switch strings.TrimSpace(sc) {
	case RRS:
		if !sCfg.initialized {
			return -1
		}
		return sCfg.RRS.Parity
	default:
		if !sCfg.initialized {
			return -1
		}
		return sCfg.Standard.Parity
	}
}

// ShouldInline returns true if the shardSize is worthy of inline
// if versioned is true then we chosen 1/8th inline block size
// to satisfy the same constraints.
func (sCfg *Config) ShouldInline(shardSize int64, versioned bool) bool {
	if shardSize < 0 {
		return false
	}

	ConfigLock.RLock()
	inlineBlock := int64(128 * humanize.KiByte)
	if sCfg.initialized {
		inlineBlock = sCfg.inlineBlock
	}
	ConfigLock.RUnlock()

	if versioned {
		return shardSize <= inlineBlock/8
	}
	return shardSize <= inlineBlock
}

// InlineBlock indicates the size of the block which will be used to inline
// an erasure shard and written along with xl.meta on the drive, on a versioned
// bucket this value is automatically chosen to 1/8th of the this value, make
// sure to put this into consideration when choosing this value.
func (sCfg *Config) InlineBlock() int64 {
	ConfigLock.RLock()
	defer ConfigLock.RUnlock()
	if !sCfg.initialized {
		return 128 * humanize.KiByte
	}
	return sCfg.inlineBlock
}

// CapacityOptimized - returns true if the storage-class is capacity optimized
// meaning we will not use additional parities when drives are offline.
//
// Default is "availability" optimized, unless this is configured.
func (sCfg *Config) CapacityOptimized() bool {
	ConfigLock.RLock()
	defer ConfigLock.RUnlock()
	if !sCfg.initialized {
		return false
	}
	return sCfg.Optimize == "capacity"
}

// AvailabilityOptimized - returns true if the storage-class is availability
// optimized, meaning we will use additional parities when drives are offline
// to retain parity SLA.
//
// Default is "availability" optimized.
func (sCfg *Config) AvailabilityOptimized() bool {
	ConfigLock.RLock()
	defer ConfigLock.RUnlock()
	if !sCfg.initialized {
		return true
	}
	return sCfg.Optimize == "availability" || sCfg.Optimize == ""
}

// Update update storage-class with new config
func (sCfg *Config) Update(newCfg Config) {
	ConfigLock.Lock()
	defer ConfigLock.Unlock()
	sCfg.RRS = newCfg.RRS
	sCfg.Standard = newCfg.Standard
	sCfg.Optimize = newCfg.Optimize
	sCfg.inlineBlock = newCfg.inlineBlock
	sCfg.initialized = true
}

// Enabled returns if storageClass is enabled is enabled.
func Enabled(kvs config.KVS) bool {
	ssc := kvs.Get(ClassStandard)
	rrsc := kvs.Get(ClassRRS)
	return ssc != "" || rrsc != ""
}

// DefaultParityBlocks returns default parity blocks for 'drive' count
func DefaultParityBlocks(drive int) int {
	switch drive {
	case 1:
		return 0
	case 3, 2:
		return 1
	case 4, 5:
		return 2
	case 6, 7:
		return 3
	default:
		return 4
	}
}

// LookupConfig - lookup storage class config and override with valid environment settings if any.
func LookupConfig(kvs config.KVS, setDriveCount int) (cfg Config, err error) {
	cfg = Config{}

	deprecatedKeys := []string{
		"dma",
	}

	if err = config.CheckValidKeys(config.StorageClassSubSys, kvs, DefaultKVS, deprecatedKeys...); err != nil {
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
	} else {
		cfg.Standard.Parity = DefaultParityBlocks(setDriveCount)
	}

	if rrsc != "" {
		cfg.RRS, err = parseStorageClass(rrsc)
		if err != nil {
			return Config{}, err
		}
	} else {
		cfg.RRS.Parity = defaultRRSParity
		if setDriveCount == 1 {
			cfg.RRS.Parity = 0
		}
	}

	// Validation is done after parsing both the storage classes. This is needed because we need one
	// storage class value to deduce the correct value of the other storage class.
	if err = validateParity(cfg.Standard.Parity, cfg.RRS.Parity, setDriveCount); err != nil {
		return Config{}, err
	}

	cfg.Optimize = env.Get(OptimizeEnv, kvs.Get(Optimize))

	inlineBlockStr := env.Get(InlineBlockEnv, kvs.Get(InlineBlock))
	if inlineBlockStr != "" {
		inlineBlock, err := humanize.ParseBytes(inlineBlockStr)
		if err != nil {
			return cfg, err
		}
		if inlineBlock > 128*humanize.KiByte {
			configLogOnceIf(context.Background(), fmt.Errorf("inline block value bigger than recommended max of 128KiB -> %s, performance may degrade for PUT please benchmark the changes", inlineBlockStr), inlineBlockStr)
		}
		cfg.inlineBlock = int64(inlineBlock)
	} else {
		cfg.inlineBlock = 128 * humanize.KiByte
	}

	cfg.initialized = true

	return cfg, nil
}

func configLogOnceIf(ctx context.Context, err error, id string, errKind ...any) {
	logger.LogOnceIf(ctx, "config", err, id, errKind...)
}
