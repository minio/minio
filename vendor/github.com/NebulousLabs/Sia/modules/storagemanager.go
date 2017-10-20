package modules

import (
	"github.com/NebulousLabs/Sia/crypto"
)

const (
	// ContractManagerDir is the standard name used for the directory that
	// contains all files directly related to the contract manager.
	ContractManagerDir = "contractmanager"

	// StorageManagerDir is standard name used for the directory that contains
	// all of the storage manager files.
	StorageManagerDir = "storagemanager"
)

type (
	// StorageFolderMetadata contains metadata about a storage folder that is
	// tracked by the storage folder manager.
	StorageFolderMetadata struct {
		Capacity          uint64 `json:"capacity"`          // bytes
		CapacityRemaining uint64 `json:"capacityremaining"` // bytes
		Index             uint16 `json:"index"`
		Path              string `json:"path"`

		// Below are statistics about the filesystem. FailedReads and
		// FailedWrites are only incremented if the filesystem is returning
		// errors when operations are being performed. A large number of
		// FailedWrites can indicate that more space has been allocated on a
		// drive than is physically available. A high number of failures can
		// also indicate disk trouble.
		FailedReads      uint64 `json:"failedreads"`
		FailedWrites     uint64 `json:"failedwrites"`
		SuccessfulReads  uint64 `json:"successfulreads"`
		SuccessfulWrites uint64 `json:"successfulwrites"`

		// Certain operations on a storage folder can take a long time (Add,
		// Remove, and Resize). The fields below indicate the progress of any
		// long running operations that might be under way in the storage
		// folder. Progress is always reported in bytes.
		ProgressNumerator   uint64
		ProgressDenominator uint64
	}

	// A StorageManager is responsible for managing storage folders and
	// sectors. Sectors are the base unit of storage that gets moved between
	// renters and hosts, and primarily is stored on the hosts.
	StorageManager interface {
		// AddSector will add a sector to the storage manager. If the sector
		// already exists, a virtual sector will be added, meaning that the
		// 'sectorData' will be ignored and no new disk space will be consumed.
		// The expiry height is used to track what height the sector can be
		// safely deleted at, though typically the host will manually delete
		// the sector before the expiry height. The same sector can be added
		// multiple times at different expiry heights, and the storage manager
		// is expected to only store the data once.
		AddSector(sectorRoot crypto.Hash, sectorData []byte) error

		// AddSectorBatch is a performance optimization over AddSector when
		// adding a bunch of virtual sectors. It is necessary because otherwise
		// potentially thousands or even tens-of-thousands of fsync calls would
		// need to be made in serial, which would prevent renters from ever
		// successfully renewing.
		AddSectorBatch(sectorRoots []crypto.Hash) error

		// AddStorageFolder adds a storage folder to the manager. The manager
		// may not check that there is enough space available on-disk to
		// support as much storage as requested, though the manager should
		// gracefully handle running out of storage unexpectedly.
		AddStorageFolder(path string, size uint64) error

		// The storage manager needs to be able to shut down.
		Close() error

		// DeleteSector deletes a sector, meaning that the manager will be
		// unable to upload that sector and be unable to provide a storage
		// proof on that sector. DeleteSector is for removing the data
		// entirely, and will remove instances of the sector appearing at all
		// heights. The primary purpose of DeleteSector is to comply with legal
		// requests to remove data.
		DeleteSector(sectorRoot crypto.Hash) error

		// ReadSector will read a sector from the storage manager, returning the
		// bytes that match the input sector root.
		ReadSector(sectorRoot crypto.Hash) ([]byte, error)

		// RemoveSector will remove a sector from the storage manager. The
		// height at which the sector expires should be provided, so that the
		// auto-expiry information for that sector can be properly updated.
		RemoveSector(sectorRoot crypto.Hash) error

		// RemoveSectorBatch is a non-ACID performance optimization to remove a
		// ton of sectors from the storage manager all at once. This is
		// necessary when clearing out an entire contract from the host.
		RemoveSectorBatch(sectorRoots []crypto.Hash) error

		// RemoveStorageFolder will remove a storage folder from the manager.
		// All storage on the folder will be moved to other storage folders,
		// meaning that no data will be lost. If the manager is unable to save
		// data, an error will be returned and the operation will be stopped. If
		// the force flag is set to true, errors will be ignored and the remove
		// operation will be completed, meaning that data will be lost.
		RemoveStorageFolder(index uint16, force bool) error

		// ResetStorageFolderHealth will reset the health statistics on a
		// storage folder.
		ResetStorageFolderHealth(index uint16) error

		// ResizeStorageFolder will grow or shrink a storage folder in the
		// manager. The manager may not check that there is enough space
		// on-disk to support growing the storage folder, but should gracefully
		// handle running out of space unexpectedly. When shrinking a storage
		// folder, any data in the folder that needs to be moved will be placed
		// into other storage folders, meaning that no data will be lost. If
		// the manager is unable to migrate the data, an error will be returned
		// and the operation will be stopped. If the force flag is set to true,
		// errors will be ignored and the resize operation completed, meaning
		// that data will be lost.
		ResizeStorageFolder(index uint16, newSize uint64, force bool) error

		// StorageFolders will return a list of storage folders tracked by the
		// manager.
		StorageFolders() []StorageFolderMetadata
	}
)
