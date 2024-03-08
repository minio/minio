// Copyright (c) 2015-2023 MinIO, Inc.
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

package cmd

import (
	"context"
	"errors"
)

// errMaxVersionsExceeded return error beyond 10000 (default) versions per object
var errMaxVersionsExceeded = StorageErr("maximum versions exceeded, please delete few versions to proceed")

// errUnexpected - unexpected error, requires manual intervention.
var errUnexpected = StorageErr("unexpected error, please report this issue at https://github.com/minio/minio/issues")

// errCorruptedFormat - corrupted format.
var errCorruptedFormat = StorageErr("corrupted format")

// errCorruptedBackend - corrupted backend.
var errCorruptedBackend = StorageErr("corrupted backend")

// errUnformattedDisk - unformatted disk found.
var errUnformattedDisk = StorageErr("unformatted drive found")

// errInconsistentDisk - inconsistent disk found.
var errInconsistentDisk = StorageErr("inconsistent drive found")

// errUnsupporteDisk - when disk does not support O_DIRECT flag.
var errUnsupportedDisk = StorageErr("drive does not support O_DIRECT")

// errDiskFull - cannot create volume or files when disk is full.
var errDiskFull = StorageErr("drive path full")

// errDiskNotDir - cannot use storage disk if its not a directory
var errDiskNotDir = StorageErr("drive is not directory or mountpoint")

// errDiskNotFound - cannot find the underlying configured disk anymore.
var errDiskNotFound = StorageErr("drive not found")

// errDiskOngoingReq - indicates if the disk has an on-going request in progress.
var errDiskOngoingReq = StorageErr("drive still did not complete the request")

// errDriveIsRoot - cannot use the disk since its a root disk.
var errDriveIsRoot = StorageErr("drive is part of root drive, will not be used")

// errFaultyRemoteDisk - remote disk is faulty.
var errFaultyRemoteDisk = StorageErr("remote drive is faulty")

// errFaultyDisk - disk is faulty.
var errFaultyDisk = StorageErr("drive is faulty")

// errDiskAccessDenied - we don't have write permissions on disk.
var errDiskAccessDenied = StorageErr("drive access denied")

// errFileNotFound - cannot find the file.
var errFileNotFound = StorageErr("file not found")

// errFileNotFound - cannot find requested file version.
var errFileVersionNotFound = StorageErr("file version not found")

// errTooManyOpenFiles - too many open files.
var errTooManyOpenFiles = StorageErr("too many open files, please increase 'ulimit -n'")

// errFileNameTooLong - given file name is too long than supported length.
var errFileNameTooLong = StorageErr("file name too long")

// errVolumeExists - cannot create same volume again.
var errVolumeExists = StorageErr("volume already exists")

// errIsNotRegular - not of regular file type.
var errIsNotRegular = StorageErr("not of regular file type")

// errPathNotFound - cannot find the path.
var errPathNotFound = StorageErr("path not found")

// errVolumeNotFound - cannot find the volume.
var errVolumeNotFound = StorageErr("volume not found")

// errVolumeNotEmpty - volume not empty.
var errVolumeNotEmpty = StorageErr("volume is not empty")

// errVolumeAccessDenied - cannot access volume, insufficient permissions.
var errVolumeAccessDenied = StorageErr("volume access denied")

// errFileAccessDenied - cannot access file, insufficient permissions.
var errFileAccessDenied = StorageErr("file access denied")

// errFileCorrupt - file has an unexpected size, or is not readable
var errFileCorrupt = StorageErr("file is corrupted")

// errBitrotHashAlgoInvalid - the algo for bit-rot hash
// verification is empty or invalid.
var errBitrotHashAlgoInvalid = StorageErr("bit-rot hash algorithm is invalid")

// errCrossDeviceLink - rename across devices not allowed.
var errCrossDeviceLink = StorageErr("Rename across devices not allowed, please fix your backend configuration")

// errLessData - returned when less data available than what was requested.
var errLessData = StorageErr("less data available than what was requested")

// errMoreData = returned when more data was sent by the caller than what it was supposed to.
var errMoreData = StorageErr("more data was sent than what was advertised")

// indicates readDirFn to return without further applying the fn()
var errDoneForNow = errors.New("done for now")

// errSkipFile returned by the fn() for readDirFn() when it needs
// to proceed to next entry.
var errSkipFile = errors.New("skip this file")

var errIgnoreFileContrib = errors.New("ignore this file's contribution toward data-usage")

// errXLBackend XL drive mode requires fresh deployment.
var errXLBackend = errors.New("XL backend requires fresh drive")

// StorageErr represents error generated by xlStorage call.
type StorageErr string

func (h StorageErr) Error() string {
	return string(h)
}

// Collection of basic errors.
var baseErrs = []error{
	errDiskNotFound,
	errFaultyDisk,
	errFaultyRemoteDisk,
}

var baseIgnoredErrs = baseErrs

// Is a one place function which converts all os.PathError
// into a more FS object layer friendly form, converts
// known errors into their typed form for top level
// interpretation.
func osErrToFileErr(err error) error {
	if err == nil {
		return nil
	}
	if osIsNotExist(err) {
		return errFileNotFound
	}
	if osIsPermission(err) {
		return errFileAccessDenied
	}
	if isSysErrNotDir(err) || isSysErrIsDir(err) {
		return errFileNotFound
	}
	if isSysErrPathNotFound(err) {
		return errFileNotFound
	}
	if isSysErrTooManyFiles(err) {
		return errTooManyOpenFiles
	}
	if isSysErrHandleInvalid(err) {
		return errFileNotFound
	}
	if isSysErrIO(err) {
		return errFaultyDisk
	}
	if isSysErrInvalidArg(err) {
		storageLogIf(context.Background(), err)
		// For some odd calls with O_DIRECT reads
		// filesystems can return EINVAL, handle
		// these as FileNotFound instead.
		return errFileNotFound
	}
	if isSysErrNoSpace(err) {
		return errDiskFull
	}
	return err
}
