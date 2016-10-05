/*
 * Minio Cloud Storage, (C) 2016 Minio, Inc.
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
	"errors"
	"fmt"
	"io"
	"net"
	"net/rpc"
	"testing"
)

// Tests storage error transformation.
func TestStorageErr(t *testing.T) {
	unknownErr := errors.New("Unknown error")
	testCases := []struct {
		expectedErr error
		err         error
	}{
		{
			expectedErr: nil,
			err:         nil,
		},
		{
			expectedErr: io.EOF,
			err:         fmt.Errorf("%s", io.EOF.Error()),
		},
		{
			expectedErr: io.ErrUnexpectedEOF,
			err:         fmt.Errorf("%s", io.ErrUnexpectedEOF.Error()),
		},
		{
			expectedErr: errDiskNotFound,
			err:         &net.OpError{},
		},
		{
			expectedErr: errDiskNotFound,
			err:         rpc.ErrShutdown,
		},
		{
			expectedErr: errUnexpected,
			err:         fmt.Errorf("%s", errUnexpected.Error()),
		},
		{
			expectedErr: errDiskFull,
			err:         fmt.Errorf("%s", errDiskFull.Error()),
		},
		{
			expectedErr: errVolumeNotFound,
			err:         fmt.Errorf("%s", errVolumeNotFound.Error()),
		},
		{
			expectedErr: errVolumeExists,
			err:         fmt.Errorf("%s", errVolumeExists.Error()),
		},
		{
			expectedErr: errFileNotFound,
			err:         fmt.Errorf("%s", errFileNotFound.Error()),
		},
		{
			expectedErr: errFileAccessDenied,
			err:         fmt.Errorf("%s", errFileAccessDenied.Error()),
		},
		{
			expectedErr: errIsNotRegular,
			err:         fmt.Errorf("%s", errIsNotRegular.Error()),
		},
		{
			expectedErr: errVolumeNotEmpty,
			err:         fmt.Errorf("%s", errVolumeNotEmpty.Error()),
		},
		{
			expectedErr: errVolumeAccessDenied,
			err:         fmt.Errorf("%s", errVolumeAccessDenied.Error()),
		},
		{
			expectedErr: errCorruptedFormat,
			err:         fmt.Errorf("%s", errCorruptedFormat.Error()),
		},
		{
			expectedErr: errUnformattedDisk,
			err:         fmt.Errorf("%s", errUnformattedDisk.Error()),
		},
		{
			expectedErr: errFileNameTooLong,
			err:         fmt.Errorf("%s", errFileNameTooLong.Error()),
		},
		{
			expectedErr: unknownErr,
			err:         unknownErr,
		},
	}
	for i, testCase := range testCases {
		resultErr := toStorageErr(testCase.err)
		if testCase.expectedErr != resultErr {
			t.Errorf("Test %d: Expected %s, got %s", i+1, testCase.expectedErr, resultErr)
		}
	}
}
