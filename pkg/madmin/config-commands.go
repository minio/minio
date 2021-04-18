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

package madmin

import (
	"bytes"
	"context"
	"io"
	"net/http"
)

// GetConfig - returns the config.json of a minio setup, incoming data is encrypted.
func (adm *AdminClient) GetConfig(ctx context.Context) ([]byte, error) {
	// Execute GET on /minio/admin/v3/config to get config of a setup.
	resp, err := adm.executeMethod(ctx,
		http.MethodGet,
		requestData{relPath: adminAPIPrefix + "/config"})
	defer closeResponse(resp)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		return nil, httpRespToErrorResponse(resp)
	}

	return DecryptData(adm.getSecretKey(), resp.Body)
}

// SetConfig - set config supplied as config.json for the setup.
func (adm *AdminClient) SetConfig(ctx context.Context, config io.Reader) (err error) {
	const maxConfigJSONSize = 256 * 1024 // 256KiB

	// Read configuration bytes
	configBuf := make([]byte, maxConfigJSONSize+1)
	n, err := io.ReadFull(config, configBuf)
	if err == nil {
		return bytes.ErrTooLarge
	}
	if err != io.ErrUnexpectedEOF {
		return err
	}
	configBytes := configBuf[:n]
	econfigBytes, err := EncryptData(adm.getSecretKey(), configBytes)
	if err != nil {
		return err
	}

	reqData := requestData{
		relPath: adminAPIPrefix + "/config",
		content: econfigBytes,
	}

	// Execute PUT on /minio/admin/v3/config to set config.
	resp, err := adm.executeMethod(ctx, http.MethodPut, reqData)

	defer closeResponse(resp)
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK {
		return httpRespToErrorResponse(resp)
	}

	return nil
}
