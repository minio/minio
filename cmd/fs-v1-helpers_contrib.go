/*
 * MinIO Object Storage (c) 2021 MinIO, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
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

	"github.com/minio/minio/cmd/logger"
)

// Renames source path to destination path, creates all the
// missing parents if they don't exist.
func fsRenameFile(ctx context.Context, sourcePath, destPath string) error {
	if err := checkPathLength(sourcePath); err != nil {
		logger.LogIf(ctx, err)
		return err
	}
	if err := checkPathLength(destPath); err != nil {
		logger.LogIf(ctx, err)
		return err
	}

	if err := renameAll(sourcePath, destPath); err != nil {
		logger.LogIf(ctx, err)
		return err
	}

	return nil
}
