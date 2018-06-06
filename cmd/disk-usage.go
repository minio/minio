/*
 * Minio Cloud Storage, (C) 2018 Minio, Inc.
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
)

// getDiskUsage walks the file tree rooted at root, calling usageFn
// for each file or directory in the tree, including root.
func getDiskUsage(ctx context.Context, root string, usageFn usageFunc) error {
	return walk(ctx, root+slashSeparator, usageFn)
}

type usageFunc func(ctx context.Context, entry string) error

// walk recursively descends path, calling walkFn.
func walk(ctx context.Context, path string, usageFn usageFunc) error {
	if err := usageFn(ctx, path); err != nil {
		return err
	}

	if !hasSuffix(path, slashSeparator) {
		return nil
	}

	entries, err := readDir(path)
	if err != nil {
		return usageFn(ctx, path)
	}

	for _, entry := range entries {
		fname := pathJoin(path, entry)
		if err = walk(ctx, fname, usageFn); err != nil {
			return err
		}
	}

	return nil
}
