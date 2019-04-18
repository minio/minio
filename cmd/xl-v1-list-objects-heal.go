/*
 * MinIO Cloud Storage, (C) 2016, 2017, 2018 MinIO, Inc.
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

import "context"

// This is not implemented/needed anymore, look for xl-sets.ListBucketHeal()
func (xl xlObjects) ListBucketsHeal(ctx context.Context) ([]BucketInfo, error) {
	return nil, nil
}

// This is not implemented/needed anymore, look for xl-sets.HealObjects()
func (xl xlObjects) HealObjects(ctx context.Context, bucket, prefix string, healFn func(string, string) error) (e error) {
	return nil
}
