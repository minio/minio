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
	"slices"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestParseBatchJobExpire(t *testing.T) {
	expireYaml := `
expire: # Expire objects that match a condition
  apiVersion: v1
  bucket: mybucket # Bucket where this batch job will expire matching objects from
  prefix: myprefix # (Optional) Prefix under which this job will expire objects matching the rules below.
  rules:
    - type: object  # regular objects with zero or more older versions
      name: NAME # match object names that satisfy the wildcard expression.
      olderThan: 7d10h # match objects older than this value
      createdBefore: "2006-01-02T15:04:05.00Z" # match objects created before "date"
      tags:
        - key: name
          value: pick* # match objects with tag 'name', all values starting with 'pick'
      metadata:
        - key: content-type
          value: image/* # match objects with 'content-type', all values starting with 'image/'
      size:
        lessThan: "10MiB" # match objects with size less than this value (e.g. 10MiB)
        greaterThan: 1MiB # match objects with size greater than this value (e.g. 1MiB)
      purge:
          # retainVersions: 0 # (default) delete all versions of the object. This option is the fastest.
          # retainVersions: 5 # keep the latest 5 versions of the object.
  
    - type: deleted # objects with delete marker as their latest version
      name: NAME # match object names that satisfy the wildcard expression.
      olderThan: 10h # match objects older than this value (e.g. 7d10h31s)
      createdBefore: "2006-01-02T15:04:05.00Z" # match objects created before "date"
      purge:
          # retainVersions: 0 # (default) delete all versions of the object. This option is the fastest.
          # retainVersions: 5 # keep the latest 5 versions of the object including delete markers.

  notify:
    endpoint: https://notify.endpoint # notification endpoint to receive job completion status
    token: Bearer xxxxx # optional authentication token for the notification endpoint
  
  retry:
    attempts: 10 # number of retries for the job before giving up
    delay: 500ms # least amount of delay between each retry
`
	var job BatchJobRequest
	err := yaml.Unmarshal([]byte(expireYaml), &job)
	if err != nil {
		t.Fatal("Failed to parse batch-job-expire yaml", err)
	}
	if !slices.Equal(job.Expire.Prefix.F(), []string{"myprefix"}) {
		t.Fatal("Failed to parse batch-job-expire yaml")
	}

	multiPrefixExpireYaml := `
expire: # Expire objects that match a condition
  apiVersion: v1
  bucket: mybucket # Bucket where this batch job will expire matching objects from
  prefix: # (Optional) Prefix under which this job will expire objects matching the rules below.
    - myprefix
    - myprefix1
  rules:
    - type: object  # regular objects with zero or more older versions
      name: NAME # match object names that satisfy the wildcard expression.
      olderThan: 7d10h # match objects older than this value
      createdBefore: "2006-01-02T15:04:05.00Z" # match objects created before "date"
      tags:
        - key: name
          value: pick* # match objects with tag 'name', all values starting with 'pick'
      metadata:
        - key: content-type
          value: image/* # match objects with 'content-type', all values starting with 'image/'
      size:
        lessThan: "10MiB" # match objects with size less than this value (e.g. 10MiB)
        greaterThan: 1MiB # match objects with size greater than this value (e.g. 1MiB)
      purge:
          # retainVersions: 0 # (default) delete all versions of the object. This option is the fastest.
          # retainVersions: 5 # keep the latest 5 versions of the object.
  
    - type: deleted # objects with delete marker as their latest version
      name: NAME # match object names that satisfy the wildcard expression.
      olderThan: 10h # match objects older than this value (e.g. 7d10h31s)
      createdBefore: "2006-01-02T15:04:05.00Z" # match objects created before "date"
      purge:
          # retainVersions: 0 # (default) delete all versions of the object. This option is the fastest.
          # retainVersions: 5 # keep the latest 5 versions of the object including delete markers.

  notify:
    endpoint: https://notify.endpoint # notification endpoint to receive job completion status
    token: Bearer xxxxx # optional authentication token for the notification endpoint
  
  retry:
    attempts: 10 # number of retries for the job before giving up
    delay: 500ms # least amount of delay between each retry
`
	var multiPrefixJob BatchJobRequest
	err = yaml.Unmarshal([]byte(multiPrefixExpireYaml), &multiPrefixJob)
	if err != nil {
		t.Fatal("Failed to parse batch-job-expire yaml", err)
	}
	if !slices.Equal(multiPrefixJob.Expire.Prefix.F(), []string{"myprefix", "myprefix1"}) {
		t.Fatal("Failed to parse batch-job-expire yaml")
	}
}
