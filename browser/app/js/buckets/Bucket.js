/*
 * Minio Cloud Storage (C) 2018 Minio, Inc.
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

import React from "react"
import classNames from "classnames"
import BucketDropdown from "./BucketDropdown"

export const Bucket = ({ bucket, isActive, selectBucket }) => {
  return (
    <li
      className={classNames({
        active: isActive
      })}
      onClick={e => {
        e.preventDefault()
        selectBucket(bucket)
      }}
    >
      <a
        href=""
        className={classNames({
          "fesli-loading": false
        })}
      >
        {bucket}
      </a>
      <BucketDropdown bucket={bucket}/>
    </li>
  )
}

export default Bucket
