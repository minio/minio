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

import * as actionsBuckets from "../actions/buckets"

export default (
  state = { list: [], filter: "", currentBucket: "" },
  action
) => {
  switch (action.type) {
    case actionsBuckets.SET_LIST:
      return {
        ...state,
        list: action.buckets
      }
    case actionsBuckets.SET_FILTER:
      return {
        ...state,
        filter: action.filter
      }
    case actionsBuckets.SET_CURRENT_BUCKET:
      return {
        ...state,
        currentBucket: action.bucket
      }
    default:
      return state
  }
}
