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

import * as actionsBuckets from "./actions"

const removeBucket = (list, action) => {
  const idx = list.findIndex(bucket => bucket === action.bucket)
  if (idx == -1) {
    return list
  }
  return [...list.slice(0, idx), ...list.slice(idx + 1)]
}

export default (
  state = {
    list: [],
    filter: "",
    currentBucket: "",
    showMakeBucketModal: false,
    policies: [],
    showBucketPolicy: false
  },
  action
) => {
  switch (action.type) {
    case actionsBuckets.SET_LIST:
      return {
        ...state,
        list: action.buckets
      }
    case actionsBuckets.ADD:
      return {
        ...state,
        list: [action.bucket, ...state.list]
      }
    case actionsBuckets.REMOVE:
      return {
        ...state,
        list: removeBucket(state.list, action),
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
    case actionsBuckets.SHOW_MAKE_BUCKET_MODAL:
      return {
        ...state,
        showMakeBucketModal: action.show
      }
    case actionsBuckets.SET_POLICIES:
      return {
        ...state,
        policies: action.policies
      }
    case actionsBuckets.SHOW_BUCKET_POLICY:
      return {
        ...state,
        showBucketPolicy: action.show
      }
    default:
      return state
  }
}
