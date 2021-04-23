/*
 * Copyright (c) 2015-2021 MinIO, Inc.
 *
 * This file is part of MinIO Object Storage stack
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
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
