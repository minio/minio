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

import web from "../web"
import history from "../history"
import * as alertActions from "../alert/actions"
import * as objectsActions from "../objects/actions"

export const SET_LIST = "buckets/SET_LIST"
export const ADD = "buckets/ADD"
export const SET_FILTER = "buckets/SET_FILTER"
export const SET_CURRENT_BUCKET = "buckets/SET_CURRENT_BUCKET"
export const SHOW_MAKE_BUCKET_MODAL = "buckets/SHOW_MAKE_BUCKET_MODAL"

export const fetchBuckets = () => {
  return function(dispatch) {
    return web.ListBuckets().then(res => {
      const buckets = res.buckets ? res.buckets.map(bucket => bucket.name) : []
      dispatch(setList(buckets))
      if (buckets.length > 0) {
        dispatch(selectBucket(buckets[0]))
      }
    })
  }
}

export const setList = buckets => {
  return {
    type: SET_LIST,
    buckets
  }
}

export const setFilter = filter => {
  return {
    type: SET_FILTER,
    filter
  }
}

export const selectBucket = bucket => {
  return function(dispatch) {
    dispatch(setCurrentBucket(bucket))
    dispatch(objectsActions.selectPrefix(""))
    history.push(`/${bucket}`)
  }
}

export const setCurrentBucket = bucket => {
  return {
    type: SET_CURRENT_BUCKET,
    bucket
  }
}

export const makeBucket = bucket => {
  return function(dispatch) {
    return web
      .MakeBucket({
        bucketName: bucket
      })
      .then(() => {
        dispatch(addBucket(bucket))
        dispatch(selectBucket(bucket))
      })
      .catch(err =>
        dispatch(
          alertActions.set({
            type: "danger",
            message: err.message
          })
        )
      )
  }
}

export const addBucket = bucket => ({
  type: ADD,
  bucket
})

export const showMakeBucketModal = () => ({
  type: SHOW_MAKE_BUCKET_MODAL,
  show: true
})

export const hideMakeBucketModal = () => ({
  type: SHOW_MAKE_BUCKET_MODAL,
  show: false
})
