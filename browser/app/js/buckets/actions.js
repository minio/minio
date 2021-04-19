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

import web from "../web"
import history from "../history"
import * as alertActions from "../alert/actions"
import * as objectsActions from "../objects/actions"
import { pathSlice } from "../utils"

export const SET_LIST = "buckets/SET_LIST"
export const ADD = "buckets/ADD"
export const REMOVE = "buckets/REMOVE"
export const SET_FILTER = "buckets/SET_FILTER"
export const SET_CURRENT_BUCKET = "buckets/SET_CURRENT_BUCKET"
export const SHOW_MAKE_BUCKET_MODAL = "buckets/SHOW_MAKE_BUCKET_MODAL"
export const SHOW_BUCKET_POLICY = "buckets/SHOW_BUCKET_POLICY"
export const SET_POLICIES = "buckets/SET_POLICIES"

export const fetchBuckets = () => {
  return function(dispatch) {
    const { bucket, prefix } = pathSlice(history.location.pathname)
    return web.ListBuckets().then(res => {
      const buckets = res.buckets ? res.buckets.map(bucket => bucket.name) : []
      if (buckets.length > 0) {
        dispatch(setList(buckets))
        if (bucket && buckets.indexOf(bucket) > -1) {
          dispatch(selectBucket(bucket, prefix))
        } else {
          dispatch(selectBucket(buckets[0]))
        }
      } else {
        if (bucket) {
          dispatch(setList([bucket]))
          dispatch(selectBucket(bucket, prefix))
        } else {
          dispatch(selectBucket(""))
          history.replace("/")
        }
      }
    })
    .catch(err => {
      if (bucket && err.message === "Access Denied." || err.message.indexOf('Prefix access is denied') > -1 ) {
        dispatch(setList([bucket]))
        dispatch(selectBucket(bucket, prefix))
      } else {
        dispatch(
          alertActions.set({
            type: "danger",
            message: err.message,
            autoClear: true,
          })
        )
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

export const selectBucket = (bucket, prefix) => {
  return function(dispatch) {
    dispatch(setCurrentBucket(bucket))
    dispatch(objectsActions.selectPrefix(prefix || ""))
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

export const deleteBucket = bucket => {
  return function(dispatch) {
    return web
      .DeleteBucket({
        bucketName: bucket
      })
      .then(() => {
        dispatch(
          alertActions.set({
            type: "info",
            message: "Bucket '" + bucket + "' has been deleted."
          })
        )
        dispatch(removeBucket(bucket))
        dispatch(fetchBuckets())
      })
      .catch(err => { 
        dispatch(
          alertActions.set({
            type: "danger",
            message: err.message
          })
        )
      })
  }
}

export const addBucket = bucket => ({
  type: ADD,
  bucket
})

export const removeBucket = bucket => ({
  type: REMOVE,
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

export const fetchPolicies = bucket => {
  return function(dispatch) {
    return web
      .ListAllBucketPolicies({
        bucketName: bucket
      })
      .then(res => {
        let policies = res.policies
        if(policies)
          dispatch(setPolicies(policies))
        else
          dispatch(setPolicies([]))
      })
      .catch(err => {
        dispatch(
          alertActions.set({
            type: "danger",
            message: err.message
          })
        )
      })
  }
}

export const setPolicies = policies => ({
  type: SET_POLICIES,
  policies
})

export const showBucketPolicy = () => ({
  type: SHOW_BUCKET_POLICY,
  show: true
})

export const hideBucketPolicy = () => ({
  type: SHOW_BUCKET_POLICY,
  show: false
})