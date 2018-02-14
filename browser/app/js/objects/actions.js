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
import {
  sortObjectsByName,
  sortObjectsBySize,
  sortObjectsByDate
} from "../utils"
import { getCurrentBucket } from "../buckets/selectors"
import { getCurrentPrefix } from "./selectors"
import * as alertActions from "../alert/actions"

export const SET_LIST = "objects/SET_LIST"
export const APPEND_LIST = "objects/APPEND_LIST"
export const REMOVE = "objects/REMOVE"
export const RESET = "objects/RESET"
export const SET_SORT_BY = "objects/SET_SORT_BY"
export const SET_SORT_ORDER = "objects/SET_SORT_ORDER"
export const SET_CURRENT_PREFIX = "objects/SET_CURRENT_PREFIX"

export const setList = (objects, marker, isTruncated) => ({
  type: SET_LIST,
  objects,
  marker,
  isTruncated
})

export const appendList = (objects, marker, isTruncated) => ({
  type: APPEND_LIST,
  objects,
  marker,
  isTruncated
})

export const fetchObjects = append => {
  return function(dispatch, getState) {
    const {
      buckets: { currentBucket },
      objects: { currentPrefix, marker }
    } = getState()
    return web
      .ListObjects({
        bucketName: currentBucket,
        prefix: currentPrefix,
        marker: append ? marker : ""
      })
      .then(res => {
        let objects = []
        if (res.objects) {
          objects = res.objects.map(object => {
            return {
              ...object,
              name: object.name.replace(currentPrefix, "")
            }
          })
        }
        if (append) {
          dispatch(appendList(objects, res.nextmarker, res.istruncated))
        } else {
          dispatch(setList(objects, res.nextmarker, res.istruncated))
          dispatch(setSortBy(""))
          dispatch(setSortOrder(false))
        }
      })
  }
}

export const sortObjects = sortBy => {
  return function(dispatch, getState) {
    const { objects } = getState()
    const sortOrder = objects.sortBy == sortBy ? !objects.sortOrder : true
    dispatch(setSortBy(sortBy))
    dispatch(setSortOrder(sortOrder))
    let list
    switch (sortBy) {
      case "name":
        list = sortObjectsByName(objects.list, sortOrder)
        break
      case "size":
        list = sortObjectsBySize(objects.list, sortOrder)
        break
      case "last-modified":
        list = sortObjectsByDate(objects.list, sortOrder)
        break
      default:
        list = objects.list
        break
    }
    dispatch(setList(list, objects.marker, objects.isTruncated))
  }
}

export const setSortBy = sortBy => ({
  type: SET_SORT_BY,
  sortBy
})

export const setSortOrder = sortOrder => ({
  type: SET_SORT_ORDER,
  sortOrder
})

export const selectPrefix = prefix => {
  return function(dispatch, getState) {
    dispatch(setCurrentPrefix(prefix))
    dispatch(fetchObjects())
    const currentBucket = getCurrentBucket(getState())
    history.replace(`/${currentBucket}/${prefix}`)
  }
}

export const setCurrentPrefix = prefix => {
  return {
    type: SET_CURRENT_PREFIX,
    prefix
  }
}

export const deleteObject = object => {
  return function(dispatch, getState) {
    const currentBucket = getCurrentBucket(getState())
    const currentPrefix = getCurrentPrefix(getState())
    const objectName = `${currentPrefix}${object}`
    return web
      .RemoveObject({
        bucketName: currentBucket,
        objects: [objectName]
      })
      .then(() => {
        dispatch(removeObject(object))
      })
      .catch(e => {
        dispatch(
          alertActions.set({
            type: "danger",
            message: e.message
          })
        )
      })
  }
}

export const removeObject = object => ({
  type: REMOVE,
  object
})
