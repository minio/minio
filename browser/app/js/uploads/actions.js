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

import Moment from "moment"
import storage from "local-storage-fallback"
import * as alertActions from "../alert/actions"
import * as objectsActions from "../objects/actions"
import { getCurrentBucket } from "../buckets/selectors"
import { getCurrentPrefix } from "../objects/selectors"
import { minioBrowserPrefix } from "../constants"

export const ADD = "uploads/ADD"
export const UPDATE_PROGRESS = "uploads/UPDATE_PROGRESS"
export const STOP = "uploads/STOP"
export const SHOW_ABORT_MODAL = "uploads/SHOW_ABORT_MODAL"

export const add = (slug, size, name) => ({
  type: ADD,
  slug,
  size,
  name
})

export const updateProgress = (slug, loaded) => ({
  type: UPDATE_PROGRESS,
  slug,
  loaded
})

export const stop = slug => ({
  type: STOP,
  slug
})

export const showAbortModal = () => ({
  type: SHOW_ABORT_MODAL,
  show: true
})

export const hideAbortModal = () => ({
  type: SHOW_ABORT_MODAL,
  show: false
})

let requests = {}

export const addUpload = (xhr, slug, size, name) => {
  return function(dispatch) {
    requests[slug] = xhr
    dispatch(add(slug, size, name))
  }
}

export const abortUpload = slug => {
  return function(dispatch) {
    const xhr = requests[slug]
    if (xhr) {
      xhr.abort()
    }
    dispatch(stop(slug))
    dispatch(hideAbortModal())
  }
}

export const uploadFile = file => {
  return function(dispatch, getState) {
    const state = getState()
    const currentBucket = getCurrentBucket(state)
    if (!currentBucket) {
      dispatch(
        alertActions.set({
          type: "danger",
          message: "Please choose a bucket before trying to upload files."
        })
      )
      return
    }
    const currentPrefix = getCurrentPrefix(state)
    var _filePath = file.path || file.name
    if (_filePath.charAt(0) == '/') {
      _filePath = _filePath.substring(1)
    }
    const filePath = _filePath
    const objectName = encodeURIComponent(`${currentPrefix}${filePath}`)
    const uploadUrl = `${
      window.location.origin
    }${minioBrowserPrefix}/upload/${currentBucket}/${objectName}`
    const slug = `${currentBucket}-${currentPrefix}-${filePath}`

    let xhr = new XMLHttpRequest()
    xhr.open("PUT", uploadUrl, true)
    xhr.withCredentials = false
    const token = storage.getItem("token")
    if (token) {
      xhr.setRequestHeader(
        "Authorization",
        "Bearer " + storage.getItem("token")
      )
    }
    xhr.setRequestHeader(
      "x-amz-date",
      Moment()
        .utc()
        .format("YYYYMMDDTHHmmss") + "Z"
    )

    dispatch(addUpload(xhr, slug, file.size, file.name))

    xhr.onload = function(event) {
      if (xhr.status == 401 || xhr.status == 403) {
        dispatch(hideAbortModal())
        dispatch(stop(slug))
        dispatch(
          alertActions.set({
            type: "danger",
            message: "Unauthorized request."
          })
        )
      }
      if (xhr.status == 500) {
        dispatch(hideAbortModal())
        dispatch(stop(slug))
        dispatch(
          alertActions.set({
            type: "danger",
            message: xhr.responseText
          })
        )
      }
      if (xhr.status == 200) {
        dispatch(hideAbortModal())
        dispatch(stop(slug))
        dispatch(
          alertActions.set({
            type: "success",
            message: "File '" + filePath + "' uploaded successfully."
          })
        )
        dispatch(objectsActions.selectPrefix(currentPrefix))
      }
    }

    xhr.upload.addEventListener("error", event => {
      dispatch(stop(slug))
      dispatch(
        alertActions.set({
          type: "danger",
          message: "Error occurred uploading '" + filePath + "'."
        })
      )
    })

    xhr.upload.addEventListener("progress", event => {
      if (event.lengthComputable) {
        let loaded = event.loaded
        let total = event.total
        // Update the counter
        dispatch(updateProgress(slug, loaded))
      }
    })

    xhr.send(file)
  }
}
