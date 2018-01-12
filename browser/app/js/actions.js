/*
 * Minio Cloud Storage (C) 2016 Minio, Inc.
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

import Moment from 'moment'
import browserHistory from 'react-router/lib/browserHistory'
import storage from 'local-storage-fallback'
import { minioBrowserPrefix } from './constants'

export const SET_WEB = 'SET_WEB'
export const SET_CURRENT_BUCKET = 'SET_CURRENT_BUCKET'
export const SET_CURRENT_PATH = 'SET_CURRENT_PATH'
export const SET_BUCKETS = 'SET_BUCKETS'
export const ADD_BUCKET = 'ADD_BUCKET'
export const REMOVE_BUCKET = 'REMOVE_BUCKET'
export const SHOW_BUCKET_DROPDOWN = 'SHOW_BUCKET_DROPDOWN'
export const SET_VISIBLE_BUCKETS = 'SET_VISIBLE_BUCKETS'
export const SET_OBJECTS = 'SET_OBJECTS'
export const APPEND_OBJECTS = 'APPEND_OBJECTS'
export const RESET_OBJECTS = 'RESET_OBJECTS'
export const SET_STORAGE_INFO = 'SET_STORAGE_INFO'
export const SET_SERVER_INFO = 'SET_SERVER_INFO'
export const SHOW_MAKEBUCKET_MODAL = 'SHOW_MAKEBUCKET_MODAL'
export const ADD_UPLOAD = 'ADD_UPLOAD'
export const STOP_UPLOAD = 'STOP_UPLOAD'
export const UPLOAD_PROGRESS = 'UPLOAD_PROGRESS'
export const SET_ALERT = 'SET_ALERT'
export const SET_LOGIN_ERROR = 'SET_LOGIN_ERROR'
export const SET_SHOW_ABORT_MODAL = 'SET_SHOW_ABORT_MODAL'
export const SHOW_ABOUT = 'SHOW_ABOUT'
export const SET_SORT_NAME_ORDER = 'SET_SORT_NAME_ORDER'
export const SET_SORT_SIZE_ORDER = 'SET_SORT_SIZE_ORDER'
export const SET_SORT_DATE_ORDER = 'SET_SORT_DATE_ORDER'
export const SET_LATEST_UI_VERSION = 'SET_LATEST_UI_VERSION'
export const SET_SIDEBAR_STATUS = 'SET_SIDEBAR_STATUS'
export const SET_LOGIN_REDIRECT_PATH = 'SET_LOGIN_REDIRECT_PATH'
export const SET_LOAD_BUCKET = 'SET_LOAD_BUCKET'
export const SET_LOAD_PATH = 'SET_LOAD_PATH'
export const SHOW_SETTINGS = 'SHOW_SETTINGS'
export const SET_SETTINGS = 'SET_SETTINGS'
export const SHOW_BUCKET_POLICY = 'SHOW_BUCKET_POLICY'
export const SET_POLICIES = 'SET_POLICIES'
export const SET_SHARE_OBJECT = 'SET_SHARE_OBJECT'
export const DELETE_CONFIRMATION = 'DELETE_CONFIRMATION'
export const SET_PREFIX_WRITABLE = 'SET_PREFIX_WRITABLE'
export const REMOVE_OBJECT = 'REMOVE_OBJECT'
export const CHECKED_OBJECTS_ADD = 'CHECKED_OBJECTS_ADD'
export const CHECKED_OBJECTS_REMOVE = 'CHECKED_OBJECTS_REMOVE'
export const CHECKED_OBJECTS_RESET = 'CHECKED_OBJECTS_RESET'

export const showDeleteConfirmation = (object) => {
  return {
    type: DELETE_CONFIRMATION,
    payload: {
      object,
      show: true
    }
  }
}

export const hideDeleteConfirmation = () => {
  return {
    type: DELETE_CONFIRMATION,
    payload: {
      object: '',
      show: false
    }
  }
}

export const showShareObject = (object, url) => {
  return {
    type: SET_SHARE_OBJECT,
    shareObject: {
      object,
      url,
      show: true
    }
  }
}

export const hideShareObject = () => {
  return {
    type: SET_SHARE_OBJECT,
    shareObject: {
      url: '',
      show: false
    }
  }
}

export const shareObject = (object, days, hours, minutes) => (dispatch, getState) => {
  const {currentBucket, web} = getState()
  let host = location.host
  let bucket = currentBucket

  if (!web.LoggedIn()) {
    dispatch(showShareObject(object, `${host}/${bucket}/${object}`))
    return
  }

  let expiry = days * 24 * 60 * 60 + hours * 60 * 60 + minutes * 60
  web.PresignedGet({
    host,
    bucket,
    object,
    expiry
  })
    .then(obj => {
      dispatch(showShareObject(object, obj.url))
      dispatch(showAlert({
        type: 'success',
        message: `Object shared. Expires in ${days} days ${hours} hours ${minutes} minutes.`
      }))
    })
    .catch(err => {
      dispatch(showAlert({
        type: 'danger',
        message: err.message
      }))
    })
}

export const setLoginRedirectPath = (path) => {
  return {
    type: SET_LOGIN_REDIRECT_PATH,
    path
  }
}

export const setLoadPath = (loadPath) => {
  return {
    type: SET_LOAD_PATH,
    loadPath
  }
}

export const setLoadBucket = (loadBucket) => {
  return {
    type: SET_LOAD_BUCKET,
    loadBucket
  }
}

export const setWeb = web => {
  return {
    type: SET_WEB,
    web
  }
}

export const setBuckets = buckets => {
  return {
    type: SET_BUCKETS,
    buckets
  }
}

export const addBucket = bucket => {
  return {
    type: ADD_BUCKET,
    bucket
  }
}

export const removeBucket = bucket => {
  return {
    type: REMOVE_BUCKET,
    bucket
  }
}

export const showBucketDropdown = bucket => {
  return {
    type: SHOW_BUCKET_DROPDOWN,
    showBucketDropdown: true
  }
}

export const hideBucketDropdown = bucket => {
  return {
    type: SHOW_BUCKET_DROPDOWN,
    showBucketDropdown: false
  }
}

export const showMakeBucketModal = () => {
  return {
    type: SHOW_MAKEBUCKET_MODAL,
    showMakeBucketModal: true
  }
}

export const hideAlert = () => {
  return {
    type: SET_ALERT,
    alert: {
      show: false,
      message: '',
      type: ''
    }
  }
}

export const showAlert = alert => {
  return (dispatch, getState) => {
    let alertTimeout = null
    if (alert.type !== 'danger') {
      alertTimeout = setTimeout(() => {
        dispatch({
          type: SET_ALERT,
          alert: {
            show: false
          }
        })
      }, 5000)
    }
    dispatch({
      type: SET_ALERT,
      alert: Object.assign({}, alert, {
        show: true,
        alertTimeout
      })
    })
  }
}

export const removeObject = object => {
  return {
    type: REMOVE_OBJECT,
    object
  }
}

export const setSidebarStatus = (status) => {
  return {
    type: SET_SIDEBAR_STATUS,
    sidebarStatus: status
  }
}

export const hideMakeBucketModal = () => {
  return {
    type: SHOW_MAKEBUCKET_MODAL,
    showMakeBucketModal: false
  }
}

export const setVisibleBuckets = visibleBuckets => {
  return {
    type: SET_VISIBLE_BUCKETS,
    visibleBuckets
  }
}

const appendObjects = (objects, marker, istruncated) => {
  return {
    type: APPEND_OBJECTS,
    objects,
    marker,
    istruncated
  }
}

export const setObjects = (objects) => {
  return {
    type: SET_OBJECTS,
    objects,
  }
}

export const resetObjects = () => {
  return {
    type: RESET_OBJECTS
  }
}

export const setCurrentBucket = currentBucket => {
  return {
    type: SET_CURRENT_BUCKET,
    currentBucket
  }
}

export const setCurrentPath = currentPath => {
  return {
    type: SET_CURRENT_PATH,
    currentPath
  }
}

export const setStorageInfo = storageInfo => {
  return {
    type: SET_STORAGE_INFO,
    storageInfo
  }
}

export const setServerInfo = serverInfo => {
  return {
    type: SET_SERVER_INFO,
    serverInfo
  }
}

const setPrefixWritable = prefixWritable => {
  return {
    type: SET_PREFIX_WRITABLE,
    prefixWritable,
  }
}

export const selectBucket = (newCurrentBucket, prefix) => {
  if (!prefix)
    prefix = ''
  return (dispatch, getState) => {
    let web = getState().web
    let currentBucket = getState().currentBucket

    if (currentBucket !== newCurrentBucket) dispatch(setLoadBucket(newCurrentBucket))

    dispatch(setCurrentBucket(newCurrentBucket))
    dispatch(selectPrefix(prefix))
    return
  }
}

export const deleteBucket = (bucket) => {
  return (dispatch, getState) => {
    // DeleteBucket() RPC call will ONLY delete a bucket if it is empty of
    // objects. This means a call can just be sent, as it is entirely reversable
    // and won't do any permanent damage.
    web.DeleteBucket({
      bucketName: bucket
    })
      .then(() => {
        dispatch(showAlert({
          type: 'info',
          message: `Bucket '${bucket}' has been deleted.`
        }))
        dispatch(removeBucket(bucket))
      })
      .catch(err => {
        let message = err.message
        dispatch(showAlert({
          type: 'danger',
          message: message
        }))
      })
  }
}

export const listObjects = () => {
  return (dispatch, getState) => {
    const {buckets, currentBucket, currentPath, marker, objects, istruncated, web} = getState()
    if (!istruncated || buckets.length === 0) return
    web.ListObjects({
      bucketName: currentBucket,
      prefix: currentPath,
      marker: marker
    })
      .then(res => {
        let objects = res.objects
        if (!objects)
          objects = []
        objects = objects.map(object => {
          object.name = object.name.replace(`${currentPath}`, '');
          return object
        })
        dispatch(appendObjects(objects, res.nextmarker, res.istruncated))
        dispatch(setPrefixWritable(res.writable))
        dispatch(setLoadBucket(''))
        dispatch(setLoadPath(''))
      })
      .catch(err => {
        dispatch(showAlert({
          type: 'danger',
          message: err.message
        }))
        dispatch(setLoadBucket(''))
        dispatch(setLoadPath(''))
        // Use browserHistory.replace instead of push so that browser back button works fine.
        browserHistory.replace(`${minioBrowserPrefix}/login`)
      })
  }
}

export const selectPrefix = prefix => {
  return (dispatch, getState) => {
    const {currentBucket, web} = getState()
    dispatch(setLoadPath(prefix))
    web.ListObjects({
      bucketName: currentBucket,
      prefix,
      marker: ""
    })
      .then(res => {
        let objects = res.objects
        if (!objects)
          objects = []
        objects = objects.map(object => {
          object.name = object.name.replace(`${prefix}`, '');
          return object
        })
        dispatch(resetObjects())
        dispatch(appendObjects(
          objects,
          res.nextmarker,
          res.istruncated
        ))
        dispatch(setPrefixWritable(res.writable))
        dispatch(setSortNameOrder(false))
        dispatch(setCurrentPath(prefix))
        dispatch(setLoadBucket(''))
        dispatch(setLoadPath(''))
      })
      .catch(err => {
        dispatch(showAlert({
          type: 'danger',
          message: err.message
        }))
        dispatch(setLoadBucket(''))
        dispatch(setLoadPath(''))
        // Use browserHistory.replace instead of push so that browser back button works fine.
        browserHistory.replace(`${minioBrowserPrefix}/login`)
      })
  }
}

export const addUpload = options => {
  return {
    type: ADD_UPLOAD,
    slug: options.slug,
    size: options.size,
    xhr: options.xhr,
    name: options.name
  }
}

export const stopUpload = options => {
  return {
    type: STOP_UPLOAD,
    slug: options.slug
  }
}

export const uploadProgress = options => {
  return {
    type: UPLOAD_PROGRESS,
    slug: options.slug,
    loaded: options.loaded
  }
}

export const setShowAbortModal = showAbortModal => {
  return {
    type: SET_SHOW_ABORT_MODAL,
    showAbortModal
  }
}

export const setLoginError = () => {
  return {
    type: SET_LOGIN_ERROR,
    loginError: true
  }
}

export const downloadSelected = (url, req, xhr) => {
  return (dispatch) => {
    var anchor = document.createElement('a')
    document.body.appendChild(anchor);
    xhr.open('POST', url, true)
    xhr.responseType = 'blob'

    xhr.onload = function(e) {
      if (this.status == 200) {
        dispatch(checkedObjectsReset())
        var blob = new Blob([this.response], {
          type: 'octet/stream'
        })
        var blobUrl = window.URL.createObjectURL(blob);
        var separator = req.prefix.length > 1 ? '-' : ''

        anchor.href = blobUrl
        anchor.download = req.bucketName+separator+req.prefix.slice(0, -1)+'.zip';




        anchor.click()
        window.URL.revokeObjectURL(blobUrl)
        anchor.remove()
      }
    };
    xhr.send(JSON.stringify(req));
  }
}

export const uploadFile = (file, xhr) => {
  return (dispatch, getState) => {
    const {currentBucket, currentPath} = getState()
    const objectName = `${currentPath}${file.name}`
    const uploadUrl = `${window.location.origin}${minioBrowserPrefix}/upload/${currentBucket}/${objectName}`
    // The slug is a unique identifer for the file upload.
    const slug = `${currentBucket}-${currentPath}-${file.name}`

    xhr.open('PUT', uploadUrl, true)
    xhr.withCredentials = false
    const token = storage.getItem('token')
    if (token) xhr.setRequestHeader("Authorization", 'Bearer ' + storage.getItem('token'))
    xhr.setRequestHeader('x-amz-date', Moment().utc().format('YYYYMMDDTHHmmss') + 'Z')
    dispatch(addUpload({
      slug,
      xhr,
      size: file.size,
      name: file.name
    }))

    xhr.onload = function(event) {
      if (xhr.status == 401 || xhr.status == 403) {
        setShowAbortModal(false)
        dispatch(stopUpload({
          slug
        }))
        dispatch(showAlert({
          type: 'danger',
          message: 'Unauthorized request.'
        }))
      }
      if (xhr.status == 500) {
        setShowAbortModal(false)
        dispatch(stopUpload({
          slug
        }))
        dispatch(showAlert({
          type: 'danger',
          message: xhr.responseText
        }))
      }
      if (xhr.status == 200) {
        setShowAbortModal(false)
        dispatch(stopUpload({
          slug
        }))
        dispatch(showAlert({
          type: 'success',
          message: 'File \'' + file.name + '\' uploaded successfully.'
        }))
        dispatch(selectPrefix(currentPath))
      }
    }

    xhr.upload.addEventListener('error', event => {
      dispatch(showAlert({
        type: 'danger',
        message: 'Error occurred uploading \'' + file.name + '\'.'
      }))
      dispatch(stopUpload({
        slug
      }))
    })

    xhr.upload.addEventListener('progress', event => {
      if (event.lengthComputable) {
        let loaded = event.loaded
        let total = event.total

        // Update the counter.
        dispatch(uploadProgress({
          slug,
          loaded
        }))
      }
    })
    xhr.send(file)
  }
}

export const showAbout = () => {
  return {
    type: SHOW_ABOUT,
    showAbout: true
  }
}

export const hideAbout = () => {
  return {
    type: SHOW_ABOUT,
    showAbout: false
  }
}

export const setSortNameOrder = (sortNameOrder) => {
  return {
    type: SET_SORT_NAME_ORDER,
    sortNameOrder
  }
}

export const setSortSizeOrder = (sortSizeOrder) => {
  return {
    type: SET_SORT_SIZE_ORDER,
    sortSizeOrder
  }
}

export const setSortDateOrder = (sortDateOrder) => {
  return {
    type: SET_SORT_DATE_ORDER,
    sortDateOrder
  }
}

export const setLatestUIVersion = (latestUiVersion) => {
  return {
    type: SET_LATEST_UI_VERSION,
    latestUiVersion
  }
}

export const showSettings = () => {
  return {
    type: SHOW_SETTINGS,
    showSettings: true
  }
}

export const hideSettings = () => {
  return {
    type: SHOW_SETTINGS,
    showSettings: false
  }
}

export const setSettings = (settings) => {
  return {
    type: SET_SETTINGS,
    settings
  }
}

export const showBucketPolicy = () => {
  return {
    type: SHOW_BUCKET_POLICY,
    showBucketPolicy: true
  }
}

export const hideBucketPolicy = () => {
  return {
    type: SHOW_BUCKET_POLICY,
    showBucketPolicy: false
  }
}

export const setPolicies = (policies) => {
  return {
    type: SET_POLICIES,
    policies
  }
}

export const checkedObjectsAdd = (objectName) => {
  return {
    type: CHECKED_OBJECTS_ADD,
    objectName
  }
}

export const checkedObjectsRemove = (objectName) => {
  return {
    type: CHECKED_OBJECTS_REMOVE,
    objectName
  }
}

export const checkedObjectsReset = (objectName) => {
  return {
    type: CHECKED_OBJECTS_RESET,
    objectName
  }
}
