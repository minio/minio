/*
 * Minio Cloud Storage (C) 2017 Minio, Inc.
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

import { minioBrowserPrefix } from './constants'

import Vue from 'vue'
import Vuex from 'vuex'
Vue.use(Vuex)

import Moment from 'moment'
import filesize from 'file-size'
import { getDataType } from './mime'

// This store is our global state manager.
export const store = new Vuex.Store({
  // This is our default state.
  state: {
    web: null,
    buckets: [],
    objects: [],
    checkedObjects: [],
    istruncated: true,
    serverInfo: {},
    currentBucket: '',
    currentPath: '',
    uploads: {},
    alert: {
      show: false,
      type: 'danger',
      message: ''
    },
    latestUiVersion: currentUiVersion,
    sideBarActive: false,
    loginRedirectPath: minioBrowserPrefix,
    policies: [],
    shareObject: {
      url: '',
      object: ''
    },
    prefixWritable: false,
    previewStatus: {
      display: false,
      bucket: '',
      object: ''
    },
    modals: {
      policy: {
        bucket: '',
        show: false
      },
      about: {
        show: false
      },
      settings: {
        show: false
      },
      'make-bucket': {
        show: false
      },
      delete: {
        show: false
      }
    }
  },


  // Getters are computed based on the state.
  getters: {
    // Technically this is computed off of local storage, but that's synchronous.
    isLoggedIn: state => {
      // web can be null
      if (!state.web)
        return false

      return state.web.LoggedIn()
    },

    objects: state => {
      const { checkedObjects, currentPath } = state

      return state.objects.map((object, i) => {
        // Gives data about each object.
        let size = object.name.endsWith('/') ? '' : filesize(object.size).human()
        let lastModified = object.name.endsWith('/') ? '' : Moment(object.lastModified).format('lll')
        let path = currentPath + object.name

        let type = getDataType(object.name, object.contentType)

        let isChecked = (checkedObjects.indexOf(object.name) != -1)
        let isFolder = (type == 'folder')

        return {
          size,
          lastModified,
          path,
          type,
          isChecked,
          isFolder,
          name: object.name
        }
      })
    }
  },


  // Mutations synchronously change the app state.
  mutations: {
    selectBucket(state, { bucket, prefix }) {
      prefix = prefix || ''

      let currentBucket = state.currentBucket

      if (currentBucket !== bucket)
        state.commit('setLoadBucket', bucket)

      state.commit('setCurrentBucket', bucket)
      state.dispatch('selectPrefix', prefix)
    },

    addBucket(state, bucket) {
      state.buckets = [bucket].concat(state.buckets)
    },

    setBuckets(state, buckets) {
      state.buckets = buckets
    },

    removeBucket(state, bucket) {
      let index = state.buckets.indexOf(bucket)

      state.buckets = state.buckets.filter((e, i) => i != index)
    },

    setCurrentBucket(state, bucket) {
      state.currentBucket = bucket
    },

    // Resets the object list to empty.
    resetObjects(state) {
      state.objects = []
      state.marker = ''
      state.istruncated = false
    },

    setObjects(state, {objects, marker, istruncated}) {
      state.objects = objects
      state.marker = marker
      state.istruncated = istruncated
    },

    // Remove a single object from the object list.
    removeObject(state, object) {
      state.objects = state.objects.filter(obj => obj.name != object.name)
    },

    // Add a checked object.
    addCheckedObject(state, object) {
      state.checkedObjects.push(object)
    },

    // Remove a checked object.
    removeCheckedObject(state, object) {
      let index = state.checkedObjects.indexOf(object)

      state.checkedObjects = state.checkedObjects.filter((e, i) => i != index)
    },

    setPolicies(state, policies) {
      state.policies = policies
    },

    addPolicy(state, policy) {
      state.policies = state.policies.concat([policy])
    },

    removePolicy(state, policy) {
      state.policies = state.policies.filter((e) => e.prefix != policy.prefix)
    },

    setPreviewStatus(state, status) {
      state.previewStatus = status
    },

    setModalStatus(state, {modal, status}) {
      let modals = state.modals
      modals[modal] = status

      state.modals = modals
    },

    setServerInfo(state, info) {
      state.serverInfo = info
    },

    // Set the alert modal status.
    setAlert(state, alert) {
      state.alert = alert
    },

    addUpload(state, upload) {
      upload = Object.assign({loaded: 0}, upload)

      state.uploads = Object.assign({[upload.slug]: upload}, state.uploads)
    },

    // Set the progress of a single upload slug.
    setUploadProgress(state, {slug, progress}) {
      state.uploads[slug].loaded = progress

      state.uploads = Object.assign({}, state.uploads)
    },

    removeUpload(state, slug) {
      delete state.uploads[slug]

      state.uploads = Object.assign({}, state.uploads)
    }
  },


  // Actions are asynchronous state-changing events.
  actions: {
    // error is a shorthand for showAlert() with an Error type.
    error(context, err) {
      context.dispatch('showAlert', {
        type: 'danger',
        message: err.message
      })
    },

    // showAlert triggers an alert that will automatically close if it is
    // a non-error.
    showAlert(context, alert) {
      if (alert.type !== 'danger') {
        var alertTimeout = setTimeout(() => {
          // Clear the alert.
          context.commit('setAlert', {
            show: false
          })
        }, 5000)
      }

      // Set the alert with extra fields (show, timeout).
      context.commit('setAlert', Object.assign({
        show: true,
        alertTimeout
      }, alert))
    },

    // Remove an object from the checked objects list and the regular objects
    // list. This should always be used instead of the mutations.
    removeObject(context, objectName) {
      context.commit('removeObject', { name: objectName })
      context.commit('removeCheckedObject', objectName)
    },

    uploadFile(context, {file, xhr}) {
      // TODO implement. Also this probably doesn't belong in the app store.
      const {currentBucket, currentPath} = context.state

      const objectName = `${currentPath}${file.name}`
      const uploadUrl = `${window.location.origin}/minio/upload/${currentBucket}/${objectName}`
      // The slug is a unique identifer for the file upload.
      const slug = `${currentBucket}-${currentPath}-${file.name}`

      xhr.open('PUT', uploadUrl, true)
      xhr.withCredentials = false
      xhr.setRequestHeader("Authorization", 'Bearer ' + storage.getItem('token'))
      xhr.setRequestHeader('x-amz-date', Moment().utc().format('YYYYMMDDTHHmmss') + 'Z')

      context.commit('addUpload', {slug, xhr, size: file.size, name: file.name})

      xhr.onload = function(event) {
        if (xhr.status == 200) {
          context.dispatch('showAlert', {
            type: 'success',
            message: `File '${file.name}' uploaded successfully.`
          })

          context.commit('removeUpload', slug)
        } else if (xhr.status == 401 || xhr.status == 403 || xhr.status == 500) {
          context.dispatch('error', {
            message: 'Unauthorized request.'
          })

          context.commit('removeUpload', slug)
        }
      }

      xhr.upload.addEventListener('error', () => {
        context.dispatch('error', {
          message: `Error occurred uploading '${file.name}'.`
        })

        context.commit('removeUpload', slug)
      })

      xhr.upload.addEventListener('progress', event => {
        if (event.lengthComputable) {
          let loaded = event.loaded
          let total = event.total

          // Update the counter.
          context.commit('setUploadProgress', {
            slug, loaded
          })
        }
      })

      xhr.send(file)
    },

    shareObject: function(object, days, hours, minutes) {
      const state = this.$store.state

      const web = state.web
      const host = location.host
      const bucket = state.currentBucket

      if (!web || !web.LoggedIn()) {
        state.shareObject = { object, url: `${host}/${bucket}/${object}` }
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
          state.shareObject = { object, url: obj.url }

          state.dispatch('showAlert', {
            type: 'success',
            message: `Object shared. Expires in ${days} days, ${hours} hours, and ${minutes} minutes.`
          })
        })
        .catch(err => state.dispatch('error', err))
    },

    loadBuckets: function(context, fallback) {
      if (!context.getters.isLoggedIn) {
        // If we aren't logged in, we'll get a fallback bucket to list. This
        // allows a user to specify a read-only (etc) bucket to browse without
        // having access credentials.

        context.commit('setBuckets', [fallback])
        // TODO dispatch(actions.selectBucket(obj.bucket, obj.prefix))
        return
      }

      context.state.web.ListBuckets()
        .then(res => {
          if (!res.buckets)
            return

          const buckets = res.buckets.map(bucket => bucket.name)

          context.commit('setBuckets', buckets)
        })
        .catch(err => context.dispatch('error', err))
    }
  }
})

/*
TODO do download and upload handlers

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
        anchor.download = req.bucketName + separator + req.prefix.slice(0, -1) + '.zip';




        anchor.click()
        window.URL.revokeObjectURL(blobUrl)
        anchor.remove()
      }
    };
    xhr.send(JSON.stringify(req));
  }
}
}*/
