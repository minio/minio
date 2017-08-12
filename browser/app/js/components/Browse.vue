<!--
 ! Minio Cloud Storage (C) 2017 Minio, Inc.
 !
 ! Licensed under the Apache License, Version 2.0 (the "License");
 ! you may not use this file except in compliance with the License.
 ! You may obtain a copy of the License at
 !
 !     http://www.apache.org/licenses/LICENSE-2.0
 !
 ! Unless required by applicable law or agreed to in writing, software
 ! distributed under the License is distributed on an "AS IS" BASIS,
 ! WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 ! See the License for the specific language governing permissions and
 ! limitations under the License.
 !-->

<template>
  <section class="browser__inner">
    <alert />

    <section class='content' v-bind:class="{ 'content--toggled': sideBarActive }">
      <header class="header">
        <div class="toolbar">
          <div class="actions">
            <button class="zmdi zmdi-menu" v-on:click="toggleSidebar" />
            <button class="zmdi zmdi-view-comfy" />
            <button v-on:click="promptDeleteObjects" v-bind:disabled="checkedObjects.length == 0" class="zmdi zmdi-delete" />
            <button v-on:click="shareObject" v-bind:disabled="checkedObjects.length != 1" class="zmdi zmdi-share" />
            <button v-on:click="downloadSelected" v-bind:disabled="checkedObjects.length == 0" class="zmdi zmdi-download" />
          </div>

          <browser-dropdown v-if="isLoggedIn" />
          <a v-else class="btn btn-danger" href='/minio/login'>Login</a>
        </div>

        <breadcrumb-path />
      </header>

      <side-bar />

      <objects-list />

      <object-preview />

      <upload-modal />

      <dropdown v-if="isLoggedIn" class="create-new dropup">
        <button slot="dropdown-button" class="dropdown-toggle btn btn-default create-new__toggle">
          <i class="zmdi zmdi-plus"></i>
        </button>
        <ul slot="dropdown-menu" class="dropdown-menu">
          <tooltip placement="top" content="Upload File">
            <a href="#" class="create-new__btn create-new__btn--upload">
              <input type="file" @change="uploadFile" id="object-upload-input" />
              <label htmlFor="object-upload-input"> </label>
            </a>
          </tooltip>
          <tooltip placement="top" content="Make Bucket">
            <a href="#" class="create-new__btn create-new__btn--bucket" v-on:click="makeBucket"></a>
          </tooltip>
          <tooltip placement="top" content="Upload Folder">
            <a href="#" class="create-new__btn create-new__btn--folder"></a>
          </tooltip>
        </ul>
      </dropdown>

      <make-bucket-modal />

      <about-modal />

      <!--<share-modal ref="share_modal" />-->

      <settings-modal />

      <policy-modal />

      <confirm-delete-modal />

      <div class="sidebar-backdrop" v-bind:class="{ 'sidebar-backdrop--toggled': sideBarActive }" v-on:click="toggleSidebar" />
    </section>
  </section>
</template>

<script>
import storage from 'local-storage-fallback'

import { dropdown, tooltip } from 'vue-strap'

import Path from './Path.vue'
import BrowserDropdown from './BrowserDropdown.vue'
import Preview from './Preview.vue'
import ObjectsList from './ObjectsList.vue'
import SideBar from './SideBar.vue'
import Alert from './Alert.vue'

import ConfirmDeleteModal from './modals/ConfirmDeleteModal.vue'
import UploadModal from './modals/UploadModal.vue'
import SettingsModal from './modals/SettingsModal.vue'
import AboutModal from './modals/AboutModal.vue'
import ShareModal from './modals/ShareModal.vue'
import PolicyModal from './modals/PolicyModal.vue'
import MakeBucketModal from './modals/MakeBucketModal.vue'

import * as utils from '../utils'
import * as mime from '../mime'
import { minioBrowserPrefix } from '../constants'

import { mapState } from 'vuex'

import logoInvert from '../../img/logo-dark.svg'

export default {
  name: 'Browse',

  components: {
    'dropdown': dropdown,
    'tooltip': tooltip,
    'alert': Alert,
    'breadcrumb-path': Path,
    'browser-dropdown': BrowserDropdown,
    'object-preview': Preview,
    'objects-list': ObjectsList,
    'side-bar': SideBar,
    'confirm-delete-modal': ConfirmDeleteModal,
    'upload-modal': UploadModal,
    'settings-modal': SettingsModal,
    'about-modal': AboutModal,
    'share-modal': ShareModal,
    'policy-modal': PolicyModal,
    'make-bucket-modal': MakeBucketModal
  },

  computed: Object.assign({
    isLoggedIn: function() {
      return this.$store.getters.isLoggedIn
    },
  }, mapState({
    checkedObjects: state => state.checkedObjects,

    sideBarActive: state => state.sideBarActive,

    policies: state => state.policies,

    currentBucket: state => state.currentBucket
  })),

  methods: {
    makeBucket: function() {
      this.$store.commit('setModalStatus', {
        modal: 'make-bucket',
        status: {
          show: true
        }
      })
    },

    listObjects: function() {
      const {dispatch} = this.props
      dispatch(actions.listObjects())
    },

    uploadFile: function(e) {
      let file = e.target.files[0]
      e.target.value = null

      this.$store.dispatch('uploadFile', {file, xhr: new XMLHttpRequest()})
    },

    promptDeleteObjects: function() {
      this.$store.commit('setModalStatus', {
        modal: 'delete',
        status: {
          show: true
        }
      })
    },

    shareObject: function(e, object) {
      e.preventDefault()
      const {dispatch} = this.props
      // let expiry = 5 * 24 * 60 * 60 // 5 days expiry by default
      dispatch(actions.shareObject(object, 5, 0, 0))
    },

    hideShareObjectModal: function() {
      const {dispatch} = this.props
      dispatch(actions.hideShareObject())
    },

    toggleSidebar: function() {
      const old = this.$store.state.sideBarActive

      this.$store.state.sideBarActive = !old
    },

    showMessage: function() {
      // TODO
      const {dispatch} = this.props
      dispatch(actions.showAlert({
        type: 'success',
        message: 'Link copied to clipboard!'
      }))

      this.$refs.share_modal.hide()
    },

    selectTexts: function() {
      this.refs.copyTextInput.select()
    },

    downloadSelected: function() {
      // TODO
      const {dispatch, web} = this.props
      let req = {
        bucketName: this.props.currentBucket,
        objects: this.props.checkedObjects,
        prefix: this.props.currentPath
      }

      web.CreateURLToken()
        .then(res => {
          let requestUrl = location.origin + "/minio/zip?token=" + res.token

          this.xhr = new XMLHttpRequest()
          dispatch(actions.downloadSelected(requestUrl, req, this.xhr))
        })
        .catch(err => dispatch(actions.showAlert({
          type: 'danger',
          message: err.message
        })))
    },

    renderold: function() {
      // TODO
      /*
      } else {
        if (prefixWritable)
          createButton = <Dropdown dropup class="create-new" id="dropdown-create-new">
                           <Dropdown.Toggle noCaret class="create-new__toggle">
                             <i class="zmdi zmdi-times"></i>
                           </Dropdown.Toggle>
                           <Dropdown.Menu>
                             <a href="#" class="create-new__btn create-new__btn--upload">
                               <input type="file" onChange={ this.uploadFile.bind(this) } id="object-upload-input" />
                               <label htmlFor="object-upload-input"> </label>
                             </a>
                           </Dropdown.Menu>
                         </Dropdown>
      }
      */
    }
  }
}
</script>
