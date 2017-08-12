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
  <aside class="sidebar" v-bind:class="{'sidebar--toggled': sideBarActive }">
    <div class="logo">
      <img class="logo__img" src='/minio/logo-dark.svg' alt="" />
      <div class="logo__title">
        <h2>Minio Browser</h2>
        <small>{{ host }}</small>
      </div>
    </div>
    <div class="buckets">
      <div class="buckets__search">
        <div class="form-group">
          <input class="form-group__field"
            type="text"
            v-model="searchQuery"
            placeholder="Search Buckets..." />
          <i class="form-group__bar"></i>
        </div>
      </div>
      <ul class="buckets__list">
        <li v-for="bucket in buckets" v-bind:class="{ 'buckets__list__active': bucket.isCurrent }" v-on:click="selectBucket(bucket)">
          <div class="buckets__list__name">
            {{ bucket.name }}
          </div>
          <div class="buckets__list__policy">
            {{ bucket.policy }}
          </div>
          <dropdown class="buckets__list__actions">
            <button slot="dropdown-button" id="dropdown-bucket-actions" class="dropdown-toggle btn btn-default" type="button">
              <i class="zmdi zmdi-more-vert"></i>
            </button>

            <li><a @click="showPolicy(bucket.name)">Edit policy</a></li>
            <li><a @click="deleteBucket(bucket.name)">Delete</a></li>
          </dropdown>
        </li>
      </ul>
    </div>

    <div class="browser-status">
      <div class="browser-status__storage">
        <small>{{ usage.humanUsed }} of {{ usage.humanTotal }} Used</small>
        <div class="browser-status__chart">
          <div v-bind:style="{ width: usage.usedPercent }"></div>
        </div>
      </div>
    </div>
  </aside>
</template>

<script>
import filesize from 'file-size'

import { dropdown } from 'vue-strap'

import { minioBrowserPrefix } from '../constants'

export default {
  components: {
    'dropdown': dropdown
  },

  data: function() {
    return {
      searchQuery: '',
      storage: {
        total: 0,
        free: 0
      }
    }
  },

  computed: {
    buckets: function() {
      const currentBucket = this.$store.state.currentBucket

      return this.$store.state.buckets.filter(bucket => {
        // Simple search algorithm: if it contains our search query, show it.
        return bucket.indexOf(this.searchQuery) != -1
      }).map(bucket => {
        // Convert it into an object with the relevant fields.
        return {
          name: bucket,
          policy: 'read and write',
          isCurrent: (bucket == currentBucket)
        }
      })
    },

    host: function() {
      return window.location.host
    },

    sideBarActive: function() {
      return this.$store.state.sideBarActive
    },

    usage: function() {
      const total = this.storage.total
      const free = this.storage.free
      const used = total - free

      return {
        total, free, used,
        humanUsed: filesize(used).human(),
        humanTotal: filesize(total).human(),
        usedPercent: ((used / total) * 100) + '%'
      }
    }
  },

  methods: {
    showPolicy: function(bucket) {
      this.$store.commit('setModalStatus', {
        modal: 'policy',
        status: {
          show: true,
          bucket: bucket
        }
      })
    },

    selectBucket: function(bucket) {
      this.$router.push(minioBrowserPrefix + '/bucket/' + bucket.name)
    },

    deleteBucket: function(bucket) {
      const store = this.$store

      // DeleteBucket() RPC call will ONLY delete a bucket if it is empty of
      // objects. This means a call can just be sent, as it is entirely reversable
      // and won't do any permanent damage.
      store.state.web.DeleteBucket({
        bucketName: bucket
      })
        .then(() => {
          store.dispatch('showAlert', {
            type: 'info',
            message: `Bucket '${bucket}'' has been deleted.`
          })
          store.commit('removeBucket', bucket)
        })
        .catch(err => {
          // Show a custom "bucket not empty" message, as it can be confusing.
          if (/Bucket not empty/.test(err.message)) {
            err.message = `Bucket '${bucket}' must be empty to delete.`
          }

          store.dispatch('error', err)
        })
    },

    loadStorageInfo: function() {
      const web = this.$store.state.web

      web.StorageInfo()
        .then(res => {
          this.storage = {
            total: res.storageInfo.Total,
            free: res.storageInfo.Free
          }
        })
        .catch(err => this.$store.dispatch('error', err))
    }
  },

  // created is a lifecycle hook that will fire when the SideBar is created.
  created() {
    this.$store.dispatch('loadBuckets')

    this.loadStorageInfo()
  }
}
</script>
