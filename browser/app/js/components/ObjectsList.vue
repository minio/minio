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
  <div class="objects">
    <header class="objects__row" data-type="folder">
      <div class="objects__item objects__item--name" v-on:click="sort('name')" data-sort="name">
        Name
        <i class="objects__item__sort zmdi" v-bind:class="{ 'zmdi-sort-desc': sortColumn == 'name', 'zmdi-sort-asc': reverse }" />
      </div>
      <div class="objects__item objects__item--size" v-on:click="sort('size')" data-sort="size">
        Size
        <i class="objects__item__sort zmdi" v-bind:class="{ 'zmdi-sort-amount-desc': sortColumn == 'size', 'zmdi-sort-amount-asc': reverse }" />
      </div>
      <div class="objects__item objects__item--modified" v-on:click="sort('date')" data-sort="last-modified">
        Last Modified
        <i class="objects__item__sort zmdi" v-bind:class="{ 'zmdi-sort-amount-desc': sortColumn == 'date', 'zmdi-sort-amount-asc': reverse }" />
      </div>
    </header>
    <!--</InfiniteScroll>
      <div class="text-center" style={ { display: (istruncated && currentBucket) ? 'block' : 'none' } }>
        <span>Loading...</span>
      </div>
    <!-</Dropzone>-->
    <div class="objects__container">
      <div v-for="object in objects" class="objects__row" v-bind:class="{ 'objects__row--folder': object.isFolder, 'objects__row-selected': object.isChecked }" v-on:click="preview(object.name)">
        <div class="objects__item objects__item--select" :data-object-type="object.type">
          <div class="checkbox">
            <input type="checkbox"
              :name="object.name"
              :checked="object.isChecked"
              v-on:change="checkObject(object.name)" />
            <i class="checkbox__helper" />
          </div>
        </div>
        <div class="objects__item objects__item--name">
          <a href="#" v-on:click.prevent="selectPrefix(object.path)">
            {{ object.name }}
          </a>
        </div>
        <div class="objects__item objects__item--size">
          {{ object.size }}
        </div>
        <div class="objects__item objects__item--modified">
          {{ object.lastModified }}
        </div>
      </div>
    </div>
  <!--<Dropzone>-
    <InfiniteScroll loadMore={ this.listObjects.bind(this) }
      hasMore={ istruncated }
      useWindow={ true }
      initialLoad={ false }>-->
  </div>
</template>

<script>
import { mapGetters } from 'vuex'

/*
TODO: These need to be implemented in their respective Vuejs alternatives.
import Dropzone from './Dropzone.vue'
import CopyToClipboard from 'react-copy-to-clipboard'
import InfiniteScroll from 'react-infinite-scroller'
*/

import MaterialDesignIconicFonts from 'material-design-iconic-font/dist/css/material-design-iconic-font.min.css'

import * as utils from '../utils'

export default {
  name: 'ObjectsList',

  computed: {
    objects: function() {
      const objects = this.$store.getters.objects

      switch (this.sortColumn) {
      case 'name':
        return utils.sortObjectsByName(objects, this.reverse)
      case 'size':
        return utils.sortObjectsBySize(objects, this.reverse)
      case 'date':
        return utils.sortObjectsByDate(objects, this.reverse)
      }
    }
  },

  data: function() {
    return {
      sortColumn: 'name',
      reverse: false
    }
  },

  methods: {
    selectPrefix: function(prefix) {
      const store = this.$store

      const web = store.state.web
      const currentPath = store.state.currentPath
      const currentBucket = store.state.currentBucket

      prefix = encodeURI(prefix)

      if (prefix.endsWith('/') || prefix === '') {
        // TODO what?
        if (prefix === currentPath) return
        browserHistory.push(utils.pathJoin(currentBucket, prefix))
      } else {
        // Download the selected file.
        web.CreateURLToken()
          .then(res => {
            window.location = `${window.location.origin}/minio/download/${currentBucket}/${prefix}?token=${res.token}`
          })
          .catch(err => store.dispatch('error', err))
      }
    },

    checkObject: function(object, e) {
      const isChecked = (this.$store.state.checkedObjects.indexOf(object) > -1)

      if (isChecked)
        this.$store.commit('removeCheckedObject', object)
      else
        this.$store.commit('addCheckedObject', object)
    },

    preview: function(objectName) {
      const bucket = this.$store.state.currentBucket

      this.$store.commit('setPreviewStatus', {
        show: true,
        bucket: bucket,
        object: objectName
      })
    },

    loadObjects: function() {
      const store = this.$store
      const bucket = this.$route.params.bucket

      if (!bucket) return

      const currentPath = store.state.currentPath
      const marker = store.state.marker

      // Update the current bucket if it changed.
      if (store.state.currentBucket != bucket)
        store.commit('setCurrentBucket', bucket)

      store.state.web.ListObjects({
        bucketName: bucket,
        prefix: currentPath,
        marker: marker
      })
        .then(res => {
          // There are no objects, empty bucket.
          if (!res.objects)
            return this.$store.commit('setObjects', {
              objects: [], istruncated: false, marker: res.marker
            })

          const objects = res.objects.map(object => {
            object.name = object.name.replace(currentPath, '')

            return object
          })
          const istruncated = res.istruncated
          const marker = res.marker

          this.$store.commit('setObjects', {
            objects, istruncated, marker
          })
        })
        .catch(err => store.dispatch('error', err))
    },

    sort: function(type) {
      // If they aren't changing the column, they're reversing the order.
      if (this.sortColumn == type)
        this.reverse = !this.reverse

      this.sortColumn = type
    }
  },

  // created is a lifecycle hook that will fire when the ObjectsList is created.
  created() {
    this.loadObjects()
  },

  // if the route changes, update the objects
  watch: {
    '$route': 'loadObjects'
  }
}
</script>
