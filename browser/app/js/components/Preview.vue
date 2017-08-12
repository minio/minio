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
  <aside class="preview" v-bind:class="{ 'preview--toggled': previewStatus.show }">
    <div class="preview__header">
      <span class="preview__label">{{ previewStatus.object }}</span>
      <i class="preview__close zmdi zmdi-long-arrow-left" v-on:click="hidePreview" />
    </div>
    <div class="preview__body">
      <div class="preview__item preview__item--img">
        <thumbnail :bucket="previewStatus.bucket" :object="previewStatus.object" />
      </div>
      <div class="preview__info">
        <dl>
          <dt>Type</dt>
          <dd>
            {{ info.type }}
          </dd>
          <dt>Size</dt>
          <dd>
            {{ info.size }}
          </dd>
          <dt>Modified</dt>
          <dd>
            {{ info.lastModified }}
          </dd>
          <dt>Policy</dt>
          <dd>
            Read and Write
          </dd>
          <dt>Shared</dt>
          <dd>
            No
          </dd>
        </dl>
      </div>
      <div class="preview__actions">
        <button class="btn btn--lg btn--primary">
          Download
        </button>
      </div>
    </div>
  </aside>
</template>

<script>
import Thumbnail from './Thumbnail.vue'

// Preview is the side panel for previewing info about objects.
export default {
  name: 'Preview',

  components: {
    'thumbnail': Thumbnail
  },

  computed: {
    previewStatus: function() {
      return this.$store.state.previewStatus
    },

    info: function() {
      const name = this.$store.state.previewStatus.object
      const objects = this.$store.getters.objects

      // Get the object's information.
      // It should be the first (and only) object whose name is the same.
      const matches = objects.filter(obj => obj.name == name)

      // If we don't have any matches, it's loading or unloading. Show ellipses
      // in this case so it doesn't look weird.
      return matches[0] || {
        type: '...',
        size: '...',
        lastModified: '...'
      }
    }
  },

  methods: {
    hidePreview: function() {
      this.$store.commit('setPreviewStatus', {
        show: false,
        bucket: '',
        object: ''
      })
    }
  }
}
</script>
