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

<template v-if="canPreview">
  <img :src="dataUrl" alt="Cannot display thumbnail (not an image)." />
</template>

<script>
import storage from 'local-storage-fallback'
import mime from 'mime-types'

// Thumbnail is an asynchronously loaded thumbnail image.
export default {
  name: 'Thumbnail',

  props: ['bucket', 'object'],

  data: function() {
    return {
      dataUrl: '#'
    }
  },

  computed: {
    url: function() {
      return `${window.location.origin}/minio/thumbnail/${this.bucket}/${this.object}`
    }
  },

  methods: {
    load: function() {
      // Set it to # to make loading more pleasant.
      this.dataUrl = '#'

      if (!this.canPreview(this.object))
        return

      let xhr = new XMLHttpRequest()
      xhr.open('GET', this.url, true)
      xhr.responseType = 'blob'

      xhr.setRequestHeader("Authorization", 'Bearer ' + storage.getItem('token'))
      xhr.send()

      xhr.onload = (response) => {
        if (xhr.status != 200) {
          // Oops. This might mean the image isn't formatted correctly, but
          // has an image file extension.
          return
        }

        this.dataUrl = window.URL.createObjectURL(xhr.response)
      }
    },

    canPreview() {
      // Returns a boolean, whether or not we can preview this file.
      const acceptable = ['image/jpeg', 'image/png', 'image/gif']
      const type = mime.lookup(this.object)

      return (acceptable.indexOf(type) != -1)
    }
  },

  created() {
    this.load()
  },

  // if the bucket/object change, re-load
  watch: {
    'bucket': 'load',
    'object': 'load'
  }
}
</script>
