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
  <nav class="breadcrumb">
    <span v-on:click="selectPrefix('')">{{ currentBucket }}</span>

    <span v-for="path in paths" v-on:click="selectPrefix(path)">{{ path }}</span>
  </nav>
</template>

<script>
// Path is a component which shows the breadcrumb path for buckets and objects.
export default {
  computed: {
    paths() {
      let currentPath = this.$store.state.currentPath
      if (!currentPath) return []

      return currentPath.split('/').map((dir, i, array) => {
        // adds together dir and all previous dirs
        return array.filter((e, index) => index <= i).join('/') + '/'
      })
    },

    currentBucket() {
      return this.$store.state.currentBucket
    }
  },

  methods: {
    selectPrefix: function() {
      // TODO finish
    }
  }
}
</script>
