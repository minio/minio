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
  <div class="policy__list">
    <div class="policy__item">
      {{ readablePrefix }}
    </div>
    <div class="policy__item">
      <select disabled :value="newPolicy">
        <option :value="constants.READ_ONLY">
          Read Only
        </option>
        <option :value="constants.WRITE_ONLY">
          Write Only
        </option>
        <option :value="constants.READ_WRITE">
          Read and Write
        </option>
      </select>
    </div>
    <div class="policy__item">
      <button class="btn btn--block btn--danger" v-on:click="removePolicy">
        Remove
      </button>
    </div>
  </div>
</template>

<script>
import { READ_ONLY, WRITE_ONLY, READ_WRITE } from '../../constants'

export default {
  name: 'PolicyView',

  props: ['prefix', 'policy', 'bucket'],

  data: function() {
    return {
      newPolicy: this.policy
    }
  },

  computed: {
    // readablePrefix is a way of showing the user their created prefixes
    readablePrefix: function() {
      return this.prefix.replace(this.bucket + '/', '')
    },

    constants: function() {
      return {
        READ_ONLY, WRITE_ONLY, READ_WRITE
      }
    }
  },

  methods: {
    removePolicy: function(e) {
      const web = this.$store.state.web

      web.SetBucketPolicy({
        bucketName: this.bucket,
        prefix: this.prefix,
        policy: 'none'
      })
        .then(() => {
          this.$store.commit('removePolicy', { prefix: this.prefix })
        })
        .catch(e => this.$store.dispatch('error', e))
    },
  }
}
</script>
