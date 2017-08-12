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
  <header class="policy__list">
    <div class="policy__item">
      <div class="form-group">
        <input type="text"
          v-model="prefix" class="form-group__field form-group__field--sm" placeholder="Prefix" />
        <i class="form-group__bar" />
      </div>
    </div>
    <div class="policy__item">
      <select v-model="policy" class="form-group__field form-group__field--sm">
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
      <button class="btn btn--block btn--primary" @click="addPolicy">
        Add
      </button>
    </div>
  </header>
</template>

<script>
import { mapState } from 'vuex'

import { READ_ONLY, WRITE_ONLY, READ_WRITE } from '../../constants'

export default {
  name: 'PolicyInput',

  props: ['defaultPrefix', 'defaultPolicy', 'bucket'],

  data: function() {
    return {
      prefix: this.defaultPrefix,
      policy: this.defaultPolicy
    }
  },

  computed: Object.assign({
    constants: function() {
      return {
        READ_ONLY, WRITE_ONLY, READ_WRITE
      }
    }
  }, mapState({
    policies: state => state.policies
  })),

  methods: {
    addPolicy: function() {
      const prefix = this.prefix
      const policy = this.policy

      const state = this.$store.state

      state.web.SetBucketPolicy({
        bucketName: this.bucket,
        prefix, policy
      })
        .then(() => {
          this.$store.commit('addPolicy', {
            prefix,
            policy
          })

          this.prefix = this.defaultPrefix
          this.policy = this.defaultPolicy
        })
        .catch(e => state.dispatch('error', e))
    }
  }
}
</script>
