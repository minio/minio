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
  <modal class="policy" :value="show" @closed="hide">
    <div slot="title">
      <span>Bucket Policy</span>
      <small class="modal-header__sub">({{ bucket }})</small>
    </div>
    <div class="policy__body" slot="modal-body">
      <policy-input :bucket="bucket" defaultPrefix="" :defaultPolicy="constants.READ_ONLY" />
      <policy-view v-for="policy in policies" :key="policy.prefix" :prefix="policy.prefix" :policy="policy.policy" :bucket="bucket" />
    </div>
    <div slot="modal-footer"></div>
  </modal>
</template>


<script>
import { modal } from 'vue-strap'
import { mapState } from 'vuex'

import { READ_ONLY, WRITE_ONLY, READ_WRITE } from '../../constants'

import PolicyView from '../policy/PolicyView.vue'
import PolicyInput from '../policy/PolicyInput.vue'

export default {
  name: 'PolicyModal',

  components: {
    'policy-view': PolicyView,
    'policy-input': PolicyInput,
    'modal': modal
  },

  computed: Object.assign({
    constants: function() {
      return {
        READ_ONLY, WRITE_ONLY, READ_WRITE
      }
    }
  }, mapState({
    bucket: state => state.modals.policy.bucket,
    policies: state => state.policies,

    show: state => state.modals.policy.show
  })),

  methods: {
    hide: function() {
      this.$store.commit('setModalStatus', {
        modal: 'policy',
        status: {
          show: false,
          bucket: ''
        }
      })

      this.$store.commit('setPolicies', [])
    },

    load: function() {
      const { web } = this.$store.state

      web.ListAllBucketPolicies({
        bucketName: this.bucket
      }).then(res => {
        if (!res.policies)
          return

        this.$store.commit('setPolicies', res.policies)
      }).catch(err => this.$store.dispatch('error', err))
    }
  },

  created() {
    this.load()
  },

  watch: {
    'bucket': 'load'
  }
}
</script>
