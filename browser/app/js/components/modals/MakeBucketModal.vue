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
  <modal class="create-bucket" :small="true" :value="show" @close="hide">
    <div slot="modal-header"></div>

    <form v-on:submit="makeBucket">
      <div class="form-group">
        <label class="form-group__label">
          Create new bucket
        </label>
        <input class="form-group__field"
          type="text"
          v-model="bucketName"
          placeholder="e.g documents"
          autoFocus/>
        <i class="form-group__bar" />
      </div>
      <div class="text-right">
        <input type="submit" class="btn btn--link" value="Create" />
        <button class="btn btn--link" v-on:click="hide">
          Cancel
        </button>
      </div>
    </form>

    <div slot="modal-footer"></div>
  </modal>
</template>

<script>
import { modal } from 'vue-strap'
import { mapState } from 'vuex'

export default {
  name: 'CreateBucketModal',

  components: {
    'modal': modal
  },

  data: function() {
    return {
      bucketName: ''
    }
  },

  computed: mapState({
    show: state => state.modals['make-bucket'].show
  }),

  methods: {
    hide: function() {
      this.$store.commit('setModalStatus', {
        modal: 'make-bucket',
        status: {
          show: false
        }
      })
    },

    makeBucket: function() {
      const bucketName = this.bucketName
      const web = this.$store.state.web

      this.hide()

      web.MakeBucket({
        bucketName
      })
        .then(() => {
          this.$store.commit('addBucket', bucketName)

          this.$store.dispatch('showAlert', {
            type: 'success',
            message: `Bucket '${bucketName}' has been created.`
          })

          this.bucketName = ''
        })
        .catch(err => this.$store.dispatch('error', err))
    }
  }
}
</script>
