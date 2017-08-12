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
  <modal :small="true" :value="show" title="Change Password" @closed="hide">
    <div>
      <div class="form-group">
        <label class="form-group__label">
          Access key
        </label>
        <input type="text"
          class="form-group__field"
          id="accessKey"
          name="accesskey"
          :value="accessKey"
          :disabled="keysReadOnly" />
        <i class="form-group__bar"></i>
      </div>

      <div class="form-group">
        <label class="form-group__label">
          Secret key
        </label>
        <div class="form-group__wrap">
          <input :type="secretKeyVisible ? 'text' : 'password'"
            class="form-group__field"
            id="secretKey"
            name="secretKey"
            :value="secretKey"
            :disabled="keysReadOnly" />
          <div @click="secretKeyVisible = !secretKeyVisible" class="form-group__addon">
            <i class="form-group__toggle-type" :class="{ 'toggled': secretKeyVisible }"></i>
          </div>
          <i class="form-group__bar"></i>
        </div>
      </div>
    </div>

    <div class="modal-footer" slot="modal-footer">
      <button class="btn btn--link" :class="{ 'hidden': keysReadOnly }" @click="generateAuth">
        Generate
      </button>
      <button class="btn btn--link" :class="{ 'hidden': keysReadOnly }" @click="setAuth">
        Update
      </button>
    </div>
  </modal>
</template>


<script>
import { mapState } from 'vuex'
import { modal } from 'vue-strap'

export default {
  name: 'SettingsModal',

  components: {
    'modal': modal
  },

  data: function() {
    return {
      keysReadOnly: false,
      accessKey: '',
      secretKey: '',
      secretKeyVisible: false
    }
  },

  computed: mapState({
    show: state => state.modals.settings.show
  }),

  methods: {
    // Save the auth params and set them.
    setAuth: function() {
      const web = this.$store.state.web

      this.hide()

      web.SetAuth({
        accessKey: this.accessKey,
        secretKey: this.secretKey
      })
        .then(data => {
          this.$store.dispatch('showAlert', {
            type: 'success',
            message: 'Changed credentials'
          })
        })
        .catch(err => this.$store.dispatch('error', err))
    },

    generateAuth: function() {
      const web = this.$store.state.web

      web.GenerateAuth()
        .then(data => {
          this.secretKeyVisible = true
          this.accessKey = data.accessKey
          this.secretKey = data.secretKey
        })
    },

    load: function() {
      const web = this.$store.state.web

      web.ServerInfo()
        .then(res => {
          // Check environment variables first. They may or may not have been
          // loaded already.
          if (res.MinioGlobalInfo.isEnvCreds) {
            // We don't display them.
            this.accessKey = 'xxxxxxxxx'
            this.secretKey = 'xxxxxxxxx'
            this.keysReadOnly = true

            return
          }

          return web.GetAuth()
        })
        .then(data => {
          if (!data) return

          this.accessKey = data.accessKey
          this.secretKey = data.secretKey
        })
        .catch(err => this.$store.dispatch('error', err))
    },

    hide: function() {
      this.$store.commit('setModalStatus', {
        modal: 'settings',
        status: {
          show: false
        }
      })
    }
  },

  created() {
    this.load()
  }
}
</script>
