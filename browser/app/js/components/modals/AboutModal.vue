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
  <modal :value="show" @closed="hide">
    <div class="about" slot="modal-body">
      <div class="about__logo">
        <img src="/minio/logo-dark.svg" alt="" />
      </div>
      <div class="about__content">
        <dl class="about__info">
          <dt>Version</dt>
          <dd>
            {{ serverInfo.version }}
          </dd>
          <dt>Memory</dt>
          <dd>
            {{ serverInfo.memory }}
          </dd>
          <dt>Platform</dt>
          <dd>
            {{ serverInfo.platform }}
          </dd>
          <dt>Runtime</dt>
          <dd>
            {{ serverInfo.runtime }}
          </dd>
        </dl>
      </div>
    </div>
    <div slot="modal-footer"></div>
  </modal>
</template>

<script>
import { mapState } from 'vuex'
import { modal } from 'vue-strap'

export default {
  name: 'AboutModal',

  components: {
    'modal': modal
  },

  computed: mapState({
    serverInfo: state => state.serverInfo,

    show: state => state.modals.about.show
  }),

  methods: {
    load: function() {
      const web = this.$store.state.web
      if (!web || !web.LoggedIn()) return

      web.ServerInfo()
        .then(res => {
          let info = {
            version: res.MinioVersion,
            memory: res.MinioMemory,
            platform: res.MinioPlatform,
            runtime: res.MinioRuntime,
            info: res.MinioGlobalInfo
          }

          this.$store.commit('setServerInfo', info)
        })
        .catch(err => this.$store.dispatch('error', err))
    },

    hide: function() {
      this.$store.commit('setModalStatus', {
        modal: 'about',
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
