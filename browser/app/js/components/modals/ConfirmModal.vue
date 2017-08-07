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
  <modal :small="true" :value="value" @ok="ok" @cancel="cancel" class="modal--confirm">

    <div class="modal--confirm__icon">
      <i class="zmdi" :class="icon" />
    </div>
    <div class="modal--confirm__text">
      {{ text }}
    </div>
    <div class="modal--confirm__sub">
      {{ sub }}
    </div>

    <div slot="modal-footer" class="modal-footer text-center">
      <button class="btn btn--link" @click="ok">{{ okText }}</button>
      <button class="btn btn--link" @click="cancel">{{ cancelText }}</button>
    </div>
  </modal>
</template>


<script>
import { modal } from 'vue-strap'

// ConfirmModal opens a confirm modal, with events 'ok' and 'cancel'.
export default {
  name: 'ConfirmModal',

  props: ['cancelText', 'okText', 'sub', 'text', 'icon', 'value'],

  components: {
    'modal': modal
  },

  methods: {
    cancel: function() {
      this.$emit('cancel')
    },

    ok: function() {
      this.$emit('ok')
    }
  }
}
</script>
