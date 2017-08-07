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
  <b-modal ref="share_modal" bsSize="small">
    <ModalHeader>
      Share Object
    </ModalHeader>
    <ModalBody>
      <div class="form-group">
        <label class="form-group__label">
          Shareable Link
        </label>
        <input class="form-group__field"
          type="text"
          ref="copyTextInput"
          readOnly="readOnly"
          value={ window.location.protocol + '//' + shareObject.url }
          v-on:click="selectTexts" />
        <i class="form-group__bar" />
      </div>
      <div class="form-group" v-if:style="{ display: isLoggedIn() ? 'block' : 'none' }">
        <label class="form-group__label">
          Expires in
        </label>
        <div class="set-expire">
          <div class="set-expire__item">
            <i class="set-expire__increase" v-on:click="handleExpireValue('expireDays', 1, shareObject.object)"></i>
            <div class="set-expire__title">
              Days
            </div>
            <div class="set-expire__value">
              <input ref="expireDays"
                type="number"
                min={ 0 }
                max={ 7 }
                defaultValue={ 5 } />
            </div>
            <i class="set-expire__decrease" v-on:click="handleExpireValue('expireDays', -1, shareObject.object)"></i>
          </div>
          <div class="set-expire__item">
            <i class="set-expire__increase" v-on:click="handleExpireValue('expireHours', 1, shareObject.object)"></i>
            <div class="set-expire__title">
              Hours
            </div>
            <div class="set-expire__value">
              <input ref="expireHours"
                type="number"
                min={ 0 }
                max={ 23 }
                defaultValue={ 0 } />
            </div>
            <i class="set-expire__decrease" v-on:click="handleExpireValue('expireHours', -1, shareObject.object)"></i>
          </div>
          <div class="set-expire__item">
            <i class="set-expire__increase" v-on:click="handleExpireValue('expireMins', 1, shareObject.object)"></i>
            <div class="set-expire__title">
              Minutes
            </div>
            <div class="set-expire__value">
              <input ref="expireMins"
                type="number"
                min={ 0 }
                max={ 59 }
                defaultValue={ 0 } />
            </div>
            <i class="set-expire__decrease" v-on:click="handleExpireValue('expireMins', -1, shareObject.object)"></i>
          </div>
        </div>
      </div>
    </ModalBody>
    <div class="modal-footer">
      <!--<CopyToClipboard text={ window.location.protocol + '//' + shareObject.url } onCopy={ this.showMessage.bind(this) }>-->
        <button class="btn btn--link">
          Copy Link
        </button>
      <!--</CopyToClipboard>-->
      <button class="btn btn--link" v-on:click="hide">
        Cancel
      </button>
    </div>
  </b-modal>
</template>


<script>
export default {
  name: 'ShareModal',

  props: ['shareObject'],

  computed: {
    isLoggedIn: function() {
      return this.$store.getters.isLoggedIn()
    }
  },

  methods: {
    hide() {
      this.$refs.share_modal.hide()
    },

    show() {
      this.$refs.share_modal.show()
    },

    handleExpireValue: function(targetInput, inc, object) {
      let value = this.refs[targetInput].value
      let maxValue = (targetInput == 'expireHours') ? 23 : (targetInput == 'expireMins') ? 59 : (targetInput == 'expireDays') ? 7 : 0
      value = isNaN(value) ? 0 : value

      // Use custom step count to support browser Edge
      if( (inc === -1) ) {
        if (value != 0) {
          value--
        }
      } else {
        if (value != maxValue) {
          value++
        }
      }
      this.refs[targetInput].value = value

      // Reset hours and mins when days reaches it's max value
      if (this.refs.expireDays.value == 7) {
        this.refs.expireHours.value = 0
        this.refs.expireMins.value = 0
      }
      if (this.refs.expireDays.value + this.refs.expireHours.value + this.refs.expireMins.value == 0) {
        this.refs.expireDays.value = 7
      }

      this.$store.state.dispatch('shareObject', object, this.refs.expireDays.value, this.refs.expireHours.value, this.refs.expireMins.value)
    },
  }
}

</script>
