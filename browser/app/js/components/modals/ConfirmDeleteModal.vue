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
  <confirm-modal @ok="deleteObjects" @cancel="hide"
    :value="show"
    icon="zmdi-alert-polygon c-red"
    sub="This cannot be undone!"
    :text="confirmText"
    okText="Delete"
    cancelText="Cancel" />
</template>

<script>
import { mapState } from 'vuex'

import ConfirmModal from './ConfirmModal.vue'

export default {
  name: 'ConfirmDeleteModal',

  components: {
    'confirm-modal': ConfirmModal
  },

  computed: Object.assign(mapState({
    show: state => state.modals.delete.show
  }), {
    confirmText: function() {
      return `Are you sure you want to delete ${this.prettyObjects}?`
    },

    prettyObjects: function() {
      const objects = this.$store.state.checkedObjects

      // Either "filename" for 1 or 'x files' for multiple.
      return objects.length > 1 ? objects.length + ' files' : `"${objects[0]}"`
    }
  }),

  methods: {
    deleteObjects: function() {
      this.hide()

      const store = this.$store
      const {web, currentPath, currentBucket, deleteConfirmation, checkedObjects} = store.state

      let objects = checkedObjects.map(obj => `${currentPath}${obj}`)

      // This should never happen, but we had a case for it before, so make sure.
      if (!objects)
        return store.dispatch('error', {
          message: 'No objects are selected to delete!'
        })

      web.RemoveObject({
        bucketname: currentBucket,
        objects: objects
      })
        .then(() => {
          store.dispatch('showAlert', {
            type: 'success',
            message: `Successfully deleted ${this.prettyObjects}.`
          })

          for (let object of objects) {
            let name = object.replace(currentPath, '')

            store.dispatch('removeObject', name)
          }
        })
        .catch(err => store.dispatch('error', err))
    },

    hide: function() {
      this.$store.commit('setModalStatus', {
        modal: 'delete',
        status: {
          show: false
        }
      })
    }
  }
}
</script>
