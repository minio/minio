/*
 * Minio Cloud Storage (C) 2018 Minio, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import * as uploadsActions from "./actions"

const add = (files, action) => ({
  ...files,
  [action.slug]: {
    loaded: 0,
    size: action.size,
    name: action.name
  }
})

const updateProgress = (files, action) => ({
  ...files,
  [action.slug]: {
    ...files[action.slug],
    loaded: action.loaded
  }
})

const stop = (files, action) => {
  const newFiles = Object.assign({}, files)
  delete newFiles[action.slug]
  return newFiles
}

export default (state = { files: {}, showAbortModal: false }, action) => {
  switch (action.type) {
    case uploadsActions.ADD:
      return {
        ...state,
        files: add(state.files, action)
      }
    case uploadsActions.UPDATE_PROGRESS:
      return {
        ...state,
        files: updateProgress(state.files, action)
      }
    case uploadsActions.STOP:
      return {
        ...state,
        files: stop(state.files, action)
      }
    case uploadsActions.SHOW_ABORT_MODAL:
      return {
        ...state,
        showAbortModal: action.show
      }
    default:
      return state
  }
}
