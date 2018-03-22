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

import * as actionsAlert from "./actions"

const initialState = {
  show: false,
  type: "danger"
}
export default (state = initialState, action) => {
  switch (action.type) {
    case actionsAlert.SET:
      return {
        show: true,
        id: action.alert.id,
        type: action.alert.type,
        message: action.alert.message
      }
    case actionsAlert.CLEAR:
      if (action.alert && action.alert.id != state.id) {
        return state
      } else {
        return initialState
      }
    default:
      return state
  }
}
