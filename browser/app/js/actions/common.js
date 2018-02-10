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

import web from "../web"

export const TOGGLE_SIDEBAR = "common/TOGGLE_SIDEBAR"
export const CLOSE_SIDEBAR = "common/CLOSE_SIDEBAR"
export const SET_STORAGE_INFO = "common/SET_STORAGE_INFO"

export const toggleSidebar = () => ({
  type: TOGGLE_SIDEBAR
})

export const closeSidebar = () => ({
  type: CLOSE_SIDEBAR
})

export const fetchStorageInfo = () => {
  return function(dispatch) {
    return web.StorageInfo().then(res => {
      const storageInfo = {
        total: res.storageInfo.Total,
        free: res.storageInfo.Free
      }
      dispatch(setStorageInfo(storageInfo))
    })
  }
}

export const setStorageInfo = storageInfo => ({
  type: SET_STORAGE_INFO,
  storageInfo
})
