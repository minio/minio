/*
 * Copyright (c) 2015-2021 MinIO, Inc.
 *
 * This file is part of MinIO Object Storage stack
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

import web from "../web"

export const TOGGLE_SIDEBAR = "common/TOGGLE_SIDEBAR"
export const CLOSE_SIDEBAR = "common/CLOSE_SIDEBAR"
export const SET_STORAGE_INFO = "common/SET_STORAGE_INFO"
export const SET_SERVER_INFO = "common/SET_SERVER_INFO"

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
        used: res.used
      }
      dispatch(setStorageInfo(storageInfo))
    })
  }
}

export const setStorageInfo = storageInfo => ({
  type: SET_STORAGE_INFO,
  storageInfo
})

export const fetchServerInfo = () => {
  return function(dispatch) {
    return web.ServerInfo().then(res => {
      const serverInfo = {
        version: res.MinioVersion,
        platform: res.MinioPlatform,
        runtime: res.MinioRuntime,
        info: res.MinioGlobalInfo,
        userInfo: res.MinioUserInfo
      }
      dispatch(setServerInfo(serverInfo))
    })
  }
}

export const setServerInfo = serverInfo => ({
  type: SET_SERVER_INFO,
  serverInfo
})
