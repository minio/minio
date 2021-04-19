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

import configureStore from "redux-mock-store"
import thunk from "redux-thunk"
import * as actionsCommon from "../actions"

jest.mock("../../web", () => ({
  StorageInfo: jest.fn(() => {
    return Promise.resolve({
      used: 60
    })
  }),
  ServerInfo: jest.fn(() => {
    return Promise.resolve({
      MinioVersion: "test",
      MinioPlatform: "test",
      MinioRuntime: "test",
      MinioGlobalInfo: "test"
    })
  })
}))

const middlewares = [thunk]
const mockStore = configureStore(middlewares)

describe("Common actions", () => {
  it("creates common/SET_STORAGE_INFO after fetching the storage details ", () => {
    const store = mockStore()
    const expectedActions = [
      { type: "common/SET_STORAGE_INFO", storageInfo: { used: 60 } }
    ]
    return store.dispatch(actionsCommon.fetchStorageInfo()).then(() => {
      const actions = store.getActions()
      expect(actions).toEqual(expectedActions)
    })
  })

  it("creates common/SET_SERVER_INFO after fetching the server details", () => {
    const store = mockStore()
    const expectedActions = [
      {
        type: "common/SET_SERVER_INFO",
        serverInfo: {
          version: "test",
          platform: "test",
          runtime: "test",
          info: "test"
        }
      }
    ]
    return store.dispatch(actionsCommon.fetchServerInfo()).then(() => {
      const actions = store.getActions()
      expect(actions).toEqual(expectedActions)
    })
  })
})
