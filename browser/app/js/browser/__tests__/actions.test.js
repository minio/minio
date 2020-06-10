/*
 * MinIO Cloud Storage (C) 2018 MinIO, Inc.
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
