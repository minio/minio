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

import configureStore from "redux-mock-store"
import thunk from "redux-thunk"
import * as actionsBuckets from "../buckets"

jest.mock("../../web", () => ({
  ListBuckets: jest.fn(() => {
    return Promise.resolve({ buckets: [{ name: "test1" }, { name: "test2" }] })
  })
}))

const middlewares = [thunk]
const mockStore = configureStore(middlewares)

describe("Buckets actions", () => {
  it("creates buckets/SET_LIST after fetching the buckets", () => {
    const store = mockStore()
    const expectedActions = [
      { type: "buckets/SET_LIST", buckets: ["test1", "test2"] }
    ]
    return store.dispatch(actionsBuckets.fetchBuckets()).then(() => {
      const actions = store.getActions()
      expect(actions).toEqual(expectedActions)
    })
  })
})
