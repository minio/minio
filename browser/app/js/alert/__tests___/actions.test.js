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
import * as actionsAlert from "../actions"

const middlewares = [thunk]
const mockStore = configureStore(middlewares)

jest.useFakeTimers()

describe("Alert actions", () => {
  it("creates alert/SET action", () => {
    const store = mockStore()
    const expectedActions = [
      {
        type: "alert/SET",
        alert: { id: 0, message: "Test alert", type: "danger" }
      }
    ]
    store.dispatch(actionsAlert.set({ message: "Test alert", type: "danger" }))
    const actions = store.getActions()
    expect(actions).toEqual(expectedActions)
  })

  it("creates alert/CLEAR action for non danger alerts", () => {
    const store = mockStore()
    const expectedActions = [
      {
        type: "alert/SET",
        alert: { id: 1, message: "Test alert" }
      },
      {
        type: "alert/CLEAR",
        alert: { id: 1 }
      }
    ]
    store.dispatch(actionsAlert.set({ message: "Test alert" }))
    jest.runAllTimers()
    const actions = store.getActions()
    expect(actions).toEqual(expectedActions)
  })

  it("creates alert/CLEAR action directly", () => {
    const store = mockStore()
    const expectedActions = [
      {
        type: "alert/CLEAR"
      }
    ]
    store.dispatch(actionsAlert.clear())
    const actions = store.getActions()
    expect(actions).toEqual(expectedActions)
  })
})
