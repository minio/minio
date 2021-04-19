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
