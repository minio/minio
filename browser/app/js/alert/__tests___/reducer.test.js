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

import reducer from "../reducer"
import * as actionsAlert from "../actions"

describe("alert reducer", () => {
  it("should return the initial state", () => {
    expect(reducer(undefined, {})).toEqual({
      show: false,
      type: "danger"
    })
  })

  it("should handle SET_ALERT", () => {
    expect(
      reducer(undefined, {
        type: actionsAlert.SET,
        alert: { id: 1, type: "danger", message: "Test message" }
      })
    ).toEqual({
      show: true,
      id: 1,
      type: "danger",
      message: "Test message"
    })
  })

  it("should clear alert if id not passed", () => {
    expect(
      reducer(
        { show: true, type: "danger", message: "Test message" },
        {
          type: actionsAlert.CLEAR
        }
      )
    ).toEqual({
      show: false,
      type: "danger"
    })
  })

  it("should clear alert if id is matching", () => {
    expect(
      reducer(
        { show: true, id: 1, type: "danger", message: "Test message" },
        {
          type: actionsAlert.CLEAR,
          alert: { id: 1 }
        }
      )
    ).toEqual({
      show: false,
      type: "danger"
    })
  })

  it("should not clear alert if id is not matching", () => {
    expect(
      reducer(
        { show: true, id: 1, type: "danger", message: "Test message" },
        {
          type: actionsAlert.CLEAR,
          alert: { id: 2 }
        }
      )
    ).toEqual({
      show: true,
      id: 1,
      type: "danger",
      message: "Test message"
    })
  })
})
