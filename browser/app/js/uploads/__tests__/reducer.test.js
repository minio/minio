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
import * as actions from "../actions"

describe("uploads reducer", () => {
  it("should return the initial state", () => {
    const initialState = reducer(undefined, {})
    expect(initialState).toEqual({
      files: {},
      showAbortModal: false
    })
  })

  it("should handle ADD", () => {
    const newState = reducer(undefined, {
      type: actions.ADD,
      slug: "a-b-c",
      size: 100,
      name: "test"
    })
    expect(newState.files).toEqual({
      "a-b-c": { loaded: 0, size: 100, name: "test" }
    })
  })

  it("should handle UPDATE_PROGRESS", () => {
    const newState = reducer(
      {
        files: { "a-b-c": { loaded: 0, size: 100, name: "test" } }
      },
      {
        type: actions.UPDATE_PROGRESS,
        slug: "a-b-c",
        loaded: 50
      }
    )
    expect(newState.files).toEqual({
      "a-b-c": { loaded: 50, size: 100, name: "test" }
    })
  })

  it("should handle STOP", () => {
    const newState = reducer(
      {
        files: {
          "a-b-c": { loaded: 70, size: 100, name: "test1" },
          "x-y-z": { loaded: 50, size: 100, name: "test2" }
        }
      },
      {
        type: actions.STOP,
        slug: "a-b-c"
      }
    )
    expect(newState.files).toEqual({
      "x-y-z": { loaded: 50, size: 100, name: "test2" }
    })
  })

  it("should handle SHOW_ABORT_MODAL", () => {
    const newState = reducer(
      {
        showAbortModal: false
      },
      {
        type: actions.SHOW_ABORT_MODAL,
        show: true
      }
    )
    expect(newState.showAbortModal).toBeTruthy()
  })
})
