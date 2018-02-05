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

import reducer from "../objects"
import * as actions from "../../actions/objects"

describe("objects reducer", () => {
  it("should return the initial state", () => {
    const initialState = reducer(undefined, {})
    expect(initialState).toEqual({
      list: [],
      sortBy: "",
      sortOrder: false,
      currentPrefix: ""
    })
  })

  it("should handle SET_LIST", () => {
    const newState = reducer(undefined, {
      type: actions.SET_LIST,
      objects: [{ name: "obj1" }, { name: "obj2" }]
    })
    expect(newState.list).toEqual([{ name: "obj1" }, { name: "obj2" }])
  })

  it("should handle SET_SORT_BY", () => {
    const newState = reducer(undefined, {
      type: actions.SET_SORT_BY,
      sortBy: "name"
    })
    expect(newState.sortBy).toEqual("name")
  })

  it("should handle SET_SORT_ORDER", () => {
    const newState = reducer(undefined, {
      type: actions.SET_SORT_ORDER,
      sortOrder: true
    })
    expect(newState.sortOrder).toEqual(true)
  })

  it("should handle SET_CURRENT_PREFIX", () => {
    const newState = reducer(undefined, {
      type: actions.SET_CURRENT_PREFIX,
      prefix: "test"
    })
    expect(newState.currentPrefix).toEqual("test")
  })
})
