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

describe("objects reducer", () => {
  it("should return the initial state", () => {
    const initialState = reducer(undefined, {})
    expect(initialState).toEqual({
      list: [],
      sortBy: "",
      sortOrder: false,
      currentPrefix: "",
      marker: "",
      isTruncated: false,
      prefixWritable: false,
      shareObject: {
        show: false,
        object: "",
        url: ""
      },
      checkedList: []
    })
  })

  it("should handle SET_LIST", () => {
    const newState = reducer(undefined, {
      type: actions.SET_LIST,
      objects: [{ name: "obj1" }, { name: "obj2" }],
      marker: "obj2",
      isTruncated: false
    })
    expect(newState.list).toEqual([{ name: "obj1" }, { name: "obj2" }])
    expect(newState.marker).toBe("obj2")
    expect(newState.isTruncated).toBeFalsy()
  })

  it("should handle APPEND_LIST", () => {
    const newState = reducer(
      {
        list: [{ name: "obj1" }, { name: "obj2" }],
        marker: "obj2",
        isTruncated: true
      },
      {
        type: actions.APPEND_LIST,
        objects: [{ name: "obj3" }, { name: "obj4" }],
        marker: "obj4",
        isTruncated: false
      }
    )
    expect(newState.list).toEqual([
      { name: "obj1" },
      { name: "obj2" },
      { name: "obj3" },
      { name: "obj4" }
    ])
    expect(newState.marker).toBe("obj4")
    expect(newState.isTruncated).toBeFalsy()
  })

  it("should handle REMOVE", () => {
    const newState = reducer(
      { list: [{ name: "obj1" }, { name: "obj2" }] },
      {
        type: actions.REMOVE,
        object: "obj1"
      }
    )
    expect(newState.list).toEqual([{ name: "obj2" }])
  })

  it("should handle REMOVE with non-existent object", () => {
    const newState = reducer(
      { list: [{ name: "obj1" }, { name: "obj2" }] },
      {
        type: actions.REMOVE,
        object: "obj3"
      }
    )
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
    const newState = reducer(
      { currentPrefix: "test1/", marker: "abc", isTruncated: true },
      {
        type: actions.SET_CURRENT_PREFIX,
        prefix: "test2/"
      }
    )
    expect(newState.currentPrefix).toEqual("test2/")
    expect(newState.marker).toEqual("")
    expect(newState.isTruncated).toBeFalsy()
  })

  it("should handle SET_PREFIX_WRITABLE", () => {
    const newState = reducer(undefined, {
      type: actions.SET_PREFIX_WRITABLE,
      prefixWritable: true
    })
    expect(newState.prefixWritable).toBeTruthy()
  })

  it("should handle SET_SHARE_OBJECT", () => {
    const newState = reducer(undefined, {
      type: actions.SET_SHARE_OBJECT,
      show: true,
      object: "a.txt",
      url: "test"
    })
    expect(newState.shareObject).toEqual({
      show: true,
      object: "a.txt",
      url: "test"
    })
  })

  it("should handle CHECKED_LIST_ADD", () => {
    const newState = reducer(undefined, {
      type: actions.CHECKED_LIST_ADD,
      object: "obj1"
    })
    expect(newState.checkedList).toEqual(["obj1"])
  })

  it("should handle SELECTED_LIST_REMOVE", () => {
    const newState = reducer(
      { checkedList: ["obj1", "obj2"] },
      {
        type: actions.CHECKED_LIST_REMOVE,
        object: "obj1"
      }
    )
    expect(newState.checkedList).toEqual(["obj2"])
  })

  it("should handle CHECKED_LIST_RESET", () => {
    const newState = reducer(
      { checkedList: ["obj1", "obj2"] },
      {
        type: actions.CHECKED_LIST_RESET
      }
    )
    expect(newState.checkedList).toEqual([])
  })
})
