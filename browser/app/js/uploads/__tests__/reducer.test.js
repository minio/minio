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
