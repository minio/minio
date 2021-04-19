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
