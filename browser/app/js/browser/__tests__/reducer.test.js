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
import * as actionsCommon from "../actions"

describe("common reducer", () => {
  it("should return the initial state", () => {
    expect(reducer(undefined, {})).toEqual({
      sidebarOpen: false,
      storageInfo: {used: 0},
      serverInfo: {}
    })
  })

  it("should handle TOGGLE_SIDEBAR", () => {
    expect(
      reducer(
        { sidebarOpen: false },
        {
          type: actionsCommon.TOGGLE_SIDEBAR
        }
      )
    ).toEqual({
      sidebarOpen: true
    })
  })

  it("should handle CLOSE_SIDEBAR", () => {
    expect(
      reducer(
        { sidebarOpen: true },
        {
          type: actionsCommon.CLOSE_SIDEBAR
        }
      )
    ).toEqual({
      sidebarOpen: false
    })
  })

  it("should handle SET_STORAGE_INFO", () => {
    expect(
      reducer(
        {},
        {
          type: actionsCommon.SET_STORAGE_INFO,
          storageInfo: { }
        }
      )
    ).toEqual({
      storageInfo: { }
    })
  })

  it("should handle SET_SERVER_INFO", () => {
    expect(
      reducer(undefined, {
        type: actionsCommon.SET_SERVER_INFO,
        serverInfo: {
          version: "test",
          platform: "test",
          runtime: "test",
          info: "test"
        }
      }).serverInfo
    ).toEqual({
      version: "test",
      platform: "test",
      runtime: "test",
      info: "test"
    })
  })
})
