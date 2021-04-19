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

import React from "react"
import { shallow } from "enzyme"
import { StorageInfo } from "../StorageInfo"

describe("StorageInfo", () => {
  it("should render without crashing", () => {
    shallow(
      <StorageInfo storageInfo={ {used: 60} } fetchStorageInfo={jest.fn()} />
    )
  })

  it("should fetchStorageInfo before component is mounted", () => {
    const fetchStorageInfo = jest.fn()
    shallow(
      <StorageInfo
        storageInfo={ {used: 60} }
        fetchStorageInfo={fetchStorageInfo}
      />
    )
    expect(fetchStorageInfo).toHaveBeenCalled()
  })

  it("should not render anything if used is null", () => {
    const fetchStorageInfo = jest.fn()
    const wrapper = shallow(
      <StorageInfo
      storageInfo={ {used: 0} }
        fetchStorageInfo={fetchStorageInfo}
      />
    )
    expect(wrapper.text()).toBe("")
  })
})
