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
import { ObjectsListContainer } from "../ObjectsListContainer"

describe("ObjectsList", () => {
  it("should render without crashing", () => {
    shallow(<ObjectsListContainer filteredObjects={[]} />)
  })

  it("should render ObjectsList with objects", () => {
    const wrapper = shallow(
      <ObjectsListContainer
        filteredObjects={[{ name: "test1.jpg" }, { name: "test2.jpg" }]}
      />
    )
    expect(wrapper.find("ObjectsList").length).toBe(1)
    expect(wrapper.find("ObjectsList").prop("objects")).toEqual([
      { name: "test1.jpg" },
      { name: "test2.jpg" }
    ])
  })

  it("should show the loading indicator when the objects are being loaded", () => {
    const wrapper = shallow(
      <ObjectsListContainer
        currentBucket="test1"
        filteredObjects={[]}
        listLoading={true}
      />
    )
    expect(wrapper.find(".loading").exists()).toBeTruthy()
  })
})
