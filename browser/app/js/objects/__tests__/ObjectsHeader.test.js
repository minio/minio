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
import { ObjectsHeader } from "../ObjectsHeader"
import { SORT_ORDER_ASC, SORT_ORDER_DESC } from "../../constants"

describe("ObjectsHeader", () => {
  it("should render without crashing", () => {
    const sortObjects = jest.fn()
    shallow(<ObjectsHeader sortObjects={sortObjects} />)
  })

  it("should render the name column with asc class when objects are sorted by name asc", () => {
    const sortObjects = jest.fn()
    const wrapper = shallow(
      <ObjectsHeader
        sortObjects={sortObjects}
        sortedByName={true}
        sortOrder={SORT_ORDER_ASC}
      />
    )
    expect(
      wrapper.find("#sort-by-name i").hasClass("fa-sort-alpha-down")
    ).toBeTruthy()
  })

  it("should render the name column with desc class when objects are sorted by name desc", () => {
    const sortObjects = jest.fn()
    const wrapper = shallow(
      <ObjectsHeader
        sortObjects={sortObjects}
        sortedByName={true}
        sortOrder={SORT_ORDER_DESC}
      />
    )
    expect(
      wrapper.find("#sort-by-name i").hasClass("fa-sort-alpha-down-alt")
    ).toBeTruthy()
  })

  it("should render the size column with asc class when objects are sorted by size asc", () => {
    const sortObjects = jest.fn()
    const wrapper = shallow(
      <ObjectsHeader
        sortObjects={sortObjects}
        sortedBySize={true}
        sortOrder={SORT_ORDER_ASC}
      />
    )
    expect(
      wrapper.find("#sort-by-size i").hasClass("fa-sort-amount-down-alt")
    ).toBeTruthy()
  })

  it("should render the size column with desc class when objects are sorted by size desc", () => {
    const sortObjects = jest.fn()
    const wrapper = shallow(
      <ObjectsHeader
        sortObjects={sortObjects}
        sortedBySize={true}
        sortOrder={SORT_ORDER_DESC}
      />
    )
    expect(
      wrapper.find("#sort-by-size i").hasClass("fa-sort-amount-down")
    ).toBeTruthy()
  })

  it("should render the date column with asc class when objects are sorted by date asc", () => {
    const sortObjects = jest.fn()
    const wrapper = shallow(
      <ObjectsHeader
        sortObjects={sortObjects}
        sortedByLastModified={true}
        sortOrder={SORT_ORDER_ASC}
      />
    )
    expect(
      wrapper.find("#sort-by-last-modified i").hasClass("fa-sort-numeric-down")
    ).toBeTruthy()
  })

  it("should render the date column with desc class when objects are sorted by date desc", () => {
    const sortObjects = jest.fn()
    const wrapper = shallow(
      <ObjectsHeader
        sortObjects={sortObjects}
        sortedByLastModified={true}
        sortOrder={SORT_ORDER_DESC}
      />
    )
    expect(
      wrapper.find("#sort-by-last-modified i").hasClass("fa-sort-numeric-down-alt")
    ).toBeTruthy()
  })

  it("should call sortObjects when a column is clicked", () => {
    const sortObjects = jest.fn()
    const wrapper = shallow(<ObjectsHeader sortObjects={sortObjects} />)
    wrapper.find("#sort-by-name").simulate("click")
    expect(sortObjects).toHaveBeenCalledWith("name")
    wrapper.find("#sort-by-size").simulate("click")
    expect(sortObjects).toHaveBeenCalledWith("size")
    wrapper.find("#sort-by-last-modified").simulate("click")
    expect(sortObjects).toHaveBeenCalledWith("last-modified")
  })
})
