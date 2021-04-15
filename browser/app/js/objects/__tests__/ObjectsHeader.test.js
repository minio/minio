/*
 * MinIO Cloud Storage (C) 2018 MinIO, Inc.
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

import React from "react"
import { shallow } from "enzyme"
import { ObjectsHeader } from "../ObjectsHeader"
import { SORT_ORDER_ASC, SORT_ORDER_DESC } from "../../constants"

describe("ObjectsHeader", () => {
  it("should render without crashing", () => {
    const sortObjects = jest.fn()
    shallow(<ObjectsHeader sortObjects={sortObjects} t={key => key}/>)
  })

  it("should render the name column with asc class when objects are sorted by name asc", () => {
    const sortObjects = jest.fn()
    const wrapper = shallow(
      <ObjectsHeader
        sortObjects={sortObjects}
        sortedByName={true}
        sortOrder={SORT_ORDER_ASC}
        t={key => key}
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
        t={key => key}
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
        t={key => key}
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
        t={key => key}
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
        t={key => key}
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
        t={key => key}
      />
    )
    expect(
      wrapper.find("#sort-by-last-modified i").hasClass("fa-sort-numeric-down-alt")
    ).toBeTruthy()
  })

  it("should call sortObjects when a column is clicked", () => {
    const sortObjects = jest.fn()
    const wrapper = shallow(<ObjectsHeader sortObjects={sortObjects} t={key => key}/>)
    wrapper.find("#sort-by-name").simulate("click")
    expect(sortObjects).toHaveBeenCalledWith("name")
    wrapper.find("#sort-by-size").simulate("click")
    expect(sortObjects).toHaveBeenCalledWith("size")
    wrapper.find("#sort-by-last-modified").simulate("click")
    expect(sortObjects).toHaveBeenCalledWith("last-modified")
  })
})
