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

import React from "react"
import { shallow } from "enzyme"
import { ObjectsHeader } from "../ObjectsHeader"

describe("ObjectsHeader", () => {
  it("should render without crashing", () => {
    const sortObjects = jest.fn()
    shallow(<ObjectsHeader sortObjects={sortObjects} />)
  })

  it("should render columns with asc classes by default", () => {
    const sortObjects = jest.fn()
    const wrapper = shallow(<ObjectsHeader sortObjects={sortObjects} />)
    expect(
      wrapper.find("#sort-by-name i").hasClass("fa-sort-alpha-asc")
    ).toBeTruthy()
    expect(
      wrapper.find("#sort-by-size i").hasClass("fa-sort-amount-asc")
    ).toBeTruthy()
    expect(
      wrapper.find("#sort-by-last-modified i").hasClass("fa-sort-numeric-asc")
    ).toBeTruthy()
  })

  it("should render name column with desc class when objects are sorted by name", () => {
    const sortObjects = jest.fn()
    const wrapper = shallow(
      <ObjectsHeader sortObjects={sortObjects} sortNameOrder={true} />
    )
    expect(
      wrapper.find("#sort-by-name i").hasClass("fa-sort-alpha-desc")
    ).toBeTruthy()
  })

  it("should render size column with desc class when objects are sorted by size", () => {
    const sortObjects = jest.fn()
    const wrapper = shallow(
      <ObjectsHeader sortObjects={sortObjects} sortSizeOrder={true} />
    )
    expect(
      wrapper.find("#sort-by-size i").hasClass("fa-sort-amount-desc")
    ).toBeTruthy()
  })

  it("should render last modified column with desc class when objects are sorted by last modified", () => {
    const sortObjects = jest.fn()
    const wrapper = shallow(
      <ObjectsHeader sortObjects={sortObjects} sortLastModifiedOrder={true} />
    )
    expect(
      wrapper.find("#sort-by-last-modified i").hasClass("fa-sort-numeric-desc")
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
