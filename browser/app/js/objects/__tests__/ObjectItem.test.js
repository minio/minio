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
import { ObjectItem } from "../ObjectItem"

describe("ObjectItem", () => {
  it("should render without crashing", () => {
    shallow(<ObjectItem name={"test"} />)
  })

  it("should render with content type", () => {
    const wrapper = shallow(<ObjectItem name={"test.jpg"} contentType={""} />)
    expect(wrapper.prop("data-type")).toBe("image")
  })

  it("shouldn't call onClick when the object isclicked", () => {
    const onClick = jest.fn()
    const checkObject = jest.fn()
    const wrapper = shallow(
      <ObjectItem name={"test"} checkObject={checkObject} />
    )
    wrapper.find("a").simulate("click", { preventDefault: jest.fn() })
    expect(onClick).not.toHaveBeenCalled()
  })

  it("should call onClick when the folder isclicked", () => {
    const onClick = jest.fn()
    const wrapper = shallow(<ObjectItem name={"test/"} onClick={onClick} />)
    wrapper.find("a").simulate("click", { preventDefault: jest.fn() })
    expect(onClick).toHaveBeenCalled()
  })

  it("should call checkObject when the object/prefix is checked", () => {
    const checkObject = jest.fn()
    const wrapper = shallow(
      <ObjectItem name={"test"} checked={false} checkObject={checkObject} />
    )
    wrapper.find("input[type='checkbox']").simulate("change")
    expect(checkObject).toHaveBeenCalledWith("test")
  })

  it("should render checked checkbox", () => {
    const wrapper = shallow(<ObjectItem name={"test"} checked={true} />)
    expect(wrapper.find("input[type='checkbox']").prop("checked")).toBeTruthy()
  })

  it("should call uncheckObject when the object/prefix is unchecked", () => {
    const checkObject = jest.fn()
    const uncheckObject = jest.fn()
    const wrapper = shallow(
      <ObjectItem
        name={"test"}
        checked={true}
        checkObject={checkObject}
        uncheckObject={uncheckObject}
      />
    )
    wrapper.find("input[type='checkbox']").simulate("change")
    expect(uncheckObject).toHaveBeenCalledWith("test")
  })
})
