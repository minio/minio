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
import { shallow, mount } from "enzyme"
import { BucketDropdown } from "../BucketDropdown"

describe("BucketDropdown", () => {
  it("should render without crashing", () => {
    shallow(<BucketDropdown />)
  })

  it("should call toggleDropdown on dropdown toggle", () => {
    const spy = jest.spyOn(BucketDropdown.prototype, 'toggleDropdown')
    const wrapper = shallow(
      <BucketDropdown />
    )
    wrapper
      .find("Uncontrolled(Dropdown)")
      .simulate("toggle")
    expect(spy).toHaveBeenCalled()
    spy.mockReset()
    spy.mockRestore()
  })

  it("should call showBucketPolicy when Edit Policy link is clicked", () => {
    const showBucketPolicy = jest.fn()
    const wrapper = shallow(
      <BucketDropdown showBucketPolicy={showBucketPolicy} />
    )
    wrapper
      .find("li a")
      .at(0)
      .simulate("click", { stopPropagation: jest.fn() })
    expect(showBucketPolicy).toHaveBeenCalled()
  })

  it("should call deleteBucket when Delete link is clicked", () => {
    const deleteBucket = jest.fn()
    const wrapper = shallow(
      <BucketDropdown bucket={"test"} deleteBucket={deleteBucket} />
    )
    wrapper
      .find("li a")
      .at(1)
      .simulate("click", { stopPropagation: jest.fn() })
    expect(deleteBucket).toHaveBeenCalledWith("test")
  })
})
