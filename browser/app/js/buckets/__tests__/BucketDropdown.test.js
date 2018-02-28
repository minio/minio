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
