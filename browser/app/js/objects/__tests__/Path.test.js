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
import { Path } from "../Path"

describe("Path", () => {
  it("should render without crashing", () => {
    shallow(<Path currentBucket={"test1"} currentPrefix={"test2"} />)
  })

  it("should render only bucket if there is no prefix", () => {
    const wrapper = shallow(<Path currentBucket={"test1"} currentPrefix={""} />)
    expect(wrapper.find("span").length).toBe(1)
    expect(
      wrapper
        .find("span")
        .at(0)
        .text()
    ).toBe("test1")
  })

  it("should render bucket and prefix", () => {
    const wrapper = shallow(
      <Path currentBucket={"test1"} currentPrefix={"a/b/"} />
    )
    expect(wrapper.find("span").length).toBe(3)
    expect(
      wrapper
        .find("span")
        .at(0)
        .text()
    ).toBe("test1")
    expect(
      wrapper
        .find("span")
        .at(1)
        .text()
    ).toBe("a")
    expect(
      wrapper
        .find("span")
        .at(2)
        .text()
    ).toBe("b")
  })

  it("should call selectPrefix when a prefix part is clicked", () => {
    const selectPrefix = jest.fn()
    const wrapper = shallow(
      <Path
        currentBucket={"test1"}
        currentPrefix={"a/b/"}
        selectPrefix={selectPrefix}
      />
    )
    wrapper
      .find("a")
      .at(2)
      .simulate("click", { preventDefault: jest.fn() })
    expect(selectPrefix).toHaveBeenCalledWith("a/b/")
  })

  it("should switch to input mode when edit icon is clicked", () => {
    const wrapper = mount(<Path currentBucket={"test1"} currentPrefix={""} />)
    wrapper.find(".fe-edit").simulate("click", { preventDefault: jest.fn() })
    expect(wrapper.find(".form-control--path").exists()).toBeTruthy()
  })

  it("should navigate to prefix when user types path for existing bucket", () => {
    const selectBucket = jest.fn()
    const buckets = ["test1", "test2"]
    const wrapper = mount(
      <Path
        buckets={buckets}
        currentBucket={"test1"}
        currentPrefix={""}
        selectBucket={selectBucket}
      />
    )
    wrapper.setState({
      isEditing: true,
      path: "test2/dir1/"
    })
    wrapper.find("form").simulate("submit", { preventDefault: jest.fn() })
    expect(selectBucket).toHaveBeenCalledWith("test2", "dir1/")
  })

  it("should create a new bucket if bucket typed in path doesn't exist", () => {
    const makeBucket = jest.fn()
    const buckets = ["test1", "test2"]
    const wrapper = mount(
      <Path
        buckets={buckets}
        currentBucket={"test1"}
        currentPrefix={""}
        makeBucket={makeBucket}
      />
    )
    wrapper.setState({
      isEditing: true,
      path: "test3/dir1/"
    })
    wrapper.find("form").simulate("submit", { preventDefault: jest.fn() })
    expect(makeBucket).toHaveBeenCalledWith("test3")
  })

  it("should not make or select bucket if path doesn't point to bucket", () => {
    const makeBucket = jest.fn()
    const selectBucket = jest.fn()
    const buckets = ["test1", "test2"]
    const wrapper = mount(
      <Path
        buckets={buckets}
        currentBucket={"test1"}
        currentPrefix={""}
        makeBucket={makeBucket}
        selectBucket={selectBucket}
      />
    )
    wrapper.setState({
      isEditing: true,
      path: "//dir1/dir2/"
    })
    wrapper.find("form").simulate("submit", { preventDefault: jest.fn() })
    expect(makeBucket).not.toHaveBeenCalled()
    expect(selectBucket).not.toHaveBeenCalled()
  })
})
