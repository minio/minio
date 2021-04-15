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
import { shallow, mount } from "enzyme"
import { Path } from "../Path"

describe("Path", () => {
  it("should render without crashing", () => {
    shallow(<Path currentBucket={"test1"} currentPrefix={"test2"} t={key => key} />)
  })

  it("should render only bucket if there is no prefix", () => {
    const wrapper = shallow(<Path currentBucket={"test1"} currentPrefix={""} t={key => key} />)
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
      <Path currentBucket={"test1"} currentPrefix={"a/b/"} t={key => key} />
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
        t={key => key}
      />
    )
    wrapper
      .find("a")
      .at(2)
      .simulate("click", { preventDefault: jest.fn() })
    expect(selectPrefix).toHaveBeenCalledWith("a/b/")
  })

  it("should switch to input mode when edit icon is clicked", () => {
    const wrapper = mount(<Path currentBucket={"test1"} currentPrefix={""} t={key => key} />)
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
        t={key => key}
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
        t={key => key}
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
        t={key => key}
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
