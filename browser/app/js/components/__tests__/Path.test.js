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
import { Path } from "../Path"

describe("Path", () => {
  it("should render without crashing", () => {
    shallow(<Path currentBucket={"test1"} currentPrefix={"test2"} />)
  })

  it("should render only bucket if there is no prefix", () => {
    const wrapper = shallow(<Path currentBucket={"test1"} currentPrefix={""} />)
    expect(wrapper.find("span").length).toBe(1)
    expect(wrapper.text()).toBe("test1")
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
})
