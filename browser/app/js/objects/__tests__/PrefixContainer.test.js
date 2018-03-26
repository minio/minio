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
import { PrefixContainer } from "../PrefixContainer"

describe("PrefixContainer", () => {
  it("should render without crashing", () => {
    shallow(<PrefixContainer object={{ name: "abc/" }} />)
  })

  it("should render ObjectItem with props", () => {
    const wrapper = shallow(<PrefixContainer object={{ name: "abc/" }} />)
    expect(wrapper.find("Connect(ObjectItem)").length).toBe(1)
    expect(wrapper.find("Connect(ObjectItem)").prop("name")).toBe("abc/")
  })

  it("should call selectPrefix when the prefix is clicked", () => {
    const selectPrefix = jest.fn()
    const wrapper = shallow(
      <PrefixContainer
        object={{ name: "abc/" }}
        currentPrefix={"xyz/"}
        selectPrefix={selectPrefix}
      />
    )
    wrapper.find("Connect(ObjectItem)").prop("onClick")()
    expect(selectPrefix).toHaveBeenCalledWith("xyz/abc/")
  })
})
