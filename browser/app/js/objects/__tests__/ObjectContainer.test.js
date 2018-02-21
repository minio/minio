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
import { ObjectContainer } from "../ObjectContainer"

describe("ObjectContainer", () => {
  it("should render without crashing", () => {
    shallow(<ObjectContainer object={{ name: "test1.jpg" }} />)
  })

  it("should render ObjectItem with props", () => {
    const wrapper = shallow(<ObjectContainer object={{ name: "test1.jpg" }} />)
    expect(wrapper.find("Connect(ObjectItem)").length).toBe(1)
    expect(wrapper.find("Connect(ObjectItem)").prop("name")).toBe("test1.jpg")
  })

  it("should pass actions to ObjectItem", () => {
    const wrapper = shallow(
      <ObjectContainer object={{ name: "test1.jpg" }} checkedObjectsCount={0} />
    )
    expect(wrapper.find("Connect(ObjectItem)").prop("actionButtons")).not.toBe(
      undefined
    )
  })

  it("should pass empty actions to ObjectItem when checkedObjectCount is more than 0", () => {
    const wrapper = shallow(
      <ObjectContainer object={{ name: "test1.jpg" }} checkedObjectsCount={1} />
    )
    expect(wrapper.find("Connect(ObjectItem)").prop("actionButtons")).toBe(
      undefined
    )
  })
})
