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
import { ObjectsListContainer } from "../ObjectsListContainer"

describe("ObjectsList", () => {
  it("should render without crashing", () => {
    shallow(<ObjectsListContainer loadObjects={jest.fn()} />)
  })

  it("should render ObjectsList with objects", () => {
    const wrapper = shallow(
      <ObjectsListContainer
        objects={[{ name: "test1.jpg" }, { name: "test2.jpg" }]}
        loadObjects={jest.fn()}
      />
    )
    expect(wrapper.find("ObjectsList").length).toBe(1)
    expect(wrapper.find("ObjectsList").prop("objects")).toEqual([
      { name: "test1.jpg" },
      { name: "test2.jpg" }
    ])
  })

  it("should show the loading indicator at the bottom if there are more elements to display", () => {
    const wrapper = shallow(
      <ObjectsListContainer currentBucket="test1" isTruncated={true} />
    )
    expect(wrapper.find(".text-center").prop("style")).toHaveProperty("display", "block")
  })
})
