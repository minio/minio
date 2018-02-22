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
import { AboutModal } from "../AboutModal"

describe("AboutModal", () => {
  const serverInfo = {
    version: "test",
    memory: "test",
    platform: "test",
    runtime: "test"
  }

  it("should render without crashing", () => {
    shallow(<AboutModal serverInfo={serverInfo} />)
  })

  it("should call hideAbout when close button is clicked", () => {
    const hideAbout = jest.fn()
    const wrapper = shallow(
      <AboutModal serverInfo={serverInfo} hideAbout={hideAbout} />
    )
    wrapper.find("button").simulate("click")
    expect(hideAbout).toHaveBeenCalled()
  })
})
