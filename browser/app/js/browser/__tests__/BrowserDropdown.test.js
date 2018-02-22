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
import { BrowserDropdown } from "../BrowserDropdown"

describe("BrowserDropdown", () => {
  const serverInfo = {
    version: "test",
    memory: "test",
    platform: "test",
    runtime: "test"
  }

  it("should render without crashing", () => {
    shallow(
      <BrowserDropdown serverInfo={serverInfo} fetchServerInfo={jest.fn()} />
    )
  })

  it("should call fetchServerInfo after its mounted", () => {
    const fetchServerInfo = jest.fn()
    const wrapper = shallow(
      <BrowserDropdown
        serverInfo={serverInfo}
        fetchServerInfo={fetchServerInfo}
      />
    )
    expect(fetchServerInfo).toHaveBeenCalled()
  })

  it("should show AboutModal when About link is clicked", () => {
    const wrapper = shallow(
      <BrowserDropdown serverInfo={serverInfo} fetchServerInfo={jest.fn()} />
    )
    wrapper.find("#show-about").simulate("click", { preventDefault: jest.fn() })
    wrapper.update()
    expect(wrapper.state("showAboutModal")).toBeTruthy()
    expect(wrapper.find("AboutModal").length).toBe(1)
  })

  it("should logout and redirect to /login when logout is clicked", () => {
    const wrapper = shallow(
      <BrowserDropdown serverInfo={serverInfo} fetchServerInfo={jest.fn()} />
    )
    wrapper.find("#logout").simulate("click", { preventDefault: jest.fn() })
    expect(window.location.pathname.endsWith("/login")).toBeTruthy()
  })
})
