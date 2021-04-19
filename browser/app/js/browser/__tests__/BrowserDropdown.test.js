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
import { shallow } from "enzyme"
import { BrowserDropdown } from "../BrowserDropdown"

describe("BrowserDropdown", () => {
  const serverInfo = {
    version: "test",
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
