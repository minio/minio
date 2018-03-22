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
import Header from "../Header"

jest.mock("../../web", () => ({
  LoggedIn: jest
    .fn(() => true)
    .mockReturnValueOnce(true)
    .mockReturnValueOnce(false)
}))
describe("Header", () => {
  it("should render without crashing", () => {
    shallow(<Header />)
  })

  it("should render Login button when the user has not LoggedIn", () => {
    const wrapper = shallow(<Header />)
    expect(wrapper.find("a").text()).toBe("Login")
  })

  it("should render StorageInfo and BrowserDropdown when the user has LoggedIn", () => {
    const wrapper = shallow(<Header />)
    expect(wrapper.find("Connect(BrowserDropdown)").length).toBe(1)
    expect(wrapper.find("Connect(StorageInfo)").length).toBe(1)
  })
})
