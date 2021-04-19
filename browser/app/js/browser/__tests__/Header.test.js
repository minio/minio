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
