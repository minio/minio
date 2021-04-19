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
import { SideBar } from "../SideBar"

jest.mock("../../web", () => ({
  LoggedIn: jest.fn(() => false).mockReturnValueOnce(true)
}))

describe("SideBar", () => {
  it("should render without crashing", () => {
    shallow(<SideBar />)
  })

  it("should not render BucketSearch for non LoggedIn users", () => {
    const wrapper = shallow(<SideBar />)
    expect(wrapper.find("Connect(BucketSearch)").length).toBe(0)
  })

  it("should call clickOutside when the user clicks outside the sidebar", () => {
    const clickOutside = jest.fn()
    const wrapper = shallow(<SideBar clickOutside={clickOutside} />)
    wrapper.simulate("clickOut", {
      preventDefault: jest.fn(),
      target: { classList: { contains: jest.fn(() => false) } }
    })
    expect(clickOutside).toHaveBeenCalled()
  })

  it("should not call clickOutside when user clicks on sidebar toggle", () => {
    const clickOutside = jest.fn()
    const wrapper = shallow(<SideBar clickOutside={clickOutside} />)
    wrapper.simulate("clickOut", {
      preventDefault: jest.fn(),
      target: { classList: { contains: jest.fn(() => true) } }
    })
    expect(clickOutside).not.toHaveBeenCalled()
  })
})
