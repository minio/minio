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
import { shallow, mount } from "enzyme"
import { MemoryRouter } from "react-router-dom"
import App from "../App"

jest.mock("../browser/Login", () => () => <div>Login</div>)
jest.mock("../browser/Browser", () => () => <div>Browser</div>)

describe("App", () => {
  it("should render without crashing", () => {
    shallow(<App />)
  })

  it("should render Login component for '/login' route", () => {
    const wrapper = mount(
      <MemoryRouter initialEntries={["/login"]}>
        <App />
      </MemoryRouter>
    )
    expect(wrapper.text()).toBe("Login")
  })

  it("should render Browser component for '/' route", () => {
    const wrapper = mount(
      <MemoryRouter initialEntries={["/"]}>
        <App />
      </MemoryRouter>
    )
    expect(wrapper.text()).toBe("Browser")
  })

  it("should render Browser component for '/bucket' route", () => {
    const wrapper = mount(
      <MemoryRouter initialEntries={["/bucket"]}>
        <App />
      </MemoryRouter>
    )
    expect(wrapper.text()).toBe("Browser")
  })

  it("should render Browser component for '/bucket/a/b/c' route", () => {
    const wrapper = mount(
      <MemoryRouter initialEntries={["/bucket/a/b/c"]}>
        <App />
      </MemoryRouter>
    )
    expect(wrapper.text()).toBe("Browser")
  })
})
