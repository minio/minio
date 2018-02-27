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
