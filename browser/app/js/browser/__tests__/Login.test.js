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
import { Login } from "../Login"
import web from "../../web"

jest.mock("../../web", () => ({
  Login: jest.fn(() => {
    return Promise.resolve({ token: "test", uiVersion: "2018-02-01T01:17:47Z" })
  }),
  LoggedIn: jest.fn(),
  GetDiscoveryDoc: jest.fn(() => {
    return Promise.resolve({ DiscoveryDoc: {"authorization_endpoint": "test"} })
  })
}))

describe("Login", () => {
  const dispatchMock = jest.fn()
  const showAlertMock = jest.fn()
  const clearAlertMock = jest.fn()

  it("should render without crashing", () => {
    shallow(<Login
      dispatch={dispatchMock}
      alert={{ show: false, type: "danger"}}
      showAlert={showAlertMock}
      clearAlert={clearAlertMock}
    />)
  })

  it("should initially have the is-guest class", () => {
    const wrapper = shallow(
      <Login
        dispatch={dispatchMock}
        alert={{ show: false, type: "danger"}}
        showAlert={showAlertMock}
        clearAlert={clearAlertMock}
      />,
      { attachTo: document.body }
    )
    expect(document.body.classList.contains("is-guest")).toBeTruthy()
  })

  it("should throw an alert if the keys are empty in login form", () => {
    const wrapper = mount(
      <Login
        dispatch={dispatchMock}
        alert={{ show: false, type: "danger"}}
        showAlert={showAlertMock}
        clearAlert={clearAlertMock}
      />
    )
    // case where both keys are empty - displays the second warning
    wrapper.find("form").simulate("submit")
    expect(showAlertMock).toHaveBeenCalledWith("danger", "Secret Key cannot be empty")

    // case where access key is empty
    wrapper.setState({
      accessKey: "",
      secretKey: "secretKey"
    })
    wrapper.find("form").simulate("submit")
    expect(showAlertMock).toHaveBeenCalledWith("danger", "Access Key cannot be empty")

    // case where secret key is empty
    wrapper.setState({
      accessKey: "accessKey",
      secretKey: ""
    })
    wrapper.find("form").simulate("submit")
    expect(showAlertMock).toHaveBeenCalledWith("danger", "Secret Key cannot be empty")
  })

  it("should call web.Login with correct arguments if both keys are entered", () => {
    const wrapper = mount(
      <Login
        dispatch={dispatchMock}
        alert={{ show: false, type: "danger"}}
        showAlert={showAlertMock}
        clearAlert={clearAlertMock}
      />
    )
    wrapper.setState({
      accessKey: "accessKey",
      secretKey: "secretKey"
    })
    wrapper.find("form").simulate("submit")
    expect(web.Login).toHaveBeenCalledWith({
      "username": "accessKey",
      "password": "secretKey"
    })
  })
})
