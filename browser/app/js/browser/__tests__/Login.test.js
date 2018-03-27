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
import { Login } from "../Login"
import web from "../../web"

jest.mock('../../web', () => ({
  Login: jest.fn(() => {
    return Promise.resolve({ token: "test", uiVersion: "2018-02-01T01:17:47Z" })
  }),
  LoggedIn: jest.fn()
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
      />,
      { attachTo: document.body }
    )
    // case where both keys are empty - displays the second warning
    wrapper.find("form").simulate("submit")
    expect(showAlertMock).toHaveBeenCalledWith("danger", "Secret Key cannot be empty")

    // case where access key is empty
    document.getElementById("secretKey").value = "secretKey"
    wrapper.find("form").simulate("submit")
    expect(showAlertMock).toHaveBeenCalledWith("danger", "Access Key cannot be empty")

    // case where secret key is empty
    document.getElementById("accessKey").value = "accessKey"
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
      />,
      { attachTo: document.body }
    )
    document.getElementById("accessKey").value = "accessKey"
    document.getElementById("secretKey").value = "secretKey"
    wrapper.find("form").simulate("submit")
    expect(web.Login).toHaveBeenCalledWith({
      "username": "accessKey", 
      "password": "secretKey"
    })
  })
})
