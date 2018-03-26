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
import { ChangePasswordModal } from "../ChangePasswordModal"

jest.mock("../../web", () => ({
  GetAuth: jest.fn(() => {
    return Promise.resolve({ accessKey: "test1", secretKey: "test2" })
  }),
  GenerateAuth: jest.fn(() => {
    return Promise.resolve({ accessKey: "gen1", secretKey: "gen2" })
  }),
  SetAuth: jest.fn(({ accessKey, secretKey }) => {
    if (accessKey == "test3" && secretKey == "test4") {
      return Promise.resolve({})
    } else {
      return Promise.reject({ message: "Error" })
    }
  })
}))

describe("ChangePasswordModal", () => {
  const serverInfo = {
    version: "test",
    memory: "test",
    platform: "test",
    runtime: "test",
    info: { isEnvCreds: false }
  }

  it("should render without crashing", () => {
    shallow(<ChangePasswordModal serverInfo={serverInfo} />)
  })

  it("should get the keys when its rendered", () => {
    const wrapper = shallow(<ChangePasswordModal serverInfo={serverInfo} />)
    setImmediate(() => {
      expect(wrapper.state("accessKey")).toBe("test1")
      expect(wrapper.state("secretKey")).toBe("test2")
    })
  })

  it("should show readonly keys when isEnvCreds is true", () => {
    const newServerInfo = { ...serverInfo, info: { isEnvCreds: true } }
    const wrapper = shallow(<ChangePasswordModal serverInfo={newServerInfo} />)
    expect(wrapper.state("accessKey")).toBe("xxxxxxxxx")
    expect(wrapper.state("secretKey")).toBe("xxxxxxxxx")
    expect(wrapper.find("#accessKey").prop("readonly")).toBeTruthy()
    expect(wrapper.find("#secretKey").prop("readonly")).toBeTruthy()
    expect(wrapper.find("#generate-keys").hasClass("hidden")).toBeTruthy()
    expect(wrapper.find("#update-keys").hasClass("hidden")).toBeTruthy()
  })

  it("should generate accessKey and secretKey when Generate buttons is clicked", () => {
    const wrapper = shallow(<ChangePasswordModal serverInfo={serverInfo} />)
    wrapper.find("#generate-keys").simulate("click")
    setImmediate(() => {
      expect(wrapper.state("accessKey")).toBe("gen1")
      expect(wrapper.state("secretKey")).toBe("gen2")
    })
  })

  it("should update accessKey and secretKey when Update button is clicked", () => {
    const showAlert = jest.fn()
    const wrapper = shallow(
      <ChangePasswordModal serverInfo={serverInfo} showAlert={showAlert} />
    )
    wrapper
      .find("#accessKey")
      .simulate("change", { target: { value: "test3" } })
    wrapper
      .find("#secretKey")
      .simulate("change", { target: { value: "test4" } })
    wrapper.find("#update-keys").simulate("click")
    setImmediate(() => {
      expect(showAlert).toHaveBeenCalledWith({
        type: "success",
        message: "Changed credentials"
      })
    })
  })

  it("should call hideChangePassword when Cancel button is clicked", () => {
    const hideChangePassword = jest.fn()
    const wrapper = shallow(
      <ChangePasswordModal
        serverInfo={serverInfo}
        hideChangePassword={hideChangePassword}
      />
    )
    wrapper.find("#cancel-change-password").simulate("click")
    expect(hideChangePassword).toHaveBeenCalled()
  })
})
