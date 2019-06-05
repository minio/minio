/*
 * MinIO Cloud Storage (C) 2018 MinIO, Inc.
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
import jwtDecode from "jwt-decode"

jest.mock("jwt-decode")

jwtDecode.mockImplementation(() => ({ sub: "minio" }))

jest.mock("../../web", () => ({
  GenerateAuth: jest.fn(() => {
    return Promise.resolve({ accessKey: "gen1", secretKey: "gen2" })
  }),
  SetAuth: jest.fn(
    ({ currentAccessKey, currentSecretKey, newAccessKey, newSecretKey }) => {
      if (
        currentAccessKey == "minio" &&
        currentSecretKey == "minio123" &&
        newAccessKey == "test" &&
        newSecretKey == "test1234"
      ) {
        return Promise.resolve({})
      } else {
        return Promise.reject({
          message: "Error"
        })
      }
    }
  ),
  GetToken: jest.fn(() => "")
}))

jest.mock("../../utils", () => ({
  getRandomAccessKey: () => "raccesskey",
  getRandomSecretKey: () => "rsecretkey"
}))

describe("ChangePasswordModal", () => {
  const serverInfo = {
    version: "test",
    memory: "test",
    platform: "test",
    runtime: "test",
    info: { isEnvCreds: false },
    userInfo: { isIAMUser: false }
  }

  it("should render without crashing", () => {
    shallow(<ChangePasswordModal serverInfo={serverInfo} />)
  })

  it("should not allow changing password when isWorm is true", () => {
    const newServerInfo = { ...serverInfo, info: { isWorm: true } }
    const wrapper = shallow(<ChangePasswordModal serverInfo={newServerInfo} />)
    expect(
      wrapper
        .find("ModalBody")
        .childAt(0)
        .text()
    ).toBe("Credentials of this user cannot be updated through MinIO Browser.")
  })

  it("should not allow changing password when isEnvCreds is true and not IAM user", () => {
    const newServerInfo = {
      ...serverInfo,
      info: { isEnvCreds: true },
      userInfo: { isIAMUser: false }
    }
    const wrapper = shallow(<ChangePasswordModal serverInfo={newServerInfo} />)
    expect(
      wrapper
        .find("ModalBody")
        .childAt(0)
        .text()
    ).toBe("Credentials of this user cannot be updated through MinIO Browser.")
  })

  it("should generate accessKey and secretKey when Generate buttons is clicked", () => {
    const wrapper = shallow(<ChangePasswordModal serverInfo={serverInfo} />)
    wrapper.find("#generate-keys").simulate("click")
    setImmediate(() => {
      expect(wrapper.state("newAccessKey")).toBe("raccesskey")
      expect(wrapper.state("newSecretKey")).toBe("rsecretkey")
    })
  })

  it("should not generate accessKey for IAM User", () => {
    const newServerInfo = {
      ...serverInfo,
      userInfo: { isIAMUser: true }
    }
    const wrapper = shallow(<ChangePasswordModal serverInfo={newServerInfo} />)
    wrapper.find("#generate-keys").simulate("click")
    setImmediate(() => {
      expect(wrapper.state("newAccessKey")).toBe("minio")
      expect(wrapper.state("newSecretKey")).toBe("rsecretkey")
    })
  })

  it("should not show new accessKey field for IAM User", () => {
    const newServerInfo = {
      ...serverInfo,
      userInfo: { isIAMUser: true }
    }
    const wrapper = shallow(<ChangePasswordModal serverInfo={newServerInfo} />)
    expect(wrapper.find("#newAccesskey").exists()).toBeFalsy()
  })

  it("should disble Update button for invalid accessKey or secretKey", () => {
    const showAlert = jest.fn()
    const wrapper = shallow(
      <ChangePasswordModal serverInfo={serverInfo} showAlert={showAlert} />
    )
    wrapper
      .find("#currentAccessKey")
      .simulate("change", { target: { value: "minio" } })
    wrapper
      .find("#currentSecretKey")
      .simulate("change", { target: { value: "minio123" } })
    wrapper.find("#newAccessKey").simulate("change", { target: { value: "t" } })
    wrapper
      .find("#newSecretKey")
      .simulate("change", { target: { value: "t1" } })
    expect(wrapper.find("#update-keys").prop("disabled")).toBeTruthy()
  })

  it("should update accessKey and secretKey when Update button is clicked", () => {
    const showAlert = jest.fn()
    const wrapper = shallow(
      <ChangePasswordModal serverInfo={serverInfo} showAlert={showAlert} />
    )
    wrapper
      .find("#currentAccessKey")
      .simulate("change", { target: { value: "minio" } })
    wrapper
      .find("#currentSecretKey")
      .simulate("change", { target: { value: "minio123" } })
    wrapper
      .find("#newAccessKey")
      .simulate("change", { target: { value: "test" } })
    wrapper
      .find("#newSecretKey")
      .simulate("change", { target: { value: "test1234" } })
    expect(wrapper.find("#update-keys").prop("disabled")).toBeFalsy()
    wrapper.find("#update-keys").simulate("click")
    setImmediate(() => {
      expect(showAlert).toHaveBeenCalledWith({
        type: "success",
        message: "Credentials updated successfully."
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
