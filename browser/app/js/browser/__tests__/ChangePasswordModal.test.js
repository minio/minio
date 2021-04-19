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
import { ChangePasswordModal } from "../ChangePasswordModal"
import jwtDecode from "jwt-decode"

jest.mock("jwt-decode")

jwtDecode.mockImplementation(() => ({ sub: "minio" }))

jest.mock("../../web", () => ({
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
    platform: "test",
    runtime: "test",
    info: {},
    userInfo: { isIAMUser: true }
  }

  it("should render without crashing", () => {
    shallow(<ChangePasswordModal serverInfo={serverInfo} />)
  })

  it("should not allow changing password when not IAM user", () => {
    const newServerInfo = {
      ...serverInfo,
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

  it("should not allow changing password for STS user", () => {
    const newServerInfo = {
      ...serverInfo,
      userInfo: { isTempUser: true }
    }
    const wrapper = shallow(<ChangePasswordModal serverInfo={newServerInfo} />)
    expect(
      wrapper
        .find("ModalBody")
        .childAt(0)
        .text()
    ).toBe("Credentials of this user cannot be updated through MinIO Browser.")
  })

  it("should not generate accessKey for IAM User", () => {
    const wrapper = shallow(<ChangePasswordModal serverInfo={serverInfo} />)
    wrapper.find("#generate-keys").simulate("click")
    setImmediate(() => {
      expect(wrapper.state("newAccessKey")).toBe("minio")
      expect(wrapper.state("newSecretKey")).toBe("rsecretkey")
    })
  })

  it("should not show new accessKey field for IAM User", () => {
    const wrapper = shallow(<ChangePasswordModal serverInfo={serverInfo} />)
    expect(wrapper.find("#newAccesskey").exists()).toBeFalsy()
  })

  it("should disable Update button for secretKey", () => {
    const showAlert = jest.fn()
    const wrapper = shallow(
      <ChangePasswordModal serverInfo={serverInfo} showAlert={showAlert} />
    )
    wrapper
      .find("#currentSecretKey")
      .simulate("change", { target: { value: "minio123" } })
    wrapper
      .find("#newSecretKey")
      .simulate("change", { target: { value: "t1" } })
    expect(wrapper.find("#update-keys").prop("disabled")).toBeTruthy()
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
