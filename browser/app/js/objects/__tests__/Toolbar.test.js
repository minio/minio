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
import { Toolbar } from "../Toolbar"

jest.mock("../../web", () => ({
  LoggedIn: jest
    .fn(() => true)
    .mockReturnValueOnce(true)
    .mockReturnValueOnce(false)
}))

describe("Toolbar", () => {
  it("should render without crashing", () => {
    shallow(<Toolbar checkedObjectsCount={0} />)
  })

  it("should render Login button when the user has not LoggedIn", () => {
    const wrapper = shallow(<Toolbar checkedObjectsCount={0} />)
    expect(wrapper.find("a").text()).toBe("Login")
  })

  it("should render StorageInfo and BrowserDropdown when the user has LoggedIn", () => {
    const wrapper = shallow(<Toolbar checkedObjectsCount={0} />)
    expect(wrapper.find("Connect(BrowserDropdown)").length).toBe(1)
  })

  it("should enable delete action when checkObjectsCount is more than 0", () => {
    const wrapper = shallow(<Toolbar checkedObjectsCount={1} />)
    expect(wrapper.find(".zmdi-delete").prop("disabled")).toBeFalsy()
  })

  it("shoud show DeleteObjectConfirmModal when delete button is clicked", () => {
    const wrapper = shallow(<Toolbar checkedObjectsCount={1} />)
    wrapper.find("button.zmdi-delete").simulate("click")
    wrapper.update()
    expect(wrapper.find("DeleteObjectConfirmModal").length).toBe(1)
  })

  it("shoud call deleteChecked when Delete is clicked on confirmation modal", () => {
    const deleteChecked = jest.fn()
    const wrapper = shallow(
      <Toolbar checkedObjectsCount={1} deleteChecked={deleteChecked} />
    )
    wrapper.find("button.zmdi-delete").simulate("click")
    wrapper.update()
    wrapper.find("DeleteObjectConfirmModal").prop("deleteObject")()
    expect(deleteChecked).toHaveBeenCalled()
    wrapper.update()
    expect(wrapper.find("DeleteObjectConfirmModal").length).toBe(0)
  })

  it("should call downloadChecked when download button is clicked", () => {
    const downloadChecked = jest.fn()
    const wrapper = shallow(
      <Toolbar checkedObjectsCount={1} downloadChecked={downloadChecked} />
    )
    wrapper.find("button.zmdi-download").simulate("click")
    expect(downloadChecked).toHaveBeenCalled()
  })
})
