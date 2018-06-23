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
import { ObjectsBulkActions } from "../ObjectsBulkActions"

describe("ObjectsBulkActions", () => {
  it("should render without crashing", () => {
    shallow(<ObjectsBulkActions checkedObjects={[]} />)
  })

  it("should show actions when checkObjectsCount is more than 0", () => {
    const wrapper = shallow(<ObjectsBulkActions checkedObjects={["test"]} />)
    expect(wrapper.hasClass("list-actions-toggled")).toBeTruthy()
  })

  it("should call downloadObject when single object is selected and download button is clicked", () => {
    const downloadObject = jest.fn()
    const clearChecked = jest.fn()
    const wrapper = shallow(
      <ObjectsBulkActions
        checkedObjects={["test"]}
        downloadObject={downloadObject}
        clearChecked={clearChecked}
      />
    )
    wrapper.find("#download-checked").simulate("click")
    expect(downloadObject).toHaveBeenCalled()
  })

  it("should call downloadChecked when a folder is selected and download button is clicked", () => {
    const downloadChecked = jest.fn()
    const wrapper = shallow(
      <ObjectsBulkActions
        checkedObjects={["test/"]}
        downloadChecked={downloadChecked}
      />
    )
    wrapper.find("#download-checked").simulate("click")
    expect(downloadChecked).toHaveBeenCalled()
  })

  it("should call downloadChecked when multiple objects are selected and download button is clicked", () => {
    const downloadChecked = jest.fn()
    const wrapper = shallow(
      <ObjectsBulkActions
        checkedObjects={["test1", "test2"]}
        downloadChecked={downloadChecked}
      />
    )
    wrapper.find("#download-checked").simulate("click")
    expect(downloadChecked).toHaveBeenCalled()
  })

  it("should call clearChecked when close button is clicked", () => {
    const clearChecked = jest.fn()
    const wrapper = shallow(
      <ObjectsBulkActions checkedObjects={["test"]} clearChecked={clearChecked} />
    )
    wrapper.find("#close-bulk-actions").simulate("click")
    expect(clearChecked).toHaveBeenCalled()
  })

  it("shoud show DeleteObjectConfirmModal when delete-checked button is clicked", () => {
    const wrapper = shallow(<ObjectsBulkActions checkedObjects={["test"]} />)
    wrapper.find("#delete-checked").simulate("click")
    wrapper.update()
    expect(wrapper.find("DeleteObjectConfirmModal").length).toBe(1)
  })

  it("shoud call deleteChecked when Delete is clicked on confirmation modal", () => {
    const deleteChecked = jest.fn()
    const wrapper = shallow(
      <ObjectsBulkActions
        checkedObjects={["test"]}
        deleteChecked={deleteChecked}
      />
    )
    wrapper.find("#delete-checked").simulate("click")
    wrapper.update()
    wrapper.find("DeleteObjectConfirmModal").prop("deleteObject")()
    expect(deleteChecked).toHaveBeenCalled()
    wrapper.update()
    expect(wrapper.find("DeleteObjectConfirmModal").length).toBe(0)
  })
})
