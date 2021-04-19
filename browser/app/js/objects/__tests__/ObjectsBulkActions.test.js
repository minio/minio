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
