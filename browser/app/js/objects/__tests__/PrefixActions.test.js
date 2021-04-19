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
import { PrefixActions } from "../PrefixActions"

describe("PrefixActions", () => {
  it("should render without crashing", () => {
    shallow(<PrefixActions object={{ name: "abc/" }} currentPrefix={"pre1/"} />)
  })

  it("should show DeleteObjectConfirmModal when delete action is clicked", () => {
    const wrapper = shallow(
      <PrefixActions object={{ name: "abc/" }} currentPrefix={"pre1/"} />
    )
    wrapper
      .find("a")
      .last()
      .simulate("click", { preventDefault: jest.fn() })
    expect(wrapper.state("showDeleteConfirmation")).toBeTruthy()
    expect(wrapper.find("DeleteObjectConfirmModal").length).toBe(1)
  })

  it("should hide DeleteObjectConfirmModal when Cancel button is clicked", () => {
    const wrapper = shallow(
      <PrefixActions object={{ name: "abc/" }} currentPrefix={"pre1/"} />
    )
    wrapper
      .find("a")
      .last()
      .simulate("click", { preventDefault: jest.fn() })
    wrapper.find("DeleteObjectConfirmModal").prop("hideDeleteConfirmModal")()
    wrapper.update()
    expect(wrapper.state("showDeleteConfirmation")).toBeFalsy()
    expect(wrapper.find("DeleteObjectConfirmModal").length).toBe(0)
  })

  it("should call deleteObject with object name", () => {
    const deleteObject = jest.fn()
    const wrapper = shallow(
      <PrefixActions
        object={{ name: "abc/" }}
        currentPrefix={"pre1/"}
        deleteObject={deleteObject}
      />
    )
    wrapper
      .find("a")
      .last()
      .simulate("click", { preventDefault: jest.fn() })
    wrapper.find("DeleteObjectConfirmModal").prop("deleteObject")()
    expect(deleteObject).toHaveBeenCalledWith("abc/")
  })


  it("should call downloadPrefix when single object is selected and download button is clicked", () => {
    const downloadPrefix = jest.fn()
    const wrapper = shallow(
      <PrefixActions
        object={{ name: "abc/" }}
        currentPrefix={"pre1/"}
        downloadPrefix={downloadPrefix} />
    )
    wrapper
      .find("a")
      .first()
      .simulate("click", { preventDefault: jest.fn() })
    expect(downloadPrefix).toHaveBeenCalled()
  })
})
