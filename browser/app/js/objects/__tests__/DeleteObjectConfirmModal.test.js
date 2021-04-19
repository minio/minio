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
import { DeleteObjectConfirmModal } from "../DeleteObjectConfirmModal"

describe("DeleteObjectConfirmModal", () => {
  it("should render without crashing", () => {
    shallow(<DeleteObjectConfirmModal />)
  })

  it("should call deleteObject when Delete is clicked", () => {
    const deleteObject = jest.fn()
    const wrapper = shallow(
      <DeleteObjectConfirmModal deleteObject={deleteObject} />
    )
    wrapper.find("ConfirmModal").prop("okHandler")()
    expect(deleteObject).toHaveBeenCalled()
  })

  it("should call hideDeleteConfirmModal when Cancel is clicked", () => {
    const hideDeleteConfirmModal = jest.fn()
    const wrapper = shallow(
      <DeleteObjectConfirmModal
        hideDeleteConfirmModal={hideDeleteConfirmModal}
      />
    )
    wrapper.find("ConfirmModal").prop("cancelHandler")()
    expect(hideDeleteConfirmModal).toHaveBeenCalled()
  })
})
